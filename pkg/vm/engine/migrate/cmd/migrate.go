package main

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v3"
	"os"
	"path"
	"path/filepath"

	jsoniter "github.com/json-iterator/go"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/migrate"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/spf13/cobra"
)

type migrateArg struct {
}

func (c *migrateArg) PrepareCommand() *cobra.Command {
	migrateCmd := &cobra.Command{
		Use:   "migrate",
		Short: "migrate ckp",
		Run:   RunFactory(c),
	}

	replay := replayArg{}
	migrateCmd.AddCommand(replay.PrepareCommand())

	gc := gcArg{}
	migrateCmd.AddCommand(gc.PrepareCommand())

	rollback := rollbackArg{}
	migrateCmd.AddCommand(rollback.PrepareCommand())

	return migrateCmd
}

func (c *migrateArg) FromCommand(cmd *cobra.Command) (err error) {
	return nil
}

func (c *migrateArg) String() string {
	return ""
}

func (c *migrateArg) Run() error {
	return nil
}

type fsArg struct {
	Name      string `json:"name"`
	Endpoint  string `json:"endpoint"`
	Bucket    string `json:"bucket"`
	KeyPrefix string `json:"key_prefix"`
}

func getFsArg(input string) (arg fsArg, err error) {
	var data []byte
	if data, err = os.ReadFile(input); err != nil {
		return
	}
	if err = jsoniter.Unmarshal(data, &arg); err != nil {
		return
	}
	return
}

type replayArg struct {
	arg       fsArg
	cfg, meta string
	rootDir   string
	tid       uint64
}

func (c *replayArg) PrepareCommand() *cobra.Command {
	replayCmd := &cobra.Command{
		Use:   "replay",
		Short: "replay ckp",
		Run:   RunFactory(c),
	}

	replayCmd.Flags().StringP("cfg", "c", "", "config")
	replayCmd.Flags().StringP("root", "r", "", "root")
	replayCmd.Flags().Uint64P("tid", "t", 0, "tid")

	return replayCmd
}

func (c *replayArg) FromCommand(cmd *cobra.Command) (err error) {
	c.rootDir = cmd.Flag("root").Value.String()
	cfg := cmd.Flag("cfg").Value.String()
	if c.rootDir == "" {
		c.arg, err = getFsArg(cfg)
		if err != nil {
			panic(err)
		}
	}
	c.tid, _ = cmd.Flags().GetUint64("tid")
	return nil
}

func (c *replayArg) String() string {
	return ""
}

const (
	ckpDir    = "ckp"
	ckpBakDir = "ckp-bak"
	gcDir     = "gc"

	oldObjDir   = "rewritten/old"
	newObjDir   = "rewritten/new"
	rollbackDir = "rewritten/rollback"
)

func cleanDir(fs fileservice.FileService, dir string) {
	ctx := context.Background()
	entries, _ := fs.List(ctx, dir)
	for _, entry := range entries {
		err := fs.Delete(ctx, dir+"/"+entry.Name)
		if err != nil {
			logutil.Infof("delete %s/%s failed", dir, entry.Name)
		}
	}
}

const (
	rootDir = "/home/mo/wenbin/matrixone/mo-data"
)

func getLatestCkpMeta(fs fileservice.FileService, dir string) (res string) {
	dirs, _ := fs.List(context.Background(), dir)
	var name string
	maxTs := types.BuildTS(0, 0)
	minTs := types.BuildTS(0, 0)
	for _, dir := range dirs {
		start, end, _ := blockio.DecodeCheckpointMetadataFileName(dir.Name)
		if start.EQ(&minTs) && end.GT(&maxTs) {
			maxTs = end
			name = dir.Name
		}
	}
	return name
}

func (c *replayArg) Run() error {
	blockio.Start("")
	defer blockio.Stop("")

	var dataFS, oldObjFS, newObjFS, rollbackFS fileservice.FileService

	ctx := context.Background()
	if c.rootDir != "" { // just for local test
		dataFS = migrate.NewFileFs(ctx, c.rootDir)
		oldObjFS = migrate.NewFileFs(ctx, path.Join(c.rootDir, oldObjDir))
		newObjFS = migrate.NewFileFs(ctx, path.Join(c.rootDir, newObjDir))
		rollbackFS = migrate.NewFileFs(ctx, path.Join(c.rootDir, rollbackDir))
	} else {
		dataFS = migrate.NewS3Fs(ctx, c.arg.Name, c.arg.Endpoint, c.arg.Bucket, c.arg.KeyPrefix)
		oldObjFS = migrate.NewS3Fs(ctx, c.arg.Name, c.arg.Endpoint, c.arg.Bucket, path.Join(c.arg.KeyPrefix, oldObjDir))
		newObjFS = migrate.NewS3Fs(ctx, c.arg.Name, c.arg.Endpoint, c.arg.Bucket, path.Join(c.arg.KeyPrefix, newObjDir))
		rollbackFS = migrate.NewS3Fs(ctx, c.arg.Name, c.arg.Endpoint, c.arg.Bucket, path.Join(c.arg.KeyPrefix, rollbackDir))
	}

	rollbackSinker := migrate.NewSinker(migrate.ObjectListSchema, rollbackFS)

	c.meta = getLatestCkpMeta(dataFS, ckpDir)

	// 1. Backup ckp meta files
	cleanDir(dataFS, ckpBakDir)
	migrate.BackupCkpDir(ctx, dataFS, ckpDir)
	migrate.BackupCkpDir(ctx, dataFS, gcDir)

	// 2. Clean ckp and gc dir
	cleanDir(dataFS, ckpDir)
	cleanDir(dataFS, gcDir)

	// 3. ListCkpFiles
	migrate.GetCkpFiles(ctx, dataFS, oldObjFS)

	// 4. ReadCkp11File
	fromEntry, ckpbats := migrate.ReadCkp11File(ctx, dataFS, filepath.Join(ckpBakDir, c.meta))

	// 5. Replay To 1.3 catalog
	cata := migrate.ReplayCatalogFromCkpData11(ckpbats)

	//dbIt := cata.MakeDBIt(false)
	//for ; dbIt.Valid(); dbIt.Next() {
	//	dbEntry := dbIt.Get().GetPayload()
	//	tblIt := dbEntry.MakeTableIt(false)
	//	for ; tblIt.Valid(); tblIt.Next() {
	//		tblEntry := tblIt.Get().GetPayload()
	//		fmt.Println(dbEntry.GetFullName(), tblEntry.GetFullName(), tblEntry.GetID())
	//	}
	//}

	// 6. Dump catalog to 3 tables batch
	bDb, bTbl, bCol, snapshotMeta := migrate.DumpCatalogToBatches(cata)

	// 7. Sink and get object stats
	objDB := migrate.SinkBatch(ctx, catalog.SystemDBSchema, bDb, dataFS)
	objTbl := migrate.SinkBatch(ctx, catalog.SystemTableSchema, bTbl, dataFS)
	objCol := migrate.SinkBatch(ctx, catalog.SystemColumnSchema, bCol, dataFS)

	{
		var ss []objectio.ObjectStats
		ss = append(ss, objDB...)
		ss = append(ss, objTbl...)
		ss = append(ss, objCol...)

		migrate.SinkObjectBatch(ctx, rollbackSinker, ss)
	}

	// 8. Write 1.3 Global Ckp
	txnNode := &txnbase.TxnMVCCNode{
		Start:   types.BuildTS(42424242, 0),
		Prepare: types.BuildTS(42424243, 0),
		End:     types.BuildTS(42424243, 0),
	}
	entryNode := &catalog.EntryMVCCNode{
		CreatedAt: types.BuildTS(42424243, 0),
	}

	migrate.RewriteCkp(ctx, cata, dataFS, newObjFS, rollbackFS,
		fromEntry, ckpbats, txnNode, entryNode, objDB, objTbl, objCol, c.tid)

	for _, v := range objDB {
		println(v.String())
	}
	for _, v := range objTbl {
		println(v.String())
	}
	for _, v := range objCol {
		println(v.String())
	}
	name := blockio.EncodeTableMetadataFileName(
		gc.PrefixAcctMeta,
		fromEntry.GetStart(),
		fromEntry.GetEnd(),
	)
	_, err := snapshotMeta.SaveTableInfo(gc.GCMetaDir+name, dataFS)
	if err != nil {
		println(err.Error())
		return err
	}
	return nil
}

type gcArg struct {
	rootDir string
	arg     fsArg
}

func (c *gcArg) PrepareCommand() *cobra.Command {
	gcCmd := &cobra.Command{
		Use:   "gc",
		Short: "gc checkpoint files",
		Run:   RunFactory(c),
	}

	gcCmd.Flags().StringP("root", "r", "", "root")
	gcCmd.Flags().StringP("input", "i", "", "input")

	return gcCmd
}

func (c *gcArg) FromCommand(cmd *cobra.Command) (err error) {
	c.rootDir = cmd.Flag("root").Value.String()
	cfg := cmd.Flag("input").Value.String()
	if c.rootDir == "" {
		c.arg, err = getFsArg(cfg)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func (c *gcArg) String() string {
	return ""
}

func (c *gcArg) Run() error {
	blockio.Start("")
	defer blockio.Stop("")

	ctx := context.Background()
	var fs fileservice.FileService
	if c.rootDir != "" { // just for local test
		fs = migrate.NewFileFs(ctx, c.rootDir)
	} else {
		fs = migrate.NewS3Fs(ctx, c.arg.Name, c.arg.Endpoint, c.arg.Bucket, c.arg.KeyPrefix)
	}

	migrate.GcCheckpointFiles(ctx, fs)

	return nil
}

type rollbackArg struct {
	rootDir string
	arg     fsArg
}

func (c *rollbackArg) PrepareCommand() *cobra.Command {
	gcCmd := &cobra.Command{
		Use:   "rollback",
		Short: "rollback changes",
		Run:   RunFactory(c),
	}

	gcCmd.Flags().StringP("root", "r", "", "root")
	gcCmd.Flags().StringP("input", "i", "", "input")

	return gcCmd
}

func (c *rollbackArg) FromCommand(cmd *cobra.Command) (err error) {
	c.rootDir = cmd.Flag("root").Value.String()
	cfg := cmd.Flag("input").Value.String()
	if c.rootDir == "" {
		c.arg, err = getFsArg(cfg)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func (c *rollbackArg) String() string {
	return ""
}

func (c *rollbackArg) Run() error {
	blockio.Start("")
	defer blockio.Stop("")

	ctx := context.Background()
	var fs fileservice.FileService
	if c.rootDir != "" { // just for local test
		fs = migrate.NewFileFs(ctx, c.rootDir)
	} else {
		fs = migrate.NewS3Fs(ctx, c.arg.Name, c.arg.Endpoint, c.arg.Bucket, c.arg.KeyPrefix)
	}

	migrate.Rollback(ctx, fs)

	return nil
}
