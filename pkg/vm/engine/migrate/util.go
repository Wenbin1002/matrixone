package migrate

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func makeRespBatchFromSchema(schema *catalog.Schema, mp *mpool.MPool) *containers.Batch {
	bat := containers.NewBatch()

	bat.AddVector(
		pkgcatalog.Row_ID,
		containers.MakeVector(types.T_Rowid.ToType(), mp),
	)
	bat.AddVector(
		pkgcatalog.TableTailAttrCommitTs,
		containers.MakeVector(types.T_TS.ToType(), mp),
	)
	return MakeBasicRespBatchFromSchema(schema, mp, bat)
}

func MakeBasicRespBatchFromSchema(schema *catalog.Schema, mp *mpool.MPool, base *containers.Batch) *containers.Batch {
	var bat *containers.Batch
	if base == nil {
		bat = containers.NewBatch()
	} else {
		bat = base
	}

	// Types() is not used, then empty schema can also be handled here
	typs := schema.AllTypes()
	attrs := schema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(
			attr,
			containers.MakeVector(typs[i], mp),
		)
	}
	return bat
}

// ReadFile read all data from file
func ReadFile(fs fileservice.FileService, file string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	vec := &fileservice.IOVector{
		FilePath: file,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   -1,
			},
		},
	}
	if err := fs.Read(ctx, vec); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return vec.Entries[0].Data, nil
}

// WriteFile write data to file
func WriteFile(fs fileservice.FileService, file string, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	vec := fileservice.IOVector{
		FilePath: file,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   int64(len(data)),
				Data:   data,
			},
		},
	}
	return fs.Write(ctx, vec)
}

func BackupCkpDir(ctx context.Context, fs fileservice.FileService, dir string) {
	bakdir := dir + "-bak"

	{
		entries, _ := fs.List(context.Background(), bakdir)
		for _, entry := range entries {
			fs.Delete(ctx, bakdir+"/"+entry.Name)
		}
	}

	entries, err := fs.List(ctx, dir)
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		if entry.IsDir {
			panic("bad ckp dir")
		}
	}
	logutil.Infof("backup ckp dir %s to %s, %v entries", dir, bakdir, len(entries))
	for i, entry := range entries {
		data, err := ReadFile(fs, dir+"/"+entry.Name)
		if err != nil {
			panic(err)
		}
		if err := WriteFile(fs, bakdir+"/"+entry.Name, data); err != nil {
			panic(err)
		}
		if i%5 == 0 {
			logutil.Infof("backup %d/%d %s", i, len(entries), entry.Name)
		}
	}
}

func NewS3Fs(ctx context.Context, name, endpoint, bucket, keyPrefix string) fileservice.FileService {
	arg := fileservice.ObjectStorageArguments{
		Name:      name,
		Endpoint:  endpoint,
		Bucket:    bucket,
		KeyPrefix: keyPrefix,
	}
	fs, err := fileservice.NewS3FS(ctx, arg, fileservice.DisabledCacheConfig, nil, false, false)
	if err != nil {
		panic(err)
	}
	return fs
}

func RollbackDir(ctx context.Context, fs fileservice.FileService, dir string) {
	bakdir := dir + "-bak"

	{
		entries, _ := fs.List(context.Background(), dir)
		for _, entry := range entries {
			fs.Delete(ctx, dir+"/"+entry.Name)
		}
	}

	entries, err := fs.List(ctx, bakdir)
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		if entry.IsDir {
			panic("bad ckp dir")
		}
	}
	for i, entry := range entries {
		data, err := ReadFile(fs, bakdir+"/"+entry.Name)
		if err != nil {
			panic(err)
		}
		if err := WriteFile(fs, dir+"/"+entry.Name, data); err != nil {
			panic(err)
		}
		if i%5 == 0 {
			logutil.Infof("rollback %d/%d %s", i, len(entries), entry.Name)
		}
	}
}

func cleanDir(fs fileservice.FileService, dir string) {
	ctx := context.Background()
	entries, _ := fs.List(ctx, dir)
	for _, entry := range entries {
		err := fs.Delete(ctx, dir+"/"+entry.Name)
		if err != nil {
			logutil.Infof("asdf delete %s/%s failed", dir, entry.Name)
		}
	}
}

func SinkObjectBatch(ctx context.Context, sinker *engine_util.Sinker, ss []objectio.ObjectStats) {
	bat := MakeBasicRespBatchFromSchema(ObjectListSchema, common.CheckpointAllocator, nil)

	for _, s := range ss {
		objid := s.ObjectLocation().Name().String()
		bat.Vecs[0].Append([]byte(objid), false)
	}

	if err := sinker.Write(ctx, containers.ToCNBatch(bat)); err != nil {
		panic(err)
	}
	if err := sinker.Sync(ctx); err != nil {
		panic(err)
	}
}
