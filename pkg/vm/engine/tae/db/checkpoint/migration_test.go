package checkpoint

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sort"
	"sync"
	"testing"
)

const (
	sid   = "migration"
	fsDir = "/home/v/mo/matrixone/mo-data/shared"
)

func init() {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime(sid, rt)
	blockio.Start(sid)
}

func Test_Migration(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: fsDir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	fs := objectio.ObjectFS{
		Service: service,
		Dir:     fsDir,
	}
	assert.Nil(t, err)
	dirs, err := service.List(ctx, CheckpointDir)
	require.Nil(t, err)

	require.NotEqual(t, 0, len(dirs))
	metaFiles := make([]*MetaFile, 0)
	for i, dir := range dirs {
		start, end := blockio.DecodeCheckpointMetadataFileName(dir.Name)
		metaFiles = append(metaFiles, &MetaFile{
			start: start,
			end:   end,
			index: i,
		})
	}
	sort.Slice(metaFiles, func(i, j int) bool {
		return metaFiles[i].end.Less(&metaFiles[j].end)
	})
	targetIdx := metaFiles[len(metaFiles)-1].index
	dir := dirs[targetIdx]
	reader, err := blockio.NewFileReader(sid, service, CheckpointDir+dir.Name)
	require.Nil(t, err)
	bats, closeCB, err := reader.LoadAllColumns(ctx, nil, common.CheckpointAllocator)
	require.Nil(t, err)
	defer func() {
		if closeCB != nil {
			closeCB()
		}
	}()
	bat := containers.NewBatch()
	defer bat.Close()
	colNames := CheckpointSchema.Attrs()
	colTypes := CheckpointSchema.Types()
	var checkpointVersion int
	// in version 1, checkpoint metadata doesn't contain 'version'.
	vecLen := len(bats[0].Vecs)
	if vecLen < CheckpointSchemaColumnCountV1 {
		checkpointVersion = 1
	} else if vecLen < CheckpointSchemaColumnCountV2 {
		checkpointVersion = 2
	} else {
		checkpointVersion = 3
	}
	for i := range bats[0].Vecs {
		if len(bats) == 0 {
			continue
		}
		var vec containers.Vector
		if bats[0].Vecs[i].Length() == 0 {
			vec = containers.MakeVector(colTypes[i], common.CheckpointAllocator)
		} else {
			vec = containers.ToTNVector(bats[0].Vecs[i], common.CheckpointAllocator)
		}
		bat.AddVector(colNames[i], vec)
	}
	datas := make([]*logtail.CheckpointDataV1, bat.Length())

	entries, maxGlobalEnd := replayCheckpointEntries(bat, checkpointVersion)
	for _, entry := range entries {
		entry.sid = sid
	}

	emptyFile := make([]*CheckpointEntry, 0)
	var emptyFileMu sync.RWMutex
	closecbs := make([]func(), 0)
	readfn := func(i int, readType uint16) {
		checkpointEntry := entries[i]
		if checkpointEntry.end.Less(&maxGlobalEnd) {
			return
		}
		var err2 error
		if readType == PrefetchData {
			if err2 = checkpointEntry.PrefetchV1(ctx, &fs, datas[i]); err2 != nil {
				logutil.Warnf("read %v failed: %v", checkpointEntry.String(), err2)
			}
		} else if readType == PrefetchMetaIdx {
			datas[i], err = checkpointEntry.PrefetchMetaIdxV1(ctx, &fs)
			if err != nil {
				return
			}
		} else if readType == ReadMetaIdx {
			err = checkpointEntry.ReadMetaIdxV1(ctx, &fs, datas[i])
			if err != nil {
				return
			}
		} else {
			_ = checkpointEntry.ReadV1(ctx, &fs, datas[i])
			emptyFileMu.Lock()
			emptyFile = append(emptyFile, checkpointEntry)
			emptyFileMu.Unlock()
		}
	}
	defer func() {
		for _, cb := range closecbs {
			cb()
		}
	}()
	for i := 0; i < bat.Length(); i++ {
		metaLoc := objectio.Location(bat.GetVectorByName(CheckpointAttr_MetaLocation).Get(i).([]byte))

		err = blockio.PrefetchMeta(sid, service, metaLoc)
		if err != nil {
			return
		}
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, PrefetchMetaIdx)
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, ReadMetaIdx)
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, PrefetchData)
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, ReadData)
	}
}
