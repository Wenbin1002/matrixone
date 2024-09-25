package logtail

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"sort"
	"time"
)

const (
	MetaIDXV1 uint16 = iota

	DBInsertIDX
	DBInsertTxnIDX
	DBDeleteIDX
	DBDeleteTxnIDX

	TBLInsertIDX
	TBLInsertTxnIDX
	TBLDeleteIDX
	TBLDeleteTxnIDX
	TBLColInsertIDX
	TBLColDeleteIDX

	SEGInsertIDX
	SEGInsertTxnIDX
	SEGDeleteIDX
	SEGDeleteTxnIDX

	BLKMetaInsertIDX
	BLKMetaInsertTxnIDX
	BLKMetaDeleteIDX
	BLKMetaDeleteTxnIDX

	BLKTNMetaInsertIDX
	BLKTNMetaInsertTxnIDX
	BLKTNMetaDeleteIDX
	BLKTNMetaDeleteTxnIDX

	BLKCNMetaInsertIDX

	TNMetaIDXV1

	// supporting `show accounts` by recording extra
	// account related info in checkpoint

	StorageUsageInsIDXV1

	ObjectInfoIDXV1
	TNObjectInfoIDX

	StorageUsageDelIDXV1
)

const MaxIDXV1 = StorageUsageDelIDXV1 + 1

var checkpointDataSchemas_V11 [MaxIDXV1]*catalog.Schema

var items [MaxIDXV1]*checkpointDataItem

func init() {
	checkpointDataSchemas_V11 = [MaxIDXV1]*catalog.Schema{
		MetaSchemaV1,
		SystemDBSchema,
		TxnNodeSchema,
		DBDelSchema, // 3
		DBTNSchema,
		SystemTableSchema,
		TblTNSchema,
		TblDelSchema, // 7
		TblTNSchema,
		SystemColumnSchema,
		ColumnDelSchema,
		SegSchema, // 11
		SegTNSchema,
		DelSchema,
		SegTNSchema,
		BlkMetaSchema, // 15
		BlkTNSchema,
		DelSchema,
		BlkTNSchema,
		BlkMetaSchema, // 19
		BlkTNSchema,
		DelSchema,
		BlkTNSchema,
		BlkMetaSchema, // 23
		TNMetaSchemaV1,
		StorageUsageSchemaV1, // 25
		ObjectInfoSchemaV1,
		ObjectInfoSchemaV1,
		StorageUsageSchemaV1,
	}

	for idx, schema := range checkpointDataSchemas_V11 {
		items[idx] = &checkpointDataItem{
			schema,
			append(BaseTypes, schema.Types()...),
			append(BaseAttr, schema.Attrs()...),
		}
	}
}

type blockIdx struct {
	location objectio.Location
	dataType uint16
}

type CheckpointDataV1 struct {
	sid       string
	meta      map[uint64]*CheckpointMeta
	locations map[string]objectio.Location
	bats      [MaxIDXV1]*containers.Batch
	allocator *mpool.MPool
}

func (data *CheckpointDataV1) Print() {
	logutil.Infof("checkpoint data: %v", data.bats[24].Nameidx)
}

func NewCheckpointDataV1(
	sid string,
	mp *mpool.MPool,
) *CheckpointDataV1 {
	data := &CheckpointDataV1{
		sid:       sid,
		meta:      make(map[uint64]*CheckpointMeta),
		allocator: mp,
	}

	for idx, schema := range checkpointDataSchemas_V11 {
		data.bats[idx] = makeRespBatchFromSchemaV1(schema, mp)
	}
	return data
}

func makeRespBatchFromSchemaV1(schema *catalog.Schema, mp *mpool.MPool) *containers.Batch {
	bat := containers.NewBatch()

	bat.AddVector(
		"__mo_rowid",
		containers.MakeVector(types.T_Rowid.ToType(), mp),
	)
	bat.AddVector(
		"__mo_%1_commit_time",
		containers.MakeVector(types.T_TS.ToType(), mp),
	)
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

func (data *CheckpointDataV1) PrefetchFrom(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location) (err error) {
	blocks, area := vector.MustVarlenaRawData(data.bats[TNMetaIDXV1].GetVectorByName(CheckpointMetaAttr_BlockLocation).GetDownstreamVector())
	dataType := vector.MustFixedColNoTypeCheck[uint16](data.bats[TNMetaIDXV1].GetVectorByName(CheckpointMetaAttr_SchemaType).GetDownstreamVector())
	locations := make(map[string][]blockIdx)
	checkpointSize := uint64(0)
	for i := range blocks {
		location := objectio.Location(blocks[i].GetByteSlice(area))
		if location.IsEmpty() {
			continue
		}
		name := location.Name()
		if locations[name.String()] == nil {
			locations[name.String()] = make([]blockIdx, 0)
		}
		locations[name.String()] = append(locations[name.String()], blockIdx{location: location, dataType: dataType[i]})
	}
	for _, blockIdxes := range locations {
		checkpointSize += uint64(blockIdxes[0].location.Extent().End())
		logutil.Info("prefetch-read-checkpoint", common.OperationField("prefetch read"),
			common.OperandField("checkpoint"),
			common.AnyField("location", blockIdxes[0].location.String()),
			common.AnyField("size", checkpointSize))
		for _, idx := range blockIdxes {
			schema := items[idx.dataType]
			idxes := make([]uint16, len(schema.attrs))
			for attr := range schema.attrs {
				idxes[attr] = uint16(attr)
			}
		}
		err = blockio.Prefetch(data.sid, service, blockIdxes[0].location)
		if err != nil {
			logutil.Warnf("PrefetchFrom PrefetchWithMerged error %v", err)
		}
	}
	logutil.Info("prefetch-checkpoint",
		common.AnyField("size", checkpointSize),
		common.AnyField("count", len(locations)))
	return
}

func (data *CheckpointDataV1) PrefetchMeta(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location) (err error) {
	return blockio.Prefetch(data.sid, service, key)
}

func (data *CheckpointDataV1) ReadTNMetaBatch(
	ctx context.Context,
	version uint32,
	location objectio.Location,
	reader *blockio.BlockReader,
) (err error) {
	if data.bats[TNMetaIDXV1].Length() == 0 {
		var bats []*containers.Batch
		item := items[TNMetaIDXV1]
		bats, err = LoadBlkColumnsByMeta(version, ctx, item.types, item.attrs, TNMetaIDXV1, reader, data.allocator)
		if err != nil {
			return
		}
		data.bats[TNMetaIDXV1] = bats[0]
	}
	return
}

func (data *CheckpointDataV1) ReadFrom(
	ctx context.Context,
	version uint32,
	location objectio.Location,
	reader *blockio.BlockReader,
	fs fileservice.FileService,
) (err error) {
	err = data.readMetaBatch(ctx, version, reader, data.allocator)
	if err != nil {
		return
	}
	err = data.readAll(ctx, version, fs)
	if err != nil {
		return
	}

	return
}

func (data *CheckpointDataV1) readMetaBatch(
	ctx context.Context,
	version uint32,
	reader *blockio.BlockReader,
	_ *mpool.MPool,
) (err error) {
	if data.bats[MetaIDX].Length() == 0 {
		var bats []*containers.Batch
		item := items[MetaIDX]
		if bats, err = LoadBlkColumnsByMeta(
			version, ctx, item.types, item.attrs, uint16(0), reader, data.allocator,
		); err != nil {
			return
		}
		data.bats[MetaIDX] = bats[0]
	}
	return
}

func (data *CheckpointDataV1) readAll(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
) (err error) {
	data.replayMetaBatch(version)
	checkpointDataSize := uint64(0)
	readDuration := time.Now()
	for _, val := range data.locations {
		var reader *blockio.BlockReader
		reader, err = blockio.NewObjectReader(data.sid, service, val)
		if err != nil {
			return
		}
		var bats []*containers.Batch
		now := time.Now()
		for idx, item := range items {
			if uint16(idx) == MetaIDXV1 || uint16(idx) == TNMetaIDXV1 {
				continue
			}
			if bats, err = LoadBlkColumnsByMeta(
				version, ctx, item.types, item.attrs, uint16(idx), reader, data.allocator,
			); err != nil {
				return
			}
			for i := range bats {
				data.bats[idx].Append(bats[i])
				bats[i].Close()
			}
		}
		logutil.Info("read-checkpoint", common.OperationField("read"),
			common.OperandField("checkpoint"),
			common.AnyField("location", val.String()),
			common.AnyField("size", val.Extent().End()),
			common.AnyField("read cost", time.Since(now)))
		checkpointDataSize += uint64(val.Extent().End())
	}
	logutil.Info("read-all", common.OperationField("read"),
		common.OperandField("checkpoint"),
		common.AnyField("size", checkpointDataSize),
		common.AnyField("duration", time.Since(readDuration)))
	return
}

func (data *CheckpointDataV1) replayMetaBatch(version uint32) {
	bat := data.bats[MetaIDXV1]
	data.locations = make(map[string]objectio.Location)
	tidVec := vector.MustFixedColWithTypeCheck[uint64](bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	insVec := bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchLocation).GetDownstreamVector()
	delVec := bat.GetVectorByName(SnapshotMetaAttr_BlockCNInsertBatchLocation).GetDownstreamVector()
	delCNVec := bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchLocation).GetDownstreamVector()
	segVec := bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchLocation).GetDownstreamVector()

	var usageInsVec, usageDelVec *vector.Vector
	usageInsVec = bat.GetVectorByName(CheckpointMetaAttr_StorageUsageInsLocation).GetDownstreamVector()
	usageDelVec = bat.GetVectorByName(CheckpointMetaAttr_StorageUsageDelLocation).GetDownstreamVector()
	for i := 0; i < len(tidVec); i++ {
		tid := tidVec[i]
		if tid == 0 {
			bl := BlockLocations(insVec.GetBytesAt(i))
			it := bl.MakeIterator()
			for it.HasNext() {
				block := it.Next()
				if !block.GetLocation().IsEmpty() {
					data.locations[block.GetLocation().Name().String()] = block.GetLocation()
				}
			}
			continue
		}
		insLocation := insVec.GetBytesAt(i)
		delLocation := delVec.GetBytesAt(i)
		delCNLocation := delCNVec.GetBytesAt(i)
		segLocation := segVec.GetBytesAt(i)

		tmp := [][]byte{insLocation, delLocation, delCNLocation, segLocation}
		if usageInsVec != nil {
			tmp = append(tmp, usageInsVec.GetBytesAt(i))
			tmp = append(tmp, usageDelVec.GetBytesAt(i))
		}

		tableMeta := NewCheckpointMeta()
		tableMeta.DecodeFromString(tmp)
		data.meta[tid] = tableMeta
	}

	for _, meta := range data.meta {
		for _, table := range meta.tables {
			if table == nil {
				continue
			}

			it := table.locations.MakeIterator()
			for it.HasNext() {
				block := it.Next()
				if !block.GetLocation().IsEmpty() {
					data.locations[block.GetLocation().Name().String()] = block.GetLocation()
				}
			}
		}
	}
}

func (data *CheckpointDataV1) GetCheckpointMetaInfo(id uint64, limit int) (res *ObjectInfoJson, err error) {
	tombstone := make(map[string]struct{})
	tombstoneInfo := make(map[uint64]*tableinfo)

	insTableIDs := vector.MustFixedColWithTypeCheck[uint64](
		data.bats[ObjectInfoIDXV1].GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	insDeleteTSs := vector.MustFixedColWithTypeCheck[types.TS](
		data.bats[ObjectInfoIDXV1].GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector())
	files := make(map[uint64]*tableinfo)
	for i := range data.bats[ObjectInfoIDXV1].Length() {
		if files[insTableIDs[i]] == nil {
			files[insTableIDs[i]] = &tableinfo{
				tid: insTableIDs[i],
			}
		}
		deleteTs := insDeleteTSs[i]
		if deleteTs.IsEmpty() {
			files[insTableIDs[i]].add++
		} else {
			files[insTableIDs[i]].delete++
		}
	}

	tableinfos := make([]*tableinfo, 0)
	objectCount := uint64(0)
	addCount := uint64(0)
	deleteCount := uint64(0)
	for _, count := range files {
		tableinfos = append(tableinfos, count)
		objectCount += count.add
		addCount += count.add
		objectCount += count.delete
		deleteCount += count.delete
	}
	sort.Slice(tableinfos, func(i, j int) bool {
		return tableinfos[i].add > tableinfos[j].add
	})
	tableJsons := make([]TableInfoJson, 0, data.bats[ObjectInfoIDXV1].Length())
	tables := make(map[uint64]int)
	for i := range len(tableinfos) {
		tablejson := TableInfoJson{
			ID:     tableinfos[i].tid,
			Add:    tableinfos[i].add,
			Delete: tableinfos[i].delete,
		}
		if id == 0 || tablejson.ID == id {
			tables[tablejson.ID] = len(tableJsons)
			tableJsons = append(tableJsons, tablejson)
		}
	}
	tableinfos2 := make([]*tableinfo, 0)
	objectCount2 := uint64(0)
	addCount2 := uint64(0)
	for _, count := range tombstoneInfo {
		tableinfos2 = append(tableinfos2, count)
		objectCount2 += count.add
		addCount2 += count.add
	}
	sort.Slice(tableinfos2, func(i, j int) bool {
		return tableinfos2[i].add > tableinfos2[j].add
	})

	for i := range len(tableinfos2) {
		if idx, ok := tables[tableinfos2[i].tid]; ok {
			tablejson := &tableJsons[idx]
			tablejson.TombstoneRows = tableinfos2[i].add
			tablejson.TombstoneCount = tableinfos2[i].delete
			continue
		}
		tablejson := TableInfoJson{
			ID:             tableinfos2[i].tid,
			TombstoneRows:  tableinfos2[i].add,
			TombstoneCount: tableinfos2[i].delete,
		}
		if id == 0 || tablejson.ID == id {
			tableJsons = append(tableJsons, tablejson)
		}
	}

	res = &ObjectInfoJson{
		TableCnt:     len(tableJsons),
		ObjectCnt:    objectCount,
		ObjectAddCnt: addCount,
		ObjectDelCnt: deleteCount,
		TombstoneCnt: len(tombstone),
	}

	if id != invalid {
		if limit < len(tableJsons) {
			tableJsons = tableJsons[:limit]
		}
		res.Tables = tableJsons
	}

	return
}
