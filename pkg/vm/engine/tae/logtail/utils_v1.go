package logtail

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
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

func init() {
	checkpointDataSchemas_V11 = [MaxIDXV1]*catalog.Schema{
		MetaSchemaV1,
		catalog.SystemDBSchema,
		TxnNodeSchema,
		DBDelSchema, // 3
		DBTNSchema,
		catalog.SystemTableSchema,
		TblTNSchema,
		TblDelSchema, // 7
		TblTNSchema,
		catalog.SystemColumnSchema,
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
		data.bats[idx] = makeRespBatchFromSchema(schema, mp)
	}
	return data
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
			schema := checkpointDataReferVersions[version][idx.dataType]
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
		bats, err = LoadBlkColumnsByMeta(version, ctx, TNMetaShcemaTypesV1, TNMetaSchemaAttrV1, TNMetaIDXV1, reader, data.allocator)
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
	//err = data.readMetaBatch(ctx, version, reader, data.allocator)
	//if err != nil {
	//	return
	//}
	//err = data.readAll(ctx, version, fs)
	//if err != nil {
	//	return
	//}

	return
}

func prefetchCheckpointDataV1(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location,
) (err error) {
	//var pref blockio.PrefetchParams
	//pref, err = blockio.BuildPrefetchParams(service, key)
	//if err != nil {
	//	return
	//}
	//for idx, item := range checkpointDataReferVersions[version] {
	//	idxes := make([]uint16, len(item.attrs))
	//	for i := range item.attrs {
	//		idxes[i] = uint16(i)
	//	}
	//	pref.AddBlock(idxes, []uint16{uint16(idx)})
	//}
	//return blockio.PrefetchWithMerged(pref)
	return nil
}
