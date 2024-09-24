package logtail

import (
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type Scope = int

const (
	// changes for mo_databases
	ScopeDatabases Scope = iota + 1
	// changes for mo_tables
	ScopeTables
	// changes for mo_columns
	ScopeColumns
	// changes for user tables
	ScopeUserTables
)
const (
	ObjectAttr_ID                               = "id"
	ObjectAttr_CreateAt                         = "create_at"
	ObjectAttr_SegNode                          = "seg_node"
	ObjectAttr_State                            = "state"
	ObjectAttr_Sorted                           = "sorted"
	SnapshotAttr_BlockMaxRow                    = "block_max_row"
	SnapshotAttr_ObjectMaxBlock                 = "Object_max_block"
	SnapshotMetaAttr_BlockInsertBatchStart      = "block_insert_batch_start"
	SnapshotMetaAttr_BlockInsertBatchEnd        = "block_insert_batch_end"
	SnapshotMetaAttr_BlockDeleteBatchStart      = "block_delete_batch_start"
	SnapshotMetaAttr_BlockDeleteBatchEnd        = "block_delete_batch_end"
	SnapshotMetaAttr_BlockCNInsertBatchLocation = "block_cn_insert_batch_location"
	SnapshotMetaAttr_SegDeleteBatchStart        = "seg_delete_batch_start"
	SnapshotMetaAttr_SegDeleteBatchEnd          = "seg_delete_batch_end"
	SnapshotMetaAttr_SegDeleteBatchLocation     = "seg_delete_batch_location"

	AccountIDDbNameTblName = "account_id_db_name_tbl_name"
	AccountIDDbName        = "account_id_db_name"

	AttrRowID    = "__mo_rowid"
	AttrCommitTs = "__mo_%1_commit_time"

	SnapshotAttr_SchemaExtra = "schema_extra"
)

var (
	// for blk meta response
	BlkMetaSchema      *catalog.Schema // latest version
	BlkMetaSchema_V1   *catalog.Schema // previous version
	DelSchema          *catalog.Schema
	SegSchema          *catalog.Schema
	TxnNodeSchema      *catalog.Schema
	DBTNSchema         *catalog.Schema
	TblTNSchema        *catalog.Schema
	SegTNSchema        *catalog.Schema
	BlkTNSchema        *catalog.Schema
	MetaSchemaV1       *catalog.Schema
	DBDelSchema        *catalog.Schema
	TblDelSchema       *catalog.Schema
	ColumnDelSchema    *catalog.Schema
	TNMetaSchemaV1     *catalog.Schema
	ObjectInfoSchemaV1 *catalog.Schema

	DBSpecialDeleteSchema  *catalog.Schema
	TBLSpecialDeleteSchema *catalog.Schema

	StorageUsageSchemaV1 *catalog.Schema
)

var (
	DBSpecialDeleteAttr = []string{
		pkgcatalog.SystemDBAttr_ID,
		AccountIDDbName,
	}
	DBSpecialDeleteTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_varchar, 0, 0),
	}
	TBLSpecialDeleteAttr = []string{
		pkgcatalog.SystemRelAttr_ID,
		AccountIDDbNameTblName,
	}
	TBLSpecialDeleteTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_varchar, 0, 0),
	}
	ObjectSchemaAttr = []string{
		ObjectAttr_ID,
		ObjectAttr_CreateAt,
		ObjectAttr_SegNode,
	}
	ObjectSchemaTypes = []types.Type{
		types.New(types.T_uuid, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_blob, 0, 0),
	}
	TxnNodeSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
	}
	TxnNodeSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint32, 0, 0),
	}
	DBTNSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
	}
	DBTNSchemaType = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
	}
	TblTNSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
		SnapshotAttr_BlockMaxRow,
		SnapshotAttr_ObjectMaxBlock,
		SnapshotAttr_SchemaExtra,
	}
	TblTNSchemaType = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint16, 0, 0),
		types.New(types.T_varchar, 0, 0),
	}
	ObjectTNSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
	}
	ObjectTNSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
	}
	BlockTNSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
		pkgcatalog.BlockMeta_MetaLoc,
		pkgcatalog.BlockMeta_DeltaLoc,
	}
	BlockTNSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
	}
	MetaSchemaAttrV1 = []string{
		SnapshotAttr_TID,
		SnapshotMetaAttr_BlockInsertBatchLocation,
		SnapshotMetaAttr_BlockCNInsertBatchLocation,
		SnapshotMetaAttr_BlockDeleteBatchLocation,
		SnapshotMetaAttr_SegDeleteBatchLocation,
		CheckpointMetaAttr_StorageUsageInsLocation,
		CheckpointMetaAttr_StorageUsageDelLocation,
	}

	MetaShcemaTypesV1 = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
	}
	DBDelSchemaAttr = []string{
		pkgcatalog.SystemDBAttr_ID,
	}
	DBDelSchemaTypes = []types.Type{
		types.T_uint64.ToType(),
	}
	TblDelSchemaAttr = []string{
		pkgcatalog.SystemRelAttr_ID,
	}
	TblDelSchemaTypes = []types.Type{
		types.T_uint64.ToType(),
	}
	ColumnDelSchemaAttr = []string{
		pkgcatalog.SystemColAttr_UniqName,
	}
	ColumnDelSchemaTypes = []types.Type{
		types.T_varchar.ToType(),
	}
	TNMetaSchemaAttrV1 = []string{
		CheckpointMetaAttr_BlockLocation,
		CheckpointMetaAttr_SchemaType,
	}
	TNMetaShcemaTypesV1 = []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_uint16, 0, 0),
	}

	BaseAttrV1 = []string{
		AttrRowID,
		AttrCommitTs,
	}
	BaseTypesV1 = []types.Type{
		types.T_Rowid.ToType(),
		types.T_TS.ToType(),
	}
	ObjectInfoAttrV1 = []string{
		ObjectAttr_ObjectStats,
		ObjectAttr_State, // entry_state, true for appendable
		ObjectAttr_Sorted,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
		EntryNode_CreateAt,
		EntryNode_DeleteAt,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
	}
	ObjectInfoTypesV1 = []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_bool, 0, 0),
		types.New(types.T_bool, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
	}

	StorageUsageSchemaAttrsV1 = []string{
		pkgcatalog.SystemColAttr_AccID,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
		CheckpointMetaAttr_ObjectID,
		CheckpointMetaAttr_ObjectSize,
	}

	StorageUsageSchemaTypesV1 = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uuid, 0, 0),
		types.New(types.T_uint64, 0, 0),
	}
)

func init() {

	DBSpecialDeleteSchema = catalog.NewEmptySchema("db_special_delete")

	for i, colname := range DBSpecialDeleteAttr {
		if i == 0 {
			if err := DBSpecialDeleteSchema.AppendPKCol(colname, DBSpecialDeleteTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := DBSpecialDeleteSchema.AppendCol(colname, DBSpecialDeleteTypes[i]); err != nil {
				panic(err)
			}
		}
	}
	if err := DBSpecialDeleteSchema.Finalize(true); err != nil { // no phyaddr column
		panic(err)
	}

	TBLSpecialDeleteSchema = catalog.NewEmptySchema("tbl_special_delete")

	for i, colname := range TBLSpecialDeleteAttr {
		if i == 0 {
			if err := TBLSpecialDeleteSchema.AppendPKCol(colname, TBLSpecialDeleteTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := TBLSpecialDeleteSchema.AppendCol(colname, TBLSpecialDeleteTypes[i]); err != nil {
				panic(err)
			}
		}
	}
	if err := TBLSpecialDeleteSchema.Finalize(true); err != nil { // no phyaddr column
		panic(err)
	}

	BlkMetaSchema = catalog.NewEmptySchema("blkMeta")

	for i, colname := range pkgcatalog.MoTableMetaSchema {
		if i == 0 {
			if err := BlkMetaSchema.AppendPKCol(colname, pkgcatalog.MoTableMetaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := BlkMetaSchema.AppendCol(colname, pkgcatalog.MoTableMetaTypes[i]); err != nil {
				panic(err)
			}
		}
	}
	if err := BlkMetaSchema.Finalize(true); err != nil { // no phyaddr column
		panic(err)
	}

	// empty schema, no finalize, makeRespBatchFromSchema will add necessary colunms
	DelSchema = catalog.NewEmptySchema("del")

	SegSchema = catalog.NewEmptySchema("Object")
	for i, colname := range ObjectSchemaAttr {
		if i == 0 {
			if err := SegSchema.AppendPKCol(colname, ObjectSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := SegSchema.AppendCol(colname, ObjectSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	TxnNodeSchema = catalog.NewEmptySchema("txn_node")
	for i, colname := range TxnNodeSchemaAttr {
		if i == 0 {
			if err := TxnNodeSchema.AppendPKCol(colname, TxnNodeSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := TxnNodeSchema.AppendCol(colname, TxnNodeSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	DBTNSchema = catalog.NewEmptySchema("db_dn")
	for i, colname := range DBTNSchemaAttr {
		if i == 0 {
			if err := DBTNSchema.AppendPKCol(colname, DBTNSchemaType[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := DBTNSchema.AppendCol(colname, DBTNSchemaType[i]); err != nil {
				panic(err)
			}
		}
	}

	TblTNSchema = catalog.NewEmptySchema("table_dn")
	for i, colname := range TblTNSchemaAttr {
		if i == 0 {
			if err := TblTNSchema.AppendPKCol(colname, TblTNSchemaType[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := TblTNSchema.AppendCol(colname, TblTNSchemaType[i]); err != nil {
				panic(err)
			}
		}
	}

	SegTNSchema = catalog.NewEmptySchema("Object_dn")
	for i, colname := range ObjectTNSchemaAttr {
		if i == 0 {
			if err := SegTNSchema.AppendPKCol(colname, ObjectTNSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := SegTNSchema.AppendCol(colname, ObjectTNSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	BlkTNSchema = catalog.NewEmptySchema("block_dn")
	for i, colname := range BlockTNSchemaAttr {
		if i == 0 {
			if err := BlkTNSchema.AppendPKCol(colname, BlockTNSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := BlkTNSchema.AppendCol(colname, BlockTNSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	MetaSchemaV1 = catalog.NewEmptySchema("meta")
	for i, colname := range MetaSchemaAttr {
		if i == 0 {
			if err := MetaSchemaV1.AppendPKCol(colname, MetaShcemaTypesV1[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := MetaSchemaV1.AppendCol(colname, MetaShcemaTypesV1[i]); err != nil {
				panic(err)
			}
		}
	}
	//logutil.Infof("MetaSchemaV1 %v", MetaSchemaV1)

	DBDelSchema = catalog.NewEmptySchema("meta")
	for i, colname := range DBDelSchemaAttr {
		if i == 0 {
			if err := DBDelSchema.AppendPKCol(colname, DBDelSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := DBDelSchema.AppendCol(colname, DBDelSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	TblDelSchema = catalog.NewEmptySchema("meta")
	for i, colname := range TblDelSchemaAttr {
		if i == 0 {
			if err := TblDelSchema.AppendPKCol(colname, TblDelSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := TblDelSchema.AppendCol(colname, TblDelSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	ColumnDelSchema = catalog.NewEmptySchema("meta")
	for i, colname := range ColumnDelSchemaAttr {
		if i == 0 {
			if err := ColumnDelSchema.AppendPKCol(colname, ColumnDelSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := ColumnDelSchema.AppendCol(colname, ColumnDelSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	TNMetaSchemaV1 = catalog.NewEmptySchema("meta")
	for i, colname := range TNMetaSchemaAttr {
		if i == 0 {
			if err := TNMetaSchemaV1.AppendPKCol(colname, TNMetaShcemaTypesV1[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := TNMetaSchemaV1.AppendCol(colname, TNMetaShcemaTypesV1[i]); err != nil {
				panic(err)
			}
		}
	}

	ObjectInfoSchemaV1 = catalog.NewEmptySchema("object_info")
	for i, colname := range ObjectInfoAttr {
		if i == 0 {
			if err := ObjectInfoSchemaV1.AppendPKCol(colname, ObjectInfoTypesV1[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := ObjectInfoSchemaV1.AppendCol(colname, ObjectInfoTypesV1[i]); err != nil {
				panic(err)
			}
		}
	}

	StorageUsageSchemaV1 = catalog.NewEmptySchema("storage_usage")
	for i, colname := range StorageUsageSchemaAttrsV1 {
		if i == 0 {
			if err := StorageUsageSchemaV1.AppendPKCol(colname, StorageUsageSchemaTypesV1[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := StorageUsageSchemaV1.AppendCol(colname, StorageUsageSchemaTypesV1[i]); err != nil {
				panic(err)
			}
		}
	}
}
