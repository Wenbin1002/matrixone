// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	testutil2 "github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_BasicInsert(t *testing.T) {
	var (
		//err          error
		mp        *mpool.MPool
		accountId = catalog.System_Account

		primaryKeyIdx int = 3

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	opt, err := testutil.GetS3SharedFileServiceOption(ctx, testutil.GetDefaultTestPath("test", t))
	require.NoError(t, err)

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{TaeEngineOptions: opt},
		t,
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	{
		const (
			tableName    = "tb01"
			databaseName = "db01"
		)

		schema := catalog2.MockSchemaEnhanced(4, primaryKeyIdx, 9)
		schema.Name = tableName

		_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
		require.NoError(t, err)

		// insert
		rowsCnt := 100
		bats := catalog2.MockBatch(schema, rowsCnt)
		_, relation, txn, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)
		err = relation.Write(ctx, containers.ToCNBatch(bats))
		require.NoError(t, err)

		// check
		ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
		ranges, err := relation.Ranges(ctx, nil, 0)
		ls, err := disttae.BuildLocalDataSource(ctx, relation, ranges, 0)
		reader, err := disttae.NewReader(
			ctx,
			testutil2.NewProcessWithMPool("", mp),
			disttaeEngine.Engine,
			relation.GetTableDef(ctx),
			txn.SnapshotTS(),
			nil,
			ls,
		)
		require.NoError(t, err)
		require.NoError(t, err)
		_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		require.Equal(t, rowsCnt, ret.RowCount())
		require.NoError(t, txn.Commit(ctx))

	}
}
