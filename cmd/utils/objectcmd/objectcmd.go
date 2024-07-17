// Copyright 2021 Matrix Origin
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

package objectcmd

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/spf13/cobra"
	"strconv"
	"strings"
)

var (
	level      int
	name       string
	id         int
	cols, rows []int
	col, row   string
	reader     *objectio.ObjectReader
)

const (
	invalidId = 0x3f3f3f3f

	brief    = 0
	standard = 1
	detailed = 2
)

func getInputs(input string, result *[]int) error {
	*result = make([]int, 0)
	if input == "" {
		return nil
	}
	items := strings.Split(input, ",")
	for _, item := range items {
		item = strings.TrimSpace(item)
		num, err := strconv.Atoi(item)
		if err != nil {
			return fmt.Errorf("invalid number '%s'", item)
		}
		*result = append(*result, num)
	}
	return nil
}

func initReader(name string) error {
	c := fileservice.Config{
		Name:    defines.SharedFileServiceName,
		Backend: "DISK",
		DataDir: defines.SharedFileServiceName,
		Cache:   fileservice.DisabledCacheConfig,
	}
	service, err := fileservice.NewFileService(context.Background(), c, nil)
	if err != nil {
		return err
	}
	reader, err = objectio.NewObjectReaderWithStr(name, service)

	return err
}

func GetCommands() (commands []*cobra.Command) {

	commands = append(commands, getStatCmd())
	commands = append(commands, getGetCmd())

	return
}

func getStatCmd() *cobra.Command {
	var statCmd = &cobra.Command{
		Use:   "stat",
		Short: "Perform a stat operation",
		Run: func(cmd *cobra.Command, args []string) {
			if err := checkStatCmdInputs(); err != nil {
				cmd.OutOrStdout().Write(
					[]byte(fmt.Sprintf("invalid inputs: %v\n", err)),
				)
				return
			}

			if err := initReader(name); err != nil {
				cmd.OutOrStdout().Write(
					[]byte(fmt.Sprintf("fail to init reader %v", err)),
				)
				return
			}

			cmd.OutOrStdout().Write(
				[]byte(getStat()),
			)
		},
	}

	statCmd.Flags().IntVarP(&id, "id", "i", invalidId, "id")
	statCmd.Flags().IntVarP(&level, "level", "l", brief, "level")
	statCmd.Flags().StringVarP(&name, "name", "n", "", "name")

	return statCmd
}

func checkStatCmdInputs() error {
	if level != brief && level != standard && level != detailed {
		return fmt.Errorf("invalid level %v, should be 0, 1, 2 ", level)
	}

	if name == "" {
		return fmt.Errorf("empty name")
	}

	return nil
}

func getStat() (res string) {
	var err error
	var m *mpool.MPool
	var meta objectio.ObjectMeta
	if m, err = mpool.NewMPool("data", 0, mpool.NoFixed); err != nil {
		return fmt.Sprintf("fail to init mpool, err: %v", err)
	}
	if meta, err = reader.ReadAllMeta(context.Background(), m); err != nil {
		return fmt.Sprintf("fail to read meta, err: %v", err)
	}

	switch level {
	case brief:
		res = getBriefStat(&meta)
	case standard:
		res = getStandardStat(&meta)
	case detailed:
		res = getDetailedStat(&meta)
	}
	return
}

func getBriefStat(obj *objectio.ObjectMeta) string {
	meta := *obj
	data, ok := meta.DataMeta()
	if !ok {
		return "no data"
	}

	cnt := data.BlockCount()
	header := data.BlockHeader()
	ext := reader.GetMetaExtent()
	return fmt.Sprintf("object %v has %v blocks, %v rows, %v cols, object size %v", name, cnt, header.Rows(), header.ColumnCount(), ext.Length())
}

func getStandardStat(obj *objectio.ObjectMeta) string {
	meta := *obj
	data, ok := meta.DataMeta()
	if !ok {
		return "no data"
	}

	var blocks []objectio.BlockObject
	cnt := data.BlockCount()

	if id != invalidId {
		println(uint32(id))
		if uint32(id) > cnt {
			return fmt.Sprintf("id %3d out of block count %3d", id, cnt)
		}
		blocks = append(blocks, data.GetBlockMeta(uint32(id)))
	} else {
		for i := range cnt {
			blk := data.GetBlockMeta(i)
			blocks = append(blocks, blk)
		}
	}

	var res string
	header := data.BlockHeader()
	ext := reader.GetMetaExtent()
	res += fmt.Sprintf("object %v has %v blocks, %v rows, %v cols, object size %v\n", name, cnt, header.Rows(), header.ColumnCount(), ext.Length())
	for _, blk := range blocks {
		res += fmt.Sprintf("block %3d: rows %4v, cols %3v\n", blk.GetID(), blk.GetRows(), blk.GetColumnCount())
	}

	return res
}

func getDetailedStat(obj *objectio.ObjectMeta) string {
	meta := *obj
	data, ok := meta.DataMeta()
	if !ok {
		return "no data"
	}

	var blocks []objectio.BlockObject
	cnt := data.BlockCount()
	if id != invalidId {
		if uint32(id) >= cnt {
			return fmt.Sprintf("id %v out of block count %v", id, cnt)
		}
		blocks = append(blocks, data.GetBlockMeta(uint32(id)))
	} else {
		for i := range cnt {
			blk := data.GetBlockMeta(i)
			blocks = append(blocks, blk)
		}
	}

	var res string
	res += fmt.Sprintf("object %v has %3d blocks\n", name, cnt)
	for _, blk := range blocks {
		cnt := blk.GetColumnCount()
		res += fmt.Sprintf("block %3d has %3d cloumns\n", blk.GetID(), cnt)

		for i := range cnt {
			col := blk.ColumnMeta(i)
			res += fmt.Sprintf("    cloumns %3d, ndv %3d, null cnt %3d, zonemap %v\n", i, col.Ndv(), col.NullCnt(), col.ZoneMap())
		}
	}

	return res
}

func getGetCmd() *cobra.Command {
	var getCmd = &cobra.Command{
		Use:   "get",
		Short: "Perform a get operation",
		Run: func(cmd *cobra.Command, args []string) {
			if err := checkGecCmdInputs(); err != nil {
				cmd.OutOrStdout().Write(
					[]byte(fmt.Sprintf("invalid inputs: %v\n", err)),
				)
				return
			}

			if err := initReader(name); err != nil {
				cmd.OutOrStdout().Write(
					[]byte(fmt.Sprintf("init reader error: %v\n", err)),
				)
				return
			}

			cmd.OutOrStdout().Write(
				[]byte(getData()),
			)
		},
	}
	getCmd.Flags().IntVarP(&id, "id", "i", invalidId, "id")
	getCmd.Flags().StringVarP(&name, "name", "n", "", "name")
	getCmd.Flags().StringVarP(&col, "col", "c", "", "col")
	getCmd.Flags().StringVarP(&row, "row", "r", "", "row")

	return getCmd
}

func checkGecCmdInputs() error {
	if err := getInputs(col, &cols); err != nil {
		return err
	}
	if err := getInputs(row, &rows); err != nil {
		return err
	}
	if len(rows) == 1 || len(rows) > 2 || rows[0] >= rows[1] {
		return fmt.Errorf("invalid rows, need two inputs [leftm, right)")
	}
	if name == "" {
		return fmt.Errorf("empty name")
	}

	return nil
}

func getData() string {
	var err error
	var m *mpool.MPool
	var meta objectio.ObjectMeta
	if m, err = mpool.NewMPool("data", 0, mpool.NoFixed); err != nil {
		return fmt.Sprintf("fail to init mpool, err: %v", err)
	}
	if meta, err = reader.ReadAllMeta(context.Background(), m); err != nil {
		return fmt.Sprintf("fail to read meta, err: %v", err)
	}

	cnt := meta.DataMetaCount()
	if id == invalidId || uint16(id) >= cnt {
		return "invalid id"
	}
	var res string

	blocks, _ := meta.DataMeta()
	blk := blocks.GetBlockMeta(uint32(id))
	cnt = blk.GetColumnCount()
	idxs := make([]uint16, 0)
	typs := make([]types.Type, 0)
	if len(cols) == 0 {
		for i := range cnt {
			cols = append(cols, int(i))
		}
	}
	for _, i := range cols {
		idx := uint16(i)
		if idx >= cnt {
			return fmt.Sprintf("column %v out of colum count %v", idx, cnt)
		}
		col := blk.ColumnMeta(idx)
		idxs = append(idxs, idx)
		tp := types.T(col.DataType()).ToType()
		typs = append(typs, tp)
	}

	v, _ := reader.ReadOneBlock(context.Background(), idxs, typs, uint16(id), m)
	for i, entry := range v.Entries {
		obj, _ := objectio.Decode(entry.CachedData.Bytes())
		vec := obj.(*vector.Vector)
		if len(rows) != 0 {
			vec, err = vec.Window(rows[0], rows[1])
			if err != nil {
				return fmt.Sprintf("invalid rows %v, err %v", rows, err)
			}
		}
		res += fmt.Sprintf("col %d:\n%v\n", cols[i], vec.String())
	}

	return res
}
