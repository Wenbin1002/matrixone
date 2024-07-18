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

package main

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

const (
	invalidId = 0x3f3f3f3f

	brief    = 0
	standard = 1
	detailed = 2
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "mo_object",
		Short: "A tool provides object visualization",
	}

	stat := &StatArg{}
	rootCmd.AddCommand(stat.PrepareCmd())

	get := &GetArg{}
	rootCmd.AddCommand(get.PrepareCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

type Cmd interface {
	FromCommand(cmd *cobra.Command) error
	String() string
	Run() error
}

func RunFactory[T Cmd](t T) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		if err := t.FromCommand(cmd); err != nil {
			cmd.OutOrStdout().Write([]byte(fmt.Sprintf("parse err: %v", err)))
			return
		}
		err := t.Run()
		if err != nil {
			cmd.OutOrStdout().Write(
				[]byte(fmt.Sprintf("run err: %v", err)),
			)
		} else {
			cmd.OutOrStdout().Write(
				[]byte(fmt.Sprintf("success. arg %v", t.String())),
			)
		}
	}
}

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
			return moerr.NewInfoNoCtx(fmt.Sprintf("invalid number '%s'", item))
		}
		*result = append(*result, num)
	}
	return nil
}

type StatArg struct {
	level  int
	name   string
	id     int
	fs     fileservice.FileService
	reader *objectio.ObjectReader
	res    string
}

func (c *StatArg) PrepareCmd() *cobra.Command {
	var statCmd = &cobra.Command{
		Use:   "stat",
		Short: "Perform a stat operation",
		Run:   RunFactory(c),
	}

	statCmd.Flags().IntP("id", "i", invalidId, "id")
	statCmd.Flags().IntP("level", "l", brief, "level")
	statCmd.Flags().StringP("name", "n", "", "name")

	return statCmd
}

func (c *StatArg) FromCommand(cmd *cobra.Command) (err error) {
	c.id, _ = cmd.Flags().GetInt("id")
	c.name, _ = cmd.Flags().GetString("name")

	logutil.Infof("objectcmd fs %v", c.fs)
	return nil
}

func (c *StatArg) String() string {
	return fmt.Sprintf("\n%v", c.res)
}

func (c *StatArg) Run() (err error) {
	if err = c.checkInputs(); err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("invalid inputs: %v\n", err))
	}

	if err = c.InitReader(c.name, c.fs); err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("fail to init reader %v", err))
	}

	c.res, err = c.GetStat()

	return
}

func (c *StatArg) InitReader(name string, fs fileservice.FileService) (err error) {
	if fs == nil {
		cfg := fileservice.Config{
			Name:    defines.SharedFileServiceName,
			Backend: "DISK",
			DataDir: defines.SharedFileServiceName,
			Cache:   fileservice.DisabledCacheConfig,
		}
		if fs, err = fileservice.NewFileService(context.Background(), cfg, nil); err != nil {
			return err
		}
	}
	c.reader, err = objectio.NewObjectReaderWithStr(name, fs)

	return err
}

func (c *StatArg) checkInputs() error {
	if c.level != brief && c.level != standard && c.level != detailed {
		return moerr.NewInfoNoCtx(fmt.Sprintf("invalid level %v, should be 0, 1, 2 ", c.level))
	}

	if c.name == "" {
		return moerr.NewInfoNoCtx(fmt.Sprintf("empty name"))
	}

	return nil
}

func (c *StatArg) GetStat() (res string, err error) {
	var m *mpool.MPool
	var meta objectio.ObjectMeta
	if m, err = mpool.NewMPool("data", 0, mpool.NoFixed); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("fail to init mpool, err: %v", err))
		return
	}
	if meta, err = c.reader.ReadAllMeta(context.Background(), m); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("fail to read meta, err: %v", err))
		return
	}

	switch c.level {
	case brief:
		res, err = c.GetBriefStat(&meta)
	case standard:
		res, err = c.GetStandardStat(&meta)
	case detailed:
		res, err = c.GetDetailedStat(&meta)
	}
	return
}

func (c *StatArg) GetBriefStat(obj *objectio.ObjectMeta) (res string, err error) {
	meta := *obj
	data, ok := meta.DataMeta()
	if !ok {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("no data"))
		return
	}

	cnt := data.BlockCount()
	header := data.BlockHeader()
	ext := c.reader.GetMetaExtent()
	res = fmt.Sprintf("object %v has %v blocks, %v rows, %v cols, object size %v", c.name, cnt, header.Rows(), header.ColumnCount(), ext.Length())

	return
}

func (c *StatArg) GetStandardStat(obj *objectio.ObjectMeta) (res string, err error) {
	meta := *obj
	data, ok := meta.DataMeta()
	if !ok {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("no data"))
		return
	}

	var blocks []objectio.BlockObject
	cnt := data.BlockCount()

	if c.id != invalidId {
		println(uint32(c.id))
		if uint32(c.id) > cnt {
			err = moerr.NewInfoNoCtx(fmt.Sprintf("id %3d out of block count %3d", c.id, cnt))
			return
		}
		blocks = append(blocks, data.GetBlockMeta(uint32(c.id)))
	} else {
		for i := range cnt {
			blk := data.GetBlockMeta(i)
			blocks = append(blocks, blk)
		}
	}

	header := data.BlockHeader()
	ext := c.reader.GetMetaExtent()
	res += fmt.Sprintf("object %v has %v blocks, %v rows, %v cols, object size %v\n", c.name, cnt, header.Rows(), header.ColumnCount(), ext.Length())
	for _, blk := range blocks {
		res += fmt.Sprintf("block %3d: rows %4v, cols %3v\n", blk.GetID(), blk.GetRows(), blk.GetColumnCount())
	}

	return
}

func (c *StatArg) GetDetailedStat(obj *objectio.ObjectMeta) (res string, err error) {
	meta := *obj
	data, ok := meta.DataMeta()
	if !ok {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("no data"))
		return
	}

	var blocks []objectio.BlockObject
	cnt := data.BlockCount()
	if c.id != invalidId {
		if uint32(c.id) >= cnt {
			err = moerr.NewInfoNoCtx(fmt.Sprintf("id %v out of block count %v", c.id, cnt))
			return
		}
		blocks = append(blocks, data.GetBlockMeta(uint32(c.id)))
	} else {
		for i := range cnt {
			blk := data.GetBlockMeta(i)
			blocks = append(blocks, blk)
		}
	}

	res += fmt.Sprintf("object %v has %3d blocks\n", c.name, cnt)
	for _, blk := range blocks {
		cnt := blk.GetColumnCount()
		res += fmt.Sprintf("block %3d has %3d cloumns\n", blk.GetID(), cnt)

		for i := range cnt {
			col := blk.ColumnMeta(i)
			res += fmt.Sprintf("    cloumns %3d, ndv %3d, null cnt %3d, zonemap %v\n", i, col.Ndv(), col.NullCnt(), col.ZoneMap())
		}
	}

	return
}

type GetArg struct {
	name       string
	id         int
	cols, rows []int
	col, row   string
	fs         fileservice.FileService
	reader     *objectio.ObjectReader
	res        string
}

func (c *GetArg) PrepareCmd() *cobra.Command {
	var getCmd = &cobra.Command{
		Use:   "get",
		Short: "Perform a get operation",
		Run:   RunFactory(c),
	}
	getCmd.Flags().IntP("id", "i", invalidId, "id")
	getCmd.Flags().StringP("name", "n", "", "name")
	getCmd.Flags().StringP("col", "c", "", "col")
	getCmd.Flags().StringP("row", "r", "", "row")

	return getCmd
}

func (c *GetArg) FromCommand(cmd *cobra.Command) (err error) {
	c.id, _ = cmd.Flags().GetInt("id")
	c.name, _ = cmd.Flags().GetString("name")
	c.col, _ = cmd.Flags().GetString("col")
	c.row, _ = cmd.Flags().GetString("row")

	return nil
}

func (c *GetArg) String() string {
	return fmt.Sprintf("\n%v", c.res)
}

func (c *GetArg) Run() (err error) {
	if err = c.checkInputs(); err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("invalid inputs: %v\n", err))
	}

	if err = c.InitReader(c.name, c.fs); err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("fail to init reader: %v", err))
	}

	c.res, err = c.GetData()

	return
}

func (c *GetArg) InitReader(name string, fs fileservice.FileService) (err error) {
	if fs == nil {
		cfg := fileservice.Config{
			Name:    defines.SharedFileServiceName,
			Backend: "DISK",
			DataDir: defines.SharedFileServiceName,
			Cache:   fileservice.DisabledCacheConfig,
		}
		if fs, err = fileservice.NewFileService(context.Background(), cfg, nil); err != nil {
			return err
		}
	}
	c.reader, err = objectio.NewObjectReaderWithStr(name, fs)

	return err
}

func (c *GetArg) checkInputs() error {
	if err := getInputs(c.col, &c.cols); err != nil {
		return err
	}
	if err := getInputs(c.row, &c.rows); err != nil {
		return err
	}
	if len(c.rows) > 2 || (len(c.rows) == 2 && c.rows[0] >= c.rows[1]) {
		return moerr.NewInfoNoCtx(fmt.Sprintf("invalid rows, need two inputs [leftm, right)"))
	}
	if c.name == "" {
		return moerr.NewInfoNoCtx(fmt.Sprintf("empty name"))
	}

	return nil
}

func (c *GetArg) GetData() (res string, err error) {
	var m *mpool.MPool
	var meta objectio.ObjectMeta
	if m, err = mpool.NewMPool("data", 0, mpool.NoFixed); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("fail to init mpool, err: %v", err))
		return
	}
	if meta, err = c.reader.ReadAllMeta(context.Background(), m); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("fail to read meta, err: %v", err))
		return
	}

	cnt := meta.DataMetaCount()
	if c.id == invalidId || uint16(c.id) >= cnt {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("invalid id"))
		return
	}

	blocks, _ := meta.DataMeta()
	blk := blocks.GetBlockMeta(uint32(c.id))
	cnt = blk.GetColumnCount()
	idxs := make([]uint16, 0)
	typs := make([]types.Type, 0)
	if len(c.cols) == 0 {
		for i := range cnt {
			c.cols = append(c.cols, int(i))
		}
	}
	for _, i := range c.cols {
		idx := uint16(i)
		if idx >= cnt {
			err = moerr.NewInfoNoCtx(fmt.Sprintf("column %v out of colum count %v", idx, cnt))
			return
		}
		col := blk.ColumnMeta(idx)
		idxs = append(idxs, idx)
		tp := types.T(col.DataType()).ToType()
		typs = append(typs, tp)
	}

	v, _ := c.reader.ReadOneBlock(context.Background(), idxs, typs, uint16(c.id), m)
	for i, entry := range v.Entries {
		obj, _ := objectio.Decode(entry.CachedData.Bytes())
		vec := obj.(*vector.Vector)
		if len(c.rows) != 0 {
			var left, right int
			left = c.rows[0]
			if len(c.rows) == 1 {
				right = left + 1
			} else {
				right = c.rows[1]
			}
			if uint32(left) >= blk.GetRows() || uint32(right) > blk.GetRows() {
				err = moerr.NewInfoNoCtx(fmt.Sprintf("invalid rows %v out of row count %v", c.rows, blk.GetRows()))
				return
			}
			vec, _ = vec.Window(left, right)
		}
		res += fmt.Sprintf("col %d:\n%v\n", c.cols[i], vec)
	}

	return
}
