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

package model

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type HashPageTable = TransferTable[*TransferHashPage]

type BlockRead interface {
	LoadTableByBlock(loc objectio.Location, fs fileservice.FileService) (bat *batch.Batch, release func(), err error)
}

var (
	RD     BlockRead
	FS     fileservice.FileService
	rdOnce sync.Once
	fsOnce sync.Once
)

func SetBlockRead(rd BlockRead) {
	rdOnce.Do(func() {
		RD = rd
	})
	logutil.Infof("[TransferHashPage] RD init %v", RD != nil)
}

func SetFileService(fs fileservice.FileService) {
	fsOnce.Do(func() {
		FS = fs
	})
	logutil.Infof("[TransferHashPage] FS %v", FS)
}

type TransferHashPageParams struct {
	TTL     time.Duration
	DiskTTL time.Duration
}

type Option func(*TransferHashPageParams)

func WithTTL(ttl time.Duration) Option {
	return func(params *TransferHashPageParams) {
		params.TTL = ttl
	}
}

func WithDiskTTL(diskTTL time.Duration) Option {
	return func(params *TransferHashPageParams) {
		params.DiskTTL = diskTTL
	}
}

type TransferHashPage struct {
	common.RefHelper
	bornTS      time.Time
	id          *common.ID // not include blk offset
	hashmap     api.HashPageMap
	loc         atomic.Pointer[objectio.Location]
	params      TransferHashPageParams
	isTransient bool
	isPersisted int32
}

func NewTransferHashPage(id *common.ID, ts time.Time, isTransient bool, opts ...Option) *TransferHashPage {
	params := TransferHashPageParams{
		TTL:     10 * time.Second,
		DiskTTL: 10 * time.Minute,
	}
	for _, opt := range opts {
		opt(&params)
	}

	page := &TransferHashPage{
		bornTS:      ts,
		id:          id,
		hashmap:     api.HashPageMap{M: make(map[uint32][]byte)},
		params:      params,
		isTransient: isTransient,
		isPersisted: 0,
	}
	page.OnZeroCB = page.Close

	cl := getCleaner()
	cl.addPage(page)

	return page
}

func (page *TransferHashPage) ID() *common.ID    { return page.id }
func (page *TransferHashPage) BornTS() time.Time { return page.bornTS }

func (page *TransferHashPage) TTL(now time.Time, ttl time.Duration) bool {
	if page.isTransient {
		ttl /= 2
	}
	return now.After(page.bornTS.Add(ttl))
}

func (page *TransferHashPage) Close() {
	logutil.Debugf("Closing %s", page.String())
	page.hashmap = api.HashPageMap{M: make(map[uint32][]byte)}
}

func (page *TransferHashPage) Length() int {
	return len(page.hashmap.M)
}

func (page *TransferHashPage) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("hashpage[%s][%s][Len=%d]",
		page.id.BlockString(),
		page.bornTS.String(),
		len(page.hashmap.M)))
	return w.String()
}

func (page *TransferHashPage) Pin() *common.PinnedItem[*TransferHashPage] {
	page.Ref()
	return &common.PinnedItem[*TransferHashPage]{
		Val: page,
	}
}

func (page *TransferHashPage) Train(from uint32, to types.Rowid) {
	page.hashmap.M[from] = to[:]
	v2.TransferRowTotalCounter.Inc()
}

func (page *TransferHashPage) Transfer(from uint32) (dest types.Rowid, ok bool) {
	if atomic.LoadInt32(&page.isPersisted) == 1 {
		diskStart := time.Now()
		page.loadTable()
		v2.TransferDiskHitCounter.Inc()
		diskDuration := time.Since(diskStart)
		v2.TransferDurationDiskGauge.Set(1000 * diskDuration.Seconds())
	} else {
		v2.TransferMemoryHitCounter.Inc()
	}
	v2.TransferTotalHitCounter.Inc()

	memstart := time.Now()
	var m []byte
	m, ok = page.hashmap.M[from]
	if ok {
		dest = types.Rowid(m)
	}
	memduration := time.Since(memstart)
	v2.TransferDurationMemoryGauge.Set(1000 * memduration.Seconds())

	var str string
	if ok {
		str = "succeeded"
	} else {
		str = "failed"
	}
	logutil.Infof("[TransferHashPage] get transfer %v", str)
	return
}

func (page *TransferHashPage) Marshal() []byte {
	data, _ := proto.Marshal(&page.hashmap)
	return data
}

func (page *TransferHashPage) Unmarshal(data []byte) error {
	err := proto.Unmarshal(data, &page.hashmap)
	return err
}

func (page *TransferHashPage) SetLocation(location objectio.Location) {
	page.loc.Store(&location)
}

func (page *TransferHashPage) clearTable() {
	logutil.Infof("[TransferHashPage] clear hash table")
	clear(page.hashmap.M)
	atomic.StoreInt32(&page.isPersisted, 1)
}

func (page *TransferHashPage) loadTable() {
	logutil.Infof("[TransferHashPage] load persist table, objectname: %v", page.loc.Load().Name().String())
	if page.loc.Load() == nil {
		return
	}

	logutil.Infof("[TransferHashPage] loc %v, rd %v, fs %v", page.loc.Load().Name().String(), RD, FS)

	var bat *batch.Batch
	var release func()
	bat, release, err := RD.LoadTableByBlock(*page.loc.Load(), FS)
	defer release()
	if err != nil {
		logutil.Errorf("[TransferHashPage] load table failed, %v", err)
		return
	}
	err = page.Unmarshal(bat.Vecs[0].GetBytesAt(0))
	if err != nil {
		return
	}

	v2.TransferRowHitCounter.Add(float64(len(page.hashmap.M)))

	atomic.StoreInt32(&page.isPersisted, 0)

	cl := getCleaner()
	cl.addPage(page)
}

func (page *TransferHashPage) clearPersistTable() {
	if atomic.LoadInt32(&page.isPersisted) == 0 {
		return
	}
	atomic.StoreInt32(&page.isPersisted, 0)
	logutil.Infof("[TransferHashPage] clear persist table, objectname: %v", page.loc.Load().Name().String())
	FS.Delete(context.Background(), page.loc.Load().Name().String())
}

func (page *TransferHashPage) IsPersist() int32 {
	return atomic.LoadInt32(&page.isPersisted)
}

type TransferPageCleaner struct {
	latch          sync.Mutex
	pages          list.List
	persistedPages list.List
}

type transferPage struct {
	page *TransferHashPage
	ts   time.Time
}

var (
	Cleaner *TransferPageCleaner
	once    sync.Once
)

func getCleaner() *TransferPageCleaner {
	once.Do(func() {
		Cleaner = &TransferPageCleaner{}
		go Cleaner.Handler()
	})
	return Cleaner
}

func (c *TransferPageCleaner) addPage(page *TransferHashPage) {
	c.latch.Lock()
	defer c.latch.Unlock()
	c.pages.PushBack(&transferPage{page: page, ts: time.Now()})
	logutil.Infof("add page %v len %v", page, c.pages.Len())
}

func (c *TransferPageCleaner) Handler() {
	for {
		c.latch.Lock()

		var delPages []*list.Element
		for e := c.pages.Front(); e != nil; e = e.Next() {
			page := e.Value.(*transferPage)
			if time.Since(page.ts) > page.page.params.TTL {
				logutil.Infof("clean page %v", page)
				page.page.clearTable()
				c.persistedPages.PushBack(page)
				delPages = append(delPages, e)
			}
		}
		for _, e := range delPages {
			c.pages.Remove(e)
		}
		c.latch.Unlock()

		for e := c.persistedPages.Front(); e != nil; e = e.Next() {
			page := e.Value.(*transferPage)
			if time.Since(page.page.bornTS) > page.page.params.DiskTTL {
				page.page.clearPersistTable()
				delPages = append(delPages, e)
			}
		}
		for _, e := range delPages {
			c.persistedPages.Remove(e)
		}
		time.Sleep(time.Second)
	}

}
