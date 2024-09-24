package checkpoint

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

func (e *CheckpointEntry) PrefetchV1(
	ctx context.Context,
	fs *objectio.ObjectFS,
	data *logtail.CheckpointDataV1,
) (err error) {
	if err = data.PrefetchFrom(
		ctx,
		e.version,
		fs.Service,
		e.tnLocation,
	); err != nil {
		return
	}
	return
}

func (e *CheckpointEntry) PrefetchMetaIdxV1(
	ctx context.Context,
	fs *objectio.ObjectFS,
) (data *logtail.CheckpointDataV1, err error) {
	data = logtail.NewCheckpointDataV1(e.sid, common.CheckpointAllocator)
	if err = data.PrefetchMeta(
		ctx,
		e.version,
		fs.Service,
		e.tnLocation,
	); err != nil {
		return
	}
	return
}

func (e *CheckpointEntry) ReadMetaIdxV1(
	ctx context.Context,
	fs *objectio.ObjectFS,
	data *logtail.CheckpointDataV1,
) (err error) {
	reader, err := blockio.NewObjectReader(e.sid, fs.Service, e.tnLocation)
	if err != nil {
		return
	}
	return data.ReadTNMetaBatch(ctx, e.version, e.tnLocation, reader)
}

func (e *CheckpointEntry) ReadV1(
	ctx context.Context,
	fs *objectio.ObjectFS,
	data *logtail.CheckpointDataV1,
) (err error) {
	reader, err := blockio.NewObjectReader(e.sid, fs.Service, e.tnLocation)
	if err != nil {
		return
	}

	if err = data.ReadFrom(
		ctx,
		e.version,
		e.tnLocation,
		reader,
		fs.Service,
	); err != nil {
		return
	}
	return
}
