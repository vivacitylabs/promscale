// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package trace

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgtype"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const insertOperationSQL = `SELECT %s.put_operation($1, $2, $3)`

type operation struct {
	serviceName string
	spanName    string
	spanKind    string
}

func (o operation) len() uint64 {
	return uint64(len(o.serviceName) + len(o.spanName) + len(o.spanKind))
}

//Operation batch queues up items to send to the db but it sorts before sending
//this avoids deadlocks in the db
type operationBatch struct {
	batch map[operation]pgtype.Int8
	cache OperationCache
}

func newOperationBatch(cache OperationCache) operationBatch {
	return operationBatch{
		cache: cache,
		batch: make(map[operation]pgtype.Int8),
	}
}

func (o operationBatch) Queue(serviceName, spanName, spanKind string) {
	op := operation{serviceName, spanName, spanKind}
	if !o.cache.Exists(op) {
		o.batch[op] = pgtype.Int8{}
	}
}

func (o operationBatch) SendBatch(ctx context.Context, conn pgxconn.PgxConn) (err error) {
	ops := make([]operation, len(o.batch))
	i := 0
	for op := range o.batch {
		ops[i] = op
		i++
	}
	sort.Slice(ops, func(i, j int) bool {
		if ops[i].serviceName != ops[j].serviceName {
			return ops[i].serviceName < ops[j].serviceName
		}
		if ops[i].spanName != ops[j].spanName {
			return ops[i].spanName < ops[j].spanName
		}
		return ops[i].spanKind < ops[j].spanKind
	})

	dbBatch := conn.NewBatch()
	for _, op := range ops {
		dbBatch.Queue(fmt.Sprintf(insertOperationSQL, schema.TracePublic), op.serviceName, op.spanName, op.spanKind)
	}
	br, err := conn.SendBatch(ctx, dbBatch)
	if err != nil {
		return err
	}
	defer func() {
		// Only return Close error if there was no previous error.
		if tempErr := br.Close(); err == nil {
			err = tempErr
		}
	}()
	var id pgtype.Int8
	for _, op := range ops {
		if err := br.QueryRow().Scan(&id); err != nil {
			return err
		}
		o.cache.Set(op, id)
	}
	return nil
}
func (o operationBatch) GetID(serviceName, spanName, spanKind string) (pgtype.Int8, error) {
	id, err := o.cache.Get(operation{serviceName, spanName, spanKind})
	if err != nil {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("error getting operation from cache: %w", err)
	}
	if id.Status != pgtype.Present {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("operation id is null")
	}
	if id.Int == 0 {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("operation id is 0")
	}
	return id, nil
}
