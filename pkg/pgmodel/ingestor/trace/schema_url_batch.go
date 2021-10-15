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

const insertSchemaURLSQL = `SELECT %s.put_schema_url($1)`

type schemaURL string

func (s schemaURL) len() uint64 {
	return uint64(len(s))
}

//schemaURLBatch queues up items to send to the DB but it sorts before sending
//this avoids deadlocks in the DB. It also avoids sending the same URLs repeatedly.
type schemaURLBatch struct {
	batch map[schemaURL]pgtype.Int8
	cache SchemaURLCache
}

func newSchemaUrlBatch(cache SchemaURLCache) schemaURLBatch {
	return schemaURLBatch{
		cache: cache,
		batch: make(map[schemaURL]pgtype.Int8),
	}
}

func (s schemaURLBatch) Queue(url string) {
	if url == "" {
		return
	}
	u := schemaURL(url)
	if !s.cache.Exists(u) {
		s.batch[u] = pgtype.Int8{}
	}
}

func (s schemaURLBatch) SendBatch(ctx context.Context, conn pgxconn.PgxConn) (err error) {
	urls := make([]schemaURL, len(s.batch))
	i := 0
	for url := range s.batch {
		urls[i] = url
		i++
	}
	sort.Slice(urls, func(i, j int) bool {
		return urls[i] < urls[j]
	})

	dbBatch := conn.NewBatch()
	for _, sURL := range urls {
		dbBatch.Queue(fmt.Sprintf(insertSchemaURLSQL, schema.TracePublic), sURL)
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
	for _, url := range urls {
		if err := br.QueryRow().Scan(&id); err != nil {
			return err
		}
		s.cache.Set(url, id)
	}

	return nil
}

func (s schemaURLBatch) GetID(url string) (pgtype.Int8, error) {
	if url == "" {
		return pgtype.Int8{Status: pgtype.Null}, nil
	}
	id, err := s.cache.Get(schemaURL(url))
	if err != nil {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("error getting schema URL ID from cache: %w", err)
	}
	if id.Status != pgtype.Present {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("schema URL ID is null for URL: %s", url)
	}
	if id.Int == 0 {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("schema URL ID is 0 for URL: %s", url)
	}
	return id, nil
}
