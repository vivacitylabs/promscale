package trace

import (
	"github.com/jackc/pgtype"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
)

const (
	urlCacheSize       = 1000
	operationCacheSize = 1000
	instLibCacheSize   = 1000
	tagCacheSize       = 1000
)

type SchemaURLCache interface {
	Get(url schemaURL) (pgtype.Int8, error)
	Set(url schemaURL, id pgtype.Int8)
	Exists(url schemaURL) bool
}

type OperationCache interface {
	Get(o operation) (pgtype.Int8, error)
	Set(o operation, id pgtype.Int8)
	Exists(o operation) bool
}

type InstrumentationLibraryCache interface {
	Get(i instrumentationLibrary) (pgtype.Int8, error)
	Set(i instrumentationLibrary, id pgtype.Int8)
	Exists(i instrumentationLibrary) bool
}

type TagCache interface {
	Get(t tag) (tagIDs, error)
	Set(t tag, tIDs tagIDs)
	Exists(t tag) bool
}

type baseCache struct {
	cache *clockcache.Cache
}

func (b baseCache) Get(v interface{}) (interface{}, error) {
	val, ok := b.cache.Get(v)
	if !ok {
		return pgtype.Int8{}, errors.ErrEntryNotFound
	}

	return val, nil
}

func (b baseCache) Set(v interface{}, id interface{}, size uint64) {
	b.cache.Insert(v, id, size)
}

func (b baseCache) Exists(v interface{}) bool {
	_, exists := b.cache.Get(v)
	return exists
}

type baseIDCache struct {
	baseCache
}

func (b baseIDCache) Get(v interface{}) (id pgtype.Int8, err error) {
	val, err := b.baseCache.Get(v)
	if err != nil {
		return pgtype.Int8{}, err
	}
	id, ok := val.(pgtype.Int8)
	if !ok {
		return pgtype.Int8{}, errors.ErrInvalidCacheEntryType
	}

	return id, nil
}

type schemaURLCache struct {
	cache baseIDCache
}

func newSchemaCache() *schemaURLCache {
	return &schemaURLCache{baseIDCache{baseCache{clockcache.WithMax(urlCacheSize)}}}
}

func (s schemaURLCache) Get(url schemaURL) (id pgtype.Int8, err error) {
	return s.cache.Get(url)
}

func (s schemaURLCache) Set(url schemaURL, id pgtype.Int8) {
	s.cache.Set(url, id, uint64(url.len()+9)) // 9 bytes for pgtype.Int8 (int64: 8 bytes, pgtype.Status: 1 byte).
}

func (s schemaURLCache) Exists(url schemaURL) bool {
	return s.cache.Exists(url)
}

type operationCache struct {
	cache baseIDCache
}

func newOperationCache() *operationCache {
	return &operationCache{baseIDCache{baseCache{clockcache.WithMax(operationCacheSize)}}}
}

func (op operationCache) Get(url operation) (id pgtype.Int8, err error) {
	return op.cache.Get(url)
}

func (op operationCache) Set(o operation, id pgtype.Int8) {
	op.cache.Set(o, id, uint64(o.len()+9)) // 9 bytes for pgtype.Int8 (int64: 8 bytes, pgtype.Status: 1 byte).
}

func (op operationCache) Exists(o operation) bool {
	return op.cache.Exists(o)
}

type instrumentationLibraryCache struct {
	cache baseIDCache
}

func newInstrumentationLibraryCache() *instrumentationLibraryCache {
	return &instrumentationLibraryCache{baseIDCache{baseCache{clockcache.WithMax(instLibCacheSize)}}}
}

func (ilc instrumentationLibraryCache) Get(i instrumentationLibrary) (id pgtype.Int8, err error) {
	return ilc.cache.Get(i)
}

func (ilc instrumentationLibraryCache) Set(il instrumentationLibrary, id pgtype.Int8) {
	ilc.cache.Set(il, id, uint64(il.len()+9)) // 9 bytes for pgtype.Int8 (int64: 8 bytes, pgtype.Status: 1 byte).
}
func (ilc instrumentationLibraryCache) Exists(i instrumentationLibrary) bool {
	return ilc.cache.Exists(i)
}

type tagIDCache struct {
	cache baseCache
}

func newTagCache() *tagIDCache {
	return &tagIDCache{baseCache{clockcache.WithMax(tagCacheSize)}}
}

func (tc tagIDCache) Get(t tag) (tagIDs, error) {
	val, err := tc.cache.Get(t)

	if err != nil {
		return tagIDs{}, err
	}

	id, ok := val.(tagIDs)
	if !ok {
		return tagIDs{}, errors.ErrInvalidCacheEntryType
	}

	return id, nil
}

func (tc tagIDCache) Set(t tag, tIDs tagIDs) {
	tc.cache.Set(t, tIDs, uint64(t.len()+tIDs.len()))
}
func (tc tagIDCache) Exists(t tag) bool {
	return tc.cache.Exists(t)
}
