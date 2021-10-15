package trace

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
)

func TestSchemaURLCache(t *testing.T) {
	cache := newSchemaCache()

	url := schemaURL("someURL")
	id := pgtype.Int8{Int: 6, Status: pgtype.Present}

	require.False(t, cache.Exists(url))

	cache.Set(url, id)

	found, err := cache.Get(url)

	require.Nil(t, err)
	require.Equal(t, found, id)

	require.True(t, cache.Exists(url))

	_, err = cache.Get(schemaURL("non-existant"))

	require.Equal(t, errors.ErrEntryNotFound, err)

	invalid := schemaURL("invalid")

	cache.cache.Set(invalid, true, 0)

	_, err = cache.Get(invalid)
	require.Equal(t, errors.ErrInvalidCacheEntryType, err)
}

func TestOperationCache(t *testing.T) {
	cache := newOperationCache()

	op := operation{"service_name", "span_name", "span_kind"}
	id := pgtype.Int8{Int: 6, Status: pgtype.Present}

	require.False(t, cache.Exists(op))

	cache.Set(op, id)

	found, err := cache.Get(op)

	require.Nil(t, err)
	require.Equal(t, found, id)

	require.True(t, cache.Exists(op))

	_, err = cache.Get(operation{serviceName: "non-existant"})

	require.Equal(t, errors.ErrEntryNotFound, err)

	invalid := operation{serviceName: "invalid"}

	cache.cache.Set(invalid, true, 0)

	_, err = cache.Get(invalid)
	require.Equal(t, errors.ErrInvalidCacheEntryType, err)
}

func TestInstrumentationLibraryCache(t *testing.T) {
	cache := newInstrumentationLibraryCache()

	il := instrumentationLibrary{"name", "version", pgtype.Int8{Int: 6, Status: pgtype.Present}}
	id := pgtype.Int8{Int: 7, Status: pgtype.Present}

	require.False(t, cache.Exists(il))

	cache.Set(il, id)

	found, err := cache.Get(il)

	require.Nil(t, err)
	require.Equal(t, found, id)

	require.True(t, cache.Exists(il))

	_, err = cache.Get(instrumentationLibrary{name: "non-existant"})

	require.Equal(t, errors.ErrEntryNotFound, err)

	invalid := instrumentationLibrary{name: "invalid"}

	cache.cache.Set(invalid, true, 0)

	_, err = cache.Get(invalid)
	require.Equal(t, errors.ErrInvalidCacheEntryType, err)
}

func TestTagCache(t *testing.T) {
	cache := newTagCache()

	tagValue := tag{"key", "value", TagType(6)}
	id := tagIDs{pgtype.Int8{Int: 7, Status: pgtype.Present}, pgtype.Int8{Int: 7, Status: pgtype.Present}}

	require.False(t, cache.Exists(tagValue))

	cache.Set(tagValue, id)

	found, err := cache.Get(tagValue)

	require.Nil(t, err)
	require.Equal(t, found, id)

	require.True(t, cache.Exists(tagValue))

	_, err = cache.Get(tag{key: "non-existant"})

	require.Equal(t, errors.ErrEntryNotFound, err)

	invalid := tag{key: "invalid"}

	cache.cache.Set(invalid, true, 0)

	_, err = cache.Get(invalid)
	require.Equal(t, errors.ErrInvalidCacheEntryType, err)
}
