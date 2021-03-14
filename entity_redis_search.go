package orm

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/pkg/errors"
)

func (e *Engine) RedisSearchIds(entity Entity, query *RedisSearchQuery, pager *Pager) (ids []uint64, totalRows uint64) {
	schema := e.GetRegistry().GetTableSchemaForEntity(entity).(*tableSchema)
	return redisSearch(e, schema, query, pager, nil)
}

func (e *Engine) RedisSearch(entities interface{}, query *RedisSearchQuery, pager *Pager, references ...string) {
	elem := reflect.ValueOf(entities).Elem()
	_, has, name := getEntityTypeForSlice(e.registry, elem.Type())
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}
	schema := e.GetRegistry().GetTableSchema(name).(*tableSchema)
	ids, total := redisSearch(e, schema, query, pager, references)
	if total > 0 {
		e.LoadByIDs(ids, entities, references...)
	}
}

func redisSearch(e *Engine, schema *tableSchema, query *RedisSearchQuery, pager *Pager, references []string) ([]uint64, uint64) {
	if schema.redisSearchIndex == nil {
		panic(errors.Errorf("entity %s is not searchable", schema.t.String()))
	}
	search := e.GetRedisSearch(schema.searchCacheName)
	totalRows, res := search.search(schema.redisSearchIndex.Name, query, pager, true)
	ids := make([]uint64, len(res))
	for i, v := range res {
		ids[i], _ = strconv.ParseUint(v.(string)[6:], 10, 64)
	}
	return ids, totalRows
}
