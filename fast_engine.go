package orm

import (
	"fmt"
	"reflect"
)

type FastEngine interface {
	LoadByID(id uint64, entity Entity, references ...string) (found bool, fastEntity FastEntity)
	LoadByIDs(ids []uint64, entity Entity, references ...string) (result []FastEntity, missing []uint64)
	Search(where *Where, pager *Pager, entity Entity, references ...string) []FastEntity
	SearchWithCount(where *Where, pager *Pager, entity Entity, references ...string) (totalRows int, results []FastEntity)
	SearchOne(where *Where, entity Entity, references ...string) (found bool, result FastEntity)
	CachedSearch(entity Entity, indexName string, pager *Pager, arguments ...interface{}) (totalRows int, results []FastEntity)
	CachedSearchWithReferences(entity Entity, indexName string, pager *Pager, arguments []interface{}, references []string) (totalRows int, results []FastEntity)
	CachedSearchOne(entity Entity, indexName string, arguments ...interface{}) (found bool, result FastEntity)
	CachedSearchOneWithReferences(entity Entity, indexName string, arguments []interface{}, references []string) (found bool, result FastEntity)
}

type FastEntity interface {
	GetID() uint64
	Get(field string) interface{}
	Entity() Entity
	Is(entity Entity) bool
}

type fastEngine struct {
	engine *Engine
}

func (fe *fastEngine) LoadByID(id uint64, entity Entity, references ...string) (found bool, result FastEntity) {
	found, result, _ = loadByID(fe.engine, id, entity, false, true, references...)
	if !found {
		return false, nil
	}
	return found, result
}

func (fe *fastEngine) LoadByIDs(ids []uint64, entity Entity, references ...string) (result []FastEntity, missing []uint64) {
	missing, _, result = tryByIDs(fe.engine, ids, false, reflect.ValueOf(entity), references)
	return result, missing
}

func (fe *fastEngine) Search(where *Where, pager *Pager, entity Entity, references ...string) []FastEntity {
	_, res := search(true, fe.engine, where, pager, false, false, reflect.ValueOf(entity).Elem(), references...)
	return res
}

func (fe *fastEngine) SearchWithCount(where *Where, pager *Pager, entity Entity, references ...string) (totalRows int, results []FastEntity) {
	return search(true, fe.engine, where, pager, true, false, reflect.ValueOf(entity).Elem(), references...)
}

func (fe *fastEngine) SearchOne(where *Where, entity Entity, references ...string) (found bool, result FastEntity) {
	found, schema, data := searchOne(true, false, fe.engine, where, entity, references)
	if !found {
		return false, nil
	}
	return found, &fastEntity{data: data, engine: fe.engine, schema: schema}
}

func (fe *fastEngine) CachedSearch(entity Entity, indexName string, pager *Pager, arguments ...interface{}) (totalRows int, results []FastEntity) {
	return fe.CachedSearchWithReferences(entity, indexName, pager, arguments, nil)
}

func (fe *fastEngine) CachedSearchWithReferences(entity Entity, indexName string, pager *Pager, arguments []interface{}, references []string) (totalRows int, results []FastEntity) {
	total, ids := cachedSearch(fe.engine, entity, indexName, pager, arguments, references)
	if total == 0 {
		return total, nil
	}
	results, _ = fe.LoadByIDs(ids, entity)
	return total, results
}

func (fe *fastEngine) CachedSearchOne(entity Entity, indexName string, arguments ...interface{}) (found bool, result FastEntity) {
	return fe.CachedSearchOneWithReferences(entity, indexName, arguments, nil)
}

func (fe *fastEngine) CachedSearchOneWithReferences(entity Entity, indexName string, arguments []interface{}, references []string) (found bool, result FastEntity) {
	found, id := cachedSearchOne(fe.engine, entity, indexName, false, arguments, references)
	if !found {
		return false, nil
	}
	return fe.LoadByID(id, entity)
}

type fastEntity struct {
	data   []interface{}
	engine *Engine
	schema *tableSchema
	entity Entity
}

func (e *fastEntity) GetID() uint64 {
	return e.data[0].(uint64)
}

func (e *fastEntity) Get(field string) interface{} {
	return e.data[e.get(field)]
}

func (e *fastEntity) Entity() Entity {
	if e.entity == nil {
		e.entity = reflect.New(e.schema.t).Interface().(Entity)
		orm := initIfNeeded(e.engine, e.entity)
		fillStruct(e.engine, 0, e.data, e.schema.fields, orm.elem)
	}
	return e.entity
}

func (e *fastEntity) Is(entity Entity) bool {
	return e.engine.registry.GetTableSchemaForEntity(entity) == e.schema
}

func (e *fastEntity) get(field string) int {
	i, has := e.schema.columnMapping[field]
	if !has {
		panic(fmt.Errorf("unknown field %s", field))
	}
	return i
}
