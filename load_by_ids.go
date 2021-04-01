package orm

import (
	"fmt"
	"reflect"
	"strings"

	jsoniter "github.com/json-iterator/go"
)

func tryByIDs(engine *Engine, ids []uint64, fillStruct bool, entities reflect.Value, references []string) (missing []uint64, schema *tableSchema, result []FastEntity) {
	missing = make([]uint64, 0)
	if !fillStruct {
		result = make([]FastEntity, 0)
	}
	valOrigin := entities
	if fillStruct {
		valOrigin.SetLen(0)
		valOrigin.SetCap(0)
	}
	originalIDs := ids
	lenIDs := len(ids)
	if lenIDs == 0 {
		return
	}
	t, has, name := getEntityTypeForSlice(engine.registry, entities.Type())
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}

	schema = getTableSchema(engine.registry, t)
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	redisCache, hasRedis := schema.GetRedisCache(engine)

	if !hasLocalCache && engine.dataLoader != nil {
		data := engine.dataLoader.LoadAll(schema, ids)
		v := valOrigin
		for i, row := range data {
			if row == nil {
				missing = append(missing, ids[i])
			} else {
				if fillStruct {
					val := reflect.New(schema.t)
					entity := val.Interface().(Entity)
					fillFromDBRow(ids[i], engine, row, entity, false)
					v = reflect.Append(v, val)
				} else {
					result = append(result, &fastEntity{data: row, engine: engine, schema: schema})
				}
			}
		}
		if fillStruct {
			valOrigin.Set(v)
			if len(references) > 0 && v.Len() > 0 {
				warmUpReferences(engine, true, schema, entities.Interface(), references, true)
			}
		} else {
			if len(references) > 0 && len(result) > 0 {
				warmUpReferences(engine, false, schema, result, references, true)
			}
		}

		return
	}

	var localCacheKeys []string
	var redisCacheKeys []string
	results := make(map[string]interface{}, lenIDs)
	keysMapping := make(map[string]uint64, lenIDs)
	keysReversed := make(map[uint64]string, lenIDs)
	cacheKeys := make([]string, lenIDs)
	for index, id := range ids {
		cacheKey := schema.getCacheKey(id)
		cacheKeys[index] = cacheKey
		keysMapping[cacheKey] = id
		keysReversed[id] = cacheKey
		results[cacheKey] = nil
	}

	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCache = engine.GetLocalCache(requestCacheKey)
	}

	if hasLocalCache || hasRedis {
		if hasLocalCache {
			resultsLocalCache := localCache.MGet(cacheKeys...)
			cacheKeys = getKeysForNils(engine, schema, resultsLocalCache, keysMapping, results, false, fillStruct)
			localCacheKeys = cacheKeys
		}
		if hasRedis && len(cacheKeys) > 0 {
			resultsRedis := redisCache.MGet(cacheKeys...)
			cacheKeys = getKeysForNils(engine, schema, resultsRedis, keysMapping, results, true, fillStruct)
			redisCacheKeys = cacheKeys
		}
		ids = make([]uint64, len(cacheKeys))
		for k, v := range cacheKeys {
			ids[k] = keysMapping[v]
		}
	}
	l := len(ids)
	if l > 0 {
		_, fastEntities := search(false, engine, NewWhere("`ID` IN ?", ids), NewPager(1, l), false, fillStruct, entities)
		if fillStruct {
			for i := 0; i < entities.Len(); i++ {
				e := entities.Index(i).Interface().(Entity)
				results[schema.getCacheKey(e.GetID())] = e
			}
		} else {
			for _, fastEntity := range fastEntities {
				results[schema.getCacheKey(fastEntity.GetID())] = fastEntity
			}
		}
	}
	if hasLocalCache {
		l = len(localCacheKeys)
		if l > 0 {
			pairs := make([]interface{}, l*2)
			i := 0
			for _, key := range localCacheKeys {
				pairs[i] = key
				val := results[key]
				var toSet interface{}
				if val == nil {
					toSet = "nil"
				} else {
					if fillStruct {
						toSet = buildLocalCacheValue(val.(Entity).getORM().dBData)
					} else {
						toSet = buildLocalCacheValue(val.(*fastEntity).data)
					}
				}
				pairs[i+1] = toSet
				i += 2
			}
			localCache.MSet(pairs...)
		}
	}

	if hasRedis {
		l = len(redisCacheKeys)
		if l > 0 {
			pairs := make([]interface{}, l*2)
			i := 0
			for _, key := range redisCacheKeys {
				pairs[i] = key
				val := results[key]
				var toSet interface{}
				if val == nil {
					toSet = "nil"
				} else {
					if fillStruct {
						toSet = buildRedisValue(val.(Entity).getORM().dBData)
					} else {
						toSet = buildRedisValue(val.(*fastEntity).data)
					}
				}
				pairs[i+1] = toSet
				i += 2
			}
			redisCache.MSet(pairs...)
		}
	}

	valOrigin = entities
	if fillStruct {
		valOrigin.SetLen(0)
		valOrigin.SetCap(0)
	}
	v := valOrigin
	for _, id := range originalIDs {
		val := results[keysReversed[id]]
		if val == nil {
			missing = append(missing, id)
		} else {
			if fillStruct {
				v = reflect.Append(v, reflect.ValueOf(val))
			} else {
				result = append(result, val.(FastEntity))
			}
		}
	}
	if fillStruct {
		valOrigin.Set(v)
		if len(references) > 0 && v.Len() > 0 {
			warmUpReferences(engine, true, schema, entities, references, true)
		}
	} else if len(references) > 0 && len(results) > 0 {
		warmUpReferences(engine, false, schema, result, references, true)
	}
	return
}

func getKeysForNils(engine *Engine, schema *tableSchema, rows map[string]interface{}, keysMapping map[string]uint64,
	results map[string]interface{}, fromRedis bool, fillStruct bool) []string {
	keys := make([]string, 0)
	for k, v := range rows {
		if v == nil {
			keys = append(keys, k)
		} else {
			if v == "nil" {
				results[k] = nil
			} else if fromRedis {
				var decoded []interface{}
				_ = jsoniter.ConfigFastest.Unmarshal([]byte(v.(string)), &decoded)
				convertDataFromJSON(schema.fields, 0, decoded)
				if fillStruct {
					entity := reflect.New(schema.t).Interface().(Entity)
					fillFromDBRow(keysMapping[k], engine, decoded, entity, false)
					results[k] = entity
				} else {
					results[k] = &fastEntity{data: decoded, engine: engine, schema: schema}
				}
			} else {
				if fillStruct {
					entity := reflect.New(schema.t).Interface().(Entity)
					fillFromDBRow(keysMapping[k], engine, v.([]interface{}), entity, false)
					results[k] = entity
				} else {
					results[k] = &fastEntity{data: v.([]interface{}), engine: engine, schema: schema}
				}
			}
		}
	}
	return keys
}

func warmUpReferences(engine *Engine, fillStruct bool, schema *tableSchema, rows interface{}, references []string, many bool) {
	dbMap := make(map[string]map[*tableSchema]map[string][]interface{})
	var localMap map[string]map[string][]interface{}
	var redisMap map[string]map[string][]interface{}
	l := 1
	if many {
		if fillStruct {
			l = rows.(reflect.Value).Len()
		} else {
			asF, is := rows.([]FastEntity)
			if is {
				l = len(asF)
			} else {
				l = len(rows.([]interface{}))
			}
		}
	}
	if references[0] == "*" {
		references = schema.refOne
	}
	var referencesNextNames map[string][]string
	var referencesNextEntities map[string][]interface{}
	for _, ref := range references {
		refName := ref
		pos := strings.Index(refName, "/")
		if pos > 0 {
			if referencesNextNames == nil {
				referencesNextNames = make(map[string][]string)
			}
			if referencesNextEntities == nil {
				referencesNextEntities = make(map[string][]interface{})
			}
			nextRef := refName[pos+1:]
			refName = refName[0:pos]
			referencesNextNames[refName] = append(referencesNextNames[refName], nextRef)
			referencesNextEntities[refName] = nil
		}
		_, has := schema.tags[refName]
		if !has {
			panic(fmt.Errorf("reference %s in %s is not valid", ref, schema.tableName))
		}
		parentRef, has := schema.tags[refName]["ref"]
		manyRef := false
		if !has {
			parentRef, has = schema.tags[refName]["refs"]
			manyRef = true
			if !has {
				panic(fmt.Errorf("reference tag %s is not valid", ref))
			}
		}
		parentSchema := engine.registry.tableSchemas[engine.registry.entities[parentRef]]
		hasLocalCache := parentSchema.hasLocalCache
		if !hasLocalCache && engine.hasRequestCache {
			hasLocalCache = true
		}
		if hasLocalCache && localMap == nil {
			localMap = make(map[string]map[string][]interface{})
		}
		if parentSchema.hasRedisCache && redisMap == nil {
			redisMap = make(map[string]map[string][]interface{})
		}
		if fillStruct {
			for i := 0; i < l; i++ {
				var ref reflect.Value
				if many {
					ref = reflect.Indirect(rows.(reflect.Value).Index(i).Elem()).FieldByName(refName)
				} else {
					ref = rows.(reflect.Value).FieldByName(refName)
				}
				if !ref.IsValid() || ref.IsZero() {
					continue
				}
				if manyRef {
					length := ref.Len()
					for i := 0; i < length; i++ {
						e := ref.Index(i).Interface().(Entity)
						if !e.Loaded() {
							id := e.GetID()
							if id > 0 {
								fillRefMap(engine, id, referencesNextEntities, refName, e, parentSchema, dbMap, localMap, redisMap)
							}
						}
					}
				} else {
					e := ref.Interface().(Entity)
					if !e.Loaded() {
						id := e.GetID()
						if id > 0 {
							fillRefMap(engine, id, referencesNextEntities, refName, e, parentSchema, dbMap, localMap, redisMap)
						}
					}
				}
			}
		} else {
			for i := 0; i < l; i++ {
				index := schema.columnMapping[refName]
				var val interface{}
				if many {
					asF, is := rows.([]FastEntity)
					if is {
						val = asF[i].(*fastEntity).data[index]
					} else {
						val = rows.([]interface{})[i].(*fastEntity).data[index]
					}
				} else {
					val = rows.(*fastEntity).data[index]
				}
				_, is := val.(FastEntity)
				if is || val == nil {
					continue
				}
				id := val.(uint64)
				if id > 0 {
					fastE := &fastEntity{engine: engine, schema: parentSchema}
					if many {
						asF, is := rows.([]FastEntity)
						if is {
							asF[i].(*fastEntity).data[index] = fastE
						} else {
							rows.([]interface{})[i].(*fastEntity).data[index] = fastE
						}
					} else {
						rows.(*fastEntity).data[index] = fastE
					}
					fillRefMap(engine, id, referencesNextEntities, refName, fastE, parentSchema, dbMap, localMap, redisMap)
				}
			}
		}
	}
	for k, v := range localMap {
		l := len(v)
		if l == 1 {
			var key string
			for k := range v {
				key = k
				break
			}
			fromCache, has := engine.GetLocalCache(k).Get(key)
			if has && fromCache != "nil" {
				data := fromCache.([]interface{})
				for _, r := range v[key] {
					if fillStruct {
						fillFromDBRow(data[0].(uint64), engine, data, r.(Entity), false)
					} else {
						r.(*fastEntity).data = data
					}
				}
				fillRef(key, localMap, redisMap, dbMap)
			}
		} else if l > 1 {
			keys := make([]string, len(v))
			i := 0
			for k := range v {
				keys[i] = k
				i++
			}
			for key, fromCache := range engine.GetLocalCache(k).MGet(keys...) {
				if fromCache != nil {
					data := fromCache.([]interface{})
					for _, r := range v[key] {
						if fillStruct {
							fillFromDBRow(data[0].(uint64), engine, data, r.(Entity), false)
						} else {
							r.(*fastEntity).data = data
						}
					}
					fillRef(key, localMap, redisMap, dbMap)
				}
			}
		}
	}
	for k, v := range redisMap {
		l := len(v)
		if l == 0 {
			continue
		}
		keys := make([]string, l)
		i := 0
		for k := range v {
			keys[i] = k
			i++
		}
		for key, fromCache := range engine.GetRedis(k).MGet(keys...) {
			if fromCache != nil {
				schema := v[key][0].(Entity).getORM().tableSchema
				decoded := make([]interface{}, len(schema.columnNames))
				_ = jsoniter.ConfigFastest.Unmarshal([]byte(fromCache.(string)), &decoded)
				convertDataFromJSON(schema.fields, 0, decoded)
				for _, r := range v[key] {
					if fillStruct {
						fillFromDBRow(decoded[0].(uint64), engine, decoded, r.(Entity), false)
					} else {
						r.(*fastEntity).data = decoded
					}
				}
				fillRef(key, nil, redisMap, dbMap)
			}
		}
	}
	for k, v := range dbMap {
		db := engine.GetMysql(k)
		for schema, v2 := range v {
			if len(v2) == 0 {
				continue
			}
			keys := make([]string, len(v2))
			q := make([]string, len(v2))
			i := 0
			for k2 := range v2 {
				keys[i] = k2[strings.Index(k2, ":")+1:]
				q[i] = keys[i]
				i++
			}
			query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE `ID` IN (" + strings.Join(q, ",") + ")"
			results, def := db.Query(query)
			for results.Next() {
				pointers := prepareScan(schema)
				results.Scan(pointers...)
				convertScan(schema.fields, 0, pointers)
				id := pointers[0].(uint64)
				if fillStruct {
					for _, r := range v2[schema.getCacheKey(id)] {
						fillFromDBRow(id, engine, pointers, r.(Entity), false)
					}
				} else {
					for _, r := range v2[schema.getCacheKey(id)] {
						r.(*fastEntity).data = pointers
					}
				}
			}
			def()
		}
	}
	for pool, v := range redisMap {
		if len(v) == 0 {
			continue
		}
		values := make([]interface{}, 0)
		if fillStruct {
			for cacheKey, refs := range v {
				e := refs[0].(Entity)
				if e.Loaded() {
					values = append(values, cacheKey, buildRedisValue(e.getORM().dBData))
				} else {
					values = append(values, cacheKey, "nil")
				}
			}
		} else {
			for cacheKey, refs := range v {
				e := refs[0].(*fastEntity)
				if e.data != nil {
					values = append(values, cacheKey, buildRedisValue(e.data))
				} else {
					values = append(values, cacheKey, "nil")
				}
			}
		}
		engine.GetRedis(pool).MSet(values...)
	}
	for pool, v := range localMap {
		if len(v) == 0 {
			continue
		}
		values := make([]interface{}, 0)
		if fillStruct {
			for cacheKey, refs := range v {
				e := refs[0].(Entity)
				if e.Loaded() {
					values = append(values, cacheKey, buildLocalCacheValue(e.getORM().dBData))
				} else {
					values = append(values, cacheKey, "nil")
				}
			}
		} else {
			for cacheKey, refs := range v {
				e := refs[0].(*fastEntity)
				if e.data != nil {
					values = append(values, cacheKey, buildLocalCacheValue(e.data))
				} else {
					values = append(values, cacheKey, "nil")
				}
			}
		}
		engine.GetLocalCache(pool).MSet(values...)
	}

	if fillStruct {
		for refName, entities := range referencesNextEntities {
			l := len(entities)
			if l == 1 {
				warmUpReferences(engine, fillStruct, entities[0].(Entity).getORM().tableSchema, reflect.ValueOf(entities[0]).Elem(),
					referencesNextNames[refName], false)
			} else if l > 1 {
				warmUpReferences(engine, fillStruct, entities[0].(Entity).getORM().tableSchema, reflect.ValueOf(entities),
					referencesNextNames[refName], true)
			}
		}
	} else {
		for refName, entities := range referencesNextEntities {
			l := len(entities)
			if l == 1 {
				warmUpReferences(engine, fillStruct, entities[0].(*fastEntity).schema, entities[0],
					referencesNextNames[refName], false)
			} else if l > 1 {
				warmUpReferences(engine, fillStruct, entities[0].(*fastEntity).schema, entities,
					referencesNextNames[refName], true)
			}
		}
	}
}

func fillRef(key string, localMap map[string]map[string][]interface{},
	redisMap map[string]map[string][]interface{}, dbMap map[string]map[*tableSchema]map[string][]interface{}) {
	for _, p := range localMap {
		delete(p, key)
	}
	for _, p := range redisMap {
		delete(p, key)
	}
	for _, p := range dbMap {
		for _, p2 := range p {
			delete(p2, key)
		}
	}
}

func fillRefMap(engine *Engine, id uint64, referencesNextEntities map[string][]interface{}, refName string, v interface{}, parentSchema *tableSchema,
	dbMap map[string]map[*tableSchema]map[string][]interface{},
	localMap map[string]map[string][]interface{}, redisMap map[string]map[string][]interface{}) {
	_, has := referencesNextEntities[refName]
	if has {
		referencesNextEntities[refName] = append(referencesNextEntities[refName], v)
	}
	cacheKey := parentSchema.getCacheKey(id)
	if dbMap[parentSchema.mysqlPoolName] == nil {
		dbMap[parentSchema.mysqlPoolName] = make(map[*tableSchema]map[string][]interface{})
	}
	if dbMap[parentSchema.mysqlPoolName][parentSchema] == nil {
		dbMap[parentSchema.mysqlPoolName][parentSchema] = make(map[string][]interface{})
	}
	dbMap[parentSchema.mysqlPoolName][parentSchema][cacheKey] = append(dbMap[parentSchema.mysqlPoolName][parentSchema][cacheKey], v)
	hasLocalCache := parentSchema.hasLocalCache
	localCacheName := parentSchema.localCacheName
	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCacheName = requestCacheKey
	}
	if hasLocalCache {
		if localMap[localCacheName] == nil {
			localMap[localCacheName] = make(map[string][]interface{})
		}
		localMap[localCacheName][cacheKey] = append(localMap[localCacheName][cacheKey], v)
	}
	if parentSchema.hasRedisCache {
		if redisMap[parentSchema.redisCacheName] == nil {
			redisMap[parentSchema.redisCacheName] = make(map[string][]interface{})
		}
		redisMap[parentSchema.redisCacheName][cacheKey] = append(redisMap[parentSchema.redisCacheName][cacheKey], v)
	}
}
