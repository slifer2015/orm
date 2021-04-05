package orm

import (
	"fmt"
	"reflect"
	"strings"

	jsoniter "github.com/json-iterator/go"
)

func tryByIDs(engine *Engine, ids []uint64, entities reflect.Value, references []string, lazy bool) (missing []uint64, schema *tableSchema) {
	missing = make([]uint64, 0)
	valOrigin := entities
	valOrigin.SetLen(0)
	valOrigin.SetCap(0)
	originalIDs := ids
	lenIDs := len(ids)
	if lenIDs == 0 {
		return
	}
	t, has, name := getEntityTypeForSlice(engine.registry, entities.Type(), true)
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
				val := reflect.New(schema.t)
				entity := val.Interface().(Entity)
				fillFromDBRow(ids[i], engine, row, entity, false, lazy)
				v = reflect.Append(v, val)
			}
		}
		valOrigin.Set(v)
		if len(references) > 0 && v.Len() > 0 {
			warmUpReferences(engine, schema, entities, references, true, lazy)
		}
		return
	}

	var localCacheKeys []string
	var redisCacheKeys []string
	results := make(map[string]Entity, lenIDs)
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
			cacheKeys = getKeysForNils(engine, schema, resultsLocalCache, keysMapping, results, false, lazy)
			localCacheKeys = cacheKeys
		}
		if hasRedis && len(cacheKeys) > 0 {
			resultsRedis := redisCache.MGet(cacheKeys...)
			cacheKeys = getKeysForNils(engine, schema, resultsRedis, keysMapping, results, true, lazy)
			redisCacheKeys = cacheKeys
		}
		ids = make([]uint64, len(cacheKeys))
		for k, v := range cacheKeys {
			ids[k] = keysMapping[v]
		}
	}
	l := len(ids)
	if l > 0 {
		search(false, engine, NewWhere("`ID` IN ?", ids), NewPager(1, l), false, lazy, true, entities)
		for i := 0; i < entities.Len(); i++ {
			e := entities.Index(i).Interface().(Entity)
			results[schema.getCacheKey(e.GetID())] = e
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
					toSet = buildLocalCacheValue(val.getORM().dBData)
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
					toSet = buildRedisValue(val.getORM().dBData)
				}
				pairs[i+1] = toSet
				i += 2
			}
			redisCache.MSet(pairs...)
		}
	}

	valOrigin = entities
	valOrigin.SetLen(0)
	valOrigin.SetCap(0)
	v := valOrigin
	for _, id := range originalIDs {
		val := results[keysReversed[id]]
		if val == nil {
			missing = append(missing, id)
		} else {
			v = reflect.Append(v, reflect.ValueOf(val))
		}
	}
	valOrigin.Set(v)
	if len(references) > 0 && v.Len() > 0 {
		warmUpReferences(engine, schema, entities, references, true, lazy)
	}
	return
}

func getKeysForNils(engine *Engine, schema *tableSchema, rows map[string]interface{}, keysMapping map[string]uint64,
	results map[string]Entity, fromRedis bool, lazy bool) []string {
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
				entity := reflect.New(schema.t).Interface().(Entity)
				fillFromDBRow(keysMapping[k], engine, decoded, entity, false, lazy)
				results[k] = entity
			} else {
				entity := reflect.New(schema.t).Interface().(Entity)
				fillFromDBRow(keysMapping[k], engine, v.([]interface{}), entity, false, lazy)
				results[k] = entity
			}
		}
	}
	return keys
}

func warmUpReferences(engine *Engine, schema *tableSchema, rows reflect.Value, references []string, many bool, lazy bool) {
	dbMap := make(map[string]map[*tableSchema]map[string][]Entity)
	var localMap map[string]map[string][]Entity
	var redisMap map[string]map[string][]Entity
	l := 1
	if many {
		l = rows.Len()
	}
	if references[0] == "*" {
		references = schema.refOne
	}
	var referencesNextNames map[string][]string
	var referencesNextEntities map[string][]Entity
	for _, ref := range references {
		refName := ref
		pos := strings.Index(refName, "/")
		if pos > 0 {
			if referencesNextNames == nil {
				referencesNextNames = make(map[string][]string)
			}
			if referencesNextEntities == nil {
				referencesNextEntities = make(map[string][]Entity)
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
			localMap = make(map[string]map[string][]Entity)
		}
		if parentSchema.hasRedisCache && redisMap == nil {
			redisMap = make(map[string]map[string][]Entity)
		}
		for i := 0; i < l; i++ {
			var ref reflect.Value
			var refEntity reflect.Value
			if many {
				refEntity = rows.Index(i).Elem()
				ref = reflect.Indirect(refEntity).FieldByName(refName)
			} else {
				refEntity = rows
				ref = reflect.Indirect(refEntity).FieldByName(refName)
			}
			if !ref.IsValid() || ref.IsZero() {
				if !lazy {
					continue
				}
				dbData := refEntity.Interface().(Entity).getORM().dBData
				idVal := dbData[schema.columnMapping[refName]]
				if idVal == nil {
					continue
				}
				id := idVal.(uint64)
				if id == 0 {
					continue
				}
				n := reflect.New(ref.Type().Elem())
				orm := initIfNeeded(engine.registry, n.Interface().(Entity))
				orm.idElem.SetUint(id)
				orm.inDB = true
				ref.Set(n)
			}
			if manyRef {
				length := ref.Len()
				for i := 0; i < length; i++ {
					e := ref.Index(i).Interface().(Entity)
					if !e.IsLoaded() {
						id := e.GetID()
						if id > 0 {
							fillRefMap(engine, id, referencesNextEntities, refName, e, parentSchema, dbMap, localMap, redisMap)
						}
					}
				}
			} else {
				e := ref.Interface().(Entity)
				if !e.IsLoaded() {
					id := e.GetID()
					if id > 0 {
						fillRefMap(engine, id, referencesNextEntities, refName, e, parentSchema, dbMap, localMap, redisMap)
					}
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
					fillFromDBRow(data[0].(uint64), engine, data, r, false, lazy)
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
						fillFromDBRow(data[0].(uint64), engine, data, r, false, lazy)
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
					fillFromDBRow(decoded[0].(uint64), engine, decoded, r, false, lazy)
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
				for _, r := range v2[schema.getCacheKey(id)] {
					fillFromDBRow(id, engine, pointers, r, false, lazy)
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
		for cacheKey, refs := range v {
			e := refs[0].(Entity)
			if e.IsLoaded() {
				values = append(values, cacheKey, buildRedisValue(e.getORM().dBData))
			} else {
				values = append(values, cacheKey, "nil")
			}
		}
		engine.GetRedis(pool).MSet(values...)
	}
	for pool, v := range localMap {
		if len(v) == 0 {
			continue
		}
		values := make([]interface{}, 0)
		for cacheKey, refs := range v {
			e := refs[0].(Entity)
			if e.IsLoaded() {
				values = append(values, cacheKey, buildLocalCacheValue(e.getORM().dBData))
			} else {
				values = append(values, cacheKey, "nil")
			}
		}
		engine.GetLocalCache(pool).MSet(values...)
	}

	for refName, entities := range referencesNextEntities {
		l := len(entities)
		if l == 1 {
			warmUpReferences(engine, entities[0].getORM().tableSchema, reflect.ValueOf(entities[0]),
				referencesNextNames[refName], false, lazy)
		} else if l > 1 {
			warmUpReferences(engine, entities[0].getORM().tableSchema, reflect.ValueOf(entities),
				referencesNextNames[refName], true, lazy)
		}
	}
}

func fillRef(key string, localMap map[string]map[string][]Entity,
	redisMap map[string]map[string][]Entity, dbMap map[string]map[*tableSchema]map[string][]Entity) {
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

func fillRefMap(engine *Engine, id uint64, referencesNextEntities map[string][]Entity, refName string, v Entity, parentSchema *tableSchema,
	dbMap map[string]map[*tableSchema]map[string][]Entity,
	localMap map[string]map[string][]Entity, redisMap map[string]map[string][]Entity) {
	_, has := referencesNextEntities[refName]
	if has {
		referencesNextEntities[refName] = append(referencesNextEntities[refName], v)
	}
	cacheKey := parentSchema.getCacheKey(id)
	if dbMap[parentSchema.mysqlPoolName] == nil {
		dbMap[parentSchema.mysqlPoolName] = make(map[*tableSchema]map[string][]Entity)
	}
	if dbMap[parentSchema.mysqlPoolName][parentSchema] == nil {
		dbMap[parentSchema.mysqlPoolName][parentSchema] = make(map[string][]Entity)
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
			localMap[localCacheName] = make(map[string][]Entity)
		}
		localMap[localCacheName][cacheKey] = append(localMap[localCacheName][cacheKey], v)
	}
	if parentSchema.hasRedisCache {
		if redisMap[parentSchema.redisCacheName] == nil {
			redisMap[parentSchema.redisCacheName] = make(map[string][]Entity)
		}
		redisMap[parentSchema.redisCacheName][cacheKey] = append(redisMap[parentSchema.redisCacheName][cacheKey], v)
	}
}
