package orm

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Bind map[string]interface{}

type DuplicatedKeyError struct {
	Message string
	Index   string
}

func (err *DuplicatedKeyError) Error() string {
	return err.Message
}

type ForeignKeyError struct {
	Message    string
	Constraint string
}

func (err *ForeignKeyError) Error() string {
	return err.Message
}

type dataLoaderSets map[*tableSchema]map[uint64][]interface{}

type Flusher interface {
	Track(entity ...Entity) Flusher
	Flush()
	FlushWithCheck() error
	FlushInTransactionWithCheck() error
	FlushWithFullCheck() error
	FlushLazy()
	FlushInTransaction()
	FlushWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration)
	FlushInTransactionWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration)
	Clear()
	MarkDirty(entity Entity, queueCode string, ids ...uint64)
	Delete(entity ...Entity) Flusher
	ForceDelete(entity ...Entity) Flusher
}

type flusher struct {
	engine                 *Engine
	trackedEntities        []Entity
	trackedEntitiesCounter int
	mutex                  sync.Mutex
	redisFlusher           *redisFlusher
}

func (f *flusher) Track(entity ...Entity) Flusher {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	for _, entity := range entity {
		initIfNeeded(f.engine, entity)
		if f.trackedEntities == nil {
			f.trackedEntities = []Entity{entity}
		} else {
			f.trackedEntities = append(f.trackedEntities, entity)
		}
		f.trackedEntitiesCounter++
		if f.trackedEntitiesCounter == 10001 {
			panic(fmt.Errorf("track limit 10000 exceeded"))
		}
	}
	return f
}

func (f *flusher) Delete(entity ...Entity) Flusher {
	for _, e := range entity {
		e.markToDelete()
	}
	f.Track(entity...)
	return f
}

func (f *flusher) ForceDelete(entity ...Entity) Flusher {
	for _, e := range entity {
		e.forceMarkToDelete()
	}
	f.Track(entity...)
	return f
}

func (f *flusher) Flush() {
	f.flushTrackedEntities(false, false)
}

func (f *flusher) FlushWithCheck() error {
	return f.flushWithCheck(false)
}

func (f *flusher) FlushInTransactionWithCheck() error {
	return f.flushWithCheck(true)
}

func (f *flusher) FlushWithFullCheck() error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				f.Clear()
				asErr := r.(error)
				err = asErr
			}
		}()
		f.flushTrackedEntities(false, false)
	}()
	return err
}

func (f *flusher) FlushLazy() {
	f.flushTrackedEntities(true, false)
}

func (f *flusher) FlushInTransaction() {
	f.flushTrackedEntities(false, true)
}

func (f *flusher) FlushWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) {
	f.flushWithLock(false, lockerPool, lockName, ttl, waitTimeout)
}

func (f *flusher) FlushInTransactionWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) {
	f.flushWithLock(true, lockerPool, lockName, ttl, waitTimeout)
}

func (f *flusher) Clear() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.trackedEntities = nil
	f.trackedEntitiesCounter = 0
}

func (f *flusher) MarkDirty(entity Entity, queueCode string, ids ...uint64) {
	entityName := f.engine.GetRegistry().GetTableSchemaForEntity(entity).GetType().String()
	flusher := f.engine.GetEventBroker().NewFlusher()
	for _, id := range ids {
		flusher.PublishMap(queueCode, EventAsMap{"A": "u", "I": id, "E": entityName})
	}
	flusher.Flush()
}

func (f *flusher) flushTrackedEntities(lazy bool, transaction bool) {
	if f.trackedEntitiesCounter == 0 {
		return
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	var dbPools map[string]*DB
	if transaction {
		dbPools = make(map[string]*DB)
		for _, entity := range f.trackedEntities {
			db := entity.getORM().tableSchema.GetMysql(f.engine)
			dbPools[db.code] = db
		}
		for _, db := range dbPools {
			db.Begin()
		}
	}
	defer func() {
		for _, db := range dbPools {
			db.Rollback()
		}
	}()
	f.flush(nil, nil, true, lazy, transaction, f.trackedEntities...)
	if transaction {
		for _, db := range dbPools {
			db.Commit()
		}
	}
}

func (f *flusher) flushWithCheck(transaction bool) error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				f.Clear()
				asErr := r.(error)
				assErr1, is := asErr.(*ForeignKeyError)
				if is {
					err = assErr1
					return
				}
				assErr2, is := asErr.(*DuplicatedKeyError)
				if is {
					err = assErr2
					return
				}
				panic(asErr)
			}
		}()
		f.flushTrackedEntities(false, transaction)
	}()
	return err
}

func (f *flusher) flushWithLock(transaction bool, lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) {
	locker := f.engine.GetLocker(lockerPool)
	lock, has := locker.Obtain(f.engine.context, lockName, ttl, waitTimeout)
	if !has {
		panic(errors.New("lock wait timeout"))
	}
	defer lock.Release()
	f.flushTrackedEntities(false, transaction)
}

func (f *flusher) flush(updateSQLs map[string][]string, deleteBinds map[reflect.Type]map[uint64][]interface{},
	root bool, lazy bool, transaction bool, entities ...Entity) {
	insertKeys := make(map[reflect.Type][]string)
	insertValues := make(map[reflect.Type]string)
	insertArguments := make(map[reflect.Type][]interface{})
	insertBinds := make(map[reflect.Type][]map[string]interface{})
	insertReflectValues := make(map[reflect.Type][]Entity)
	totalInsert := make(map[reflect.Type]int)
	localCacheSets := make(map[string]map[string][]interface{})
	dataLoaderSets := make(map[*tableSchema]map[uint64][]interface{})
	localCacheDeletes := make(map[string]map[string]bool)
	lazyMap := make(map[string]interface{})
	isInTransaction := transaction

	var referencesToFlash map[Entity]Entity

	for _, entity := range entities {
		initIfNeeded(f.engine, entity).initDBData()
		schema := entity.getORM().tableSchema
		if !isInTransaction && schema.GetMysql(f.engine).inTransaction {
			isInTransaction = true
		}
		for _, refName := range schema.refOne {
			refValue := entity.getORM().elem.FieldByName(refName)
			if refValue.IsValid() && !refValue.IsNil() {
				refEntity := refValue.Interface().(Entity)
				initIfNeeded(f.engine, refEntity).initDBData()
				if refEntity.GetID() == 0 {
					if referencesToFlash == nil {
						referencesToFlash = make(map[Entity]Entity)
					}
					referencesToFlash[refEntity] = refEntity
				}
			}
		}
		for _, refName := range schema.refMany {
			refValue := entity.getORM().elem.FieldByName(refName)
			if refValue.IsValid() && !refValue.IsNil() {
				length := refValue.Len()
				for i := 0; i < length; i++ {
					refEntity := refValue.Index(i).Interface().(Entity)
					initIfNeeded(f.engine, refEntity)
					if refEntity.GetID() == 0 {
						if referencesToFlash == nil {
							referencesToFlash = make(map[Entity]Entity)
						}
						referencesToFlash[refEntity] = refEntity
					}
				}
			}
		}
		if referencesToFlash != nil {
			continue
		}

		orm := entity.getORM()
		dbData := orm.dBData
		bind, updateBind, isDirty := orm.getDirtyBind()
		if !isDirty {
			continue
		}
		bindLength := len(bind)

		t := orm.tableSchema.t
		currentID := entity.GetID()
		if orm.fakeDelete && !orm.tableSchema.hasFakeDelete {
			orm.delete = true
		}
		if orm.delete {
			if deleteBinds == nil {
				deleteBinds = make(map[reflect.Type]map[uint64][]interface{})
			}
			if deleteBinds[t] == nil {
				deleteBinds[t] = make(map[uint64][]interface{})
			}
			deleteBinds[t][currentID] = dbData
		} else if !orm.inDB {
			onUpdate := entity.getORM().onDuplicateKeyUpdate
			if onUpdate != nil {
				if lazy {
					panic(fmt.Errorf("lazy flush on duplicate key is not supported"))
				}
				if currentID > 0 {
					bind["ID"] = currentID
					bindLength++
				}
				values := make([]string, bindLength)
				columns := make([]string, bindLength)
				bindRow := make([]interface{}, bindLength)
				i := 0
				for key, val := range bind {
					columns[i] = "`" + key + "`"
					values[i] = "?"
					bindRow[i] = val
					i++
				}
				/* #nosec */
				sql := "INSERT INTO " + schema.tableName + "(" + strings.Join(columns, ",") + ") VALUES (" + strings.Join(values, ",") + ")"
				sql += " ON DUPLICATE KEY UPDATE "
				first := true
				for k, v := range onUpdate {
					if !first {
						sql += ", "
					}
					sql += "`" + k + "` = ?"
					bindRow = append(bindRow, v)
					first = false
				}
				if len(onUpdate) == 0 {
					sql += "`Id` = `Id`"
				}
				db := schema.GetMysql(f.engine)
				result := db.Exec(sql, bindRow...)
				affected := result.RowsAffected()
				if affected > 0 {
					lastID := result.LastInsertId()
					f.injectBind(entity, bind)
					orm := entity.getORM()
					orm.idElem.SetUint(lastID)
					orm.dBData[0] = lastID
					if affected == 1 {
						f.updateCacheForInserted(entity, lazy, lastID, bind, localCacheSets, localCacheDeletes, dataLoaderSets)
					} else {
						for k, v := range onUpdate {
							err := entity.SetField(k, v)
							checkError(err)
						}
						bind, _ := orm.GetDirtyBind()
						_, _, _ = loadByID(f.engine, lastID, entity, true, false)
						f.updateCacheAfterUpdate(dbData, entity, bind, schema, localCacheSets, localCacheDeletes, db, lastID, dataLoaderSets)
					}
				} else {
				OUTER:
					for _, index := range schema.uniqueIndices {
						fields := make([]string, 0)
						binds := make([]interface{}, 0)
						for _, column := range index {
							if bind[column] == nil {
								continue OUTER
							}
							fields = append(fields, "`"+column+"` = ?")
							binds = append(binds, bind[column])
						}
						findWhere := NewWhere(strings.Join(fields, " AND "), binds)
						f.engine.SearchOne(findWhere, entity)
						break
					}
				}
				continue
			}
			if currentID > 0 {
				bind["ID"] = currentID
				bindLength++
			}

			values := make([]interface{}, bindLength)
			valuesKeys := make([]string, bindLength)
			if insertKeys[t] == nil {
				fields := make([]string, bindLength)
				i := 0
				for key := range bind {
					fields[i] = key
					i++
				}
				insertKeys[t] = fields
			}
			for index, key := range insertKeys[t] {
				value := bind[key]
				values[index] = value
				valuesKeys[index] = "?"
			}
			_, has := insertArguments[t]
			if !has {
				insertArguments[t] = make([]interface{}, 0)
				insertReflectValues[t] = make([]Entity, 0)
				insertBinds[t] = make([]map[string]interface{}, 0)
				insertValues[t] = "(" + strings.Join(valuesKeys, ",") + ")"
			}
			insertArguments[t] = append(insertArguments[t], values...)
			insertReflectValues[t] = append(insertReflectValues[t], entity)
			insertBinds[t] = append(insertBinds[t], bind)
			totalInsert[t]++
		} else {
			if !entity.Loaded() {
				panic(fmt.Errorf("entity is not loaded and can't be updated: %v [%d]", entity.getORM().elem.Type().String(), currentID))
			}
			fields := make([]string, bindLength)
			i := 0
			for key, value := range updateBind {
				fields[i] = "`" + key + "`=" + value
				i++
			}
			/* #nosec */
			sql := "UPDATE " + schema.GetTableName() + " SET " + strings.Join(fields, ",") + " WHERE `ID` = " + strconv.FormatUint(currentID, 10)
			db := schema.GetMysql(f.engine)
			if lazy {
				f.fillLazyQuery(lazyMap, db.GetPoolCode(), sql, nil)
			} else {
				if updateSQLs == nil {
					updateSQLs = make(map[string][]string)
				}
				updateSQLs[schema.mysqlPoolName] = append(updateSQLs[schema.mysqlPoolName], sql)
			}
			f.updateCacheAfterUpdate(dbData, entity, bind, schema, localCacheSets, localCacheDeletes, db, currentID, dataLoaderSets)
		}
	}

	if referencesToFlash != nil {
		if lazy {
			panic(fmt.Errorf("lazy flush for unsaved references is not supported"))
		}
		toFlush := make([]Entity, len(referencesToFlash))
		i := 0
		for _, v := range referencesToFlash {
			toFlush[i] = v
			i++
		}
		f.flush(updateSQLs, deleteBinds, false, false, transaction, toFlush...)
		rest := make([]Entity, 0)
		for _, v := range entities {
			_, has := referencesToFlash[v]
			if !has {
				rest = append(rest, v)
			}
		}
		f.flush(updateSQLs, deleteBinds, true, false, transaction, rest...)
		return
	}
	for typeOf, values := range insertKeys {
		schema := getTableSchema(f.engine.registry, typeOf)
		finalValues := make([]string, len(values))
		for key, val := range values {
			finalValues[key] = "`" + val + "`"
		}
		/* #nosec */
		sql := "INSERT INTO " + schema.tableName + "(" + strings.Join(finalValues, ",") + ") VALUES " + insertValues[typeOf]
		for i := 1; i < totalInsert[typeOf]; i++ {
			sql += "," + insertValues[typeOf]
		}
		id := uint64(0)
		db := schema.GetMysql(f.engine)
		if lazy {
			f.fillLazyQuery(lazyMap, db.GetPoolCode(), sql, insertArguments[typeOf])
		} else {
			res := db.Exec(sql, insertArguments[typeOf]...)
			id = res.LastInsertId()
		}
		for key, entity := range insertReflectValues[typeOf] {
			bind := insertBinds[typeOf][key]
			f.injectBind(entity, bind)
			insertedID := entity.GetID()
			if insertedID == 0 {
				orm := entity.getORM()
				orm.idElem.SetUint(id)
				orm.dBData[0] = id
				insertedID = id
				id = id + db.autoincrement
			}
			f.updateCacheForInserted(entity, lazy, insertedID, bind, localCacheSets, localCacheDeletes, dataLoaderSets)
		}
	}
	if root {
		for pool, queries := range updateSQLs {
			db := f.engine.GetMysql(pool)
			if len(queries) == 1 {
				db.Exec(queries[0])
				continue
			}
			_, def := db.Query(strings.Join(queries, ";") + ";")
			def()
		}
		for typeOf, deleteBinds := range deleteBinds {
			schema := getTableSchema(f.engine.registry, typeOf)
			ids := make([]interface{}, len(deleteBinds))
			i := 0
			for id := range deleteBinds {
				ids[i] = id
				i++
			}
			/* #nosec */
			sql := "DELETE FROM `" + schema.tableName + "` WHERE " + NewWhere("`ID` IN ?", ids).String()
			db := schema.GetMysql(f.engine)
			if lazy {
				f.fillLazyQuery(lazyMap, db.GetPoolCode(), sql, ids)
			} else {
				usage := schema.GetUsage(f.engine.registry)
				if len(usage) > 0 {
					for refT, refColumns := range usage {
						for _, refColumn := range refColumns {
							refSchema := getTableSchema(f.engine.registry, refT)
							_, isCascade := refSchema.tags[refColumn]["cascade"]
							if isCascade {
								subValue := reflect.New(reflect.SliceOf(reflect.PtrTo(refT)))
								subElem := subValue.Elem()
								sub := subValue.Interface()
								pager := NewPager(1, 1000)
								where := NewWhere("`"+refColumn+"` IN ?", ids)
								for {
									f.engine.Search(where, pager, sub)
									total := subElem.Len()
									if total == 0 {
										break
									}
									toDeleteAll := make([]Entity, total)
									for i := 0; i < total; i++ {
										toDeleteValue := subElem.Index(i).Interface().(Entity)
										toDeleteValue.markToDelete()
										toDeleteAll[i] = toDeleteValue
									}
									f.flush(nil, nil, true, transaction, lazy, toDeleteAll...)
								}
							}
						}
					}
				}
				_ = db.Exec(sql, ids...)
			}

			localCache, hasLocalCache := schema.GetLocalCache(f.engine)
			redisCache, hasRedis := schema.GetRedisCache(f.engine)
			if !hasLocalCache && f.engine.hasRequestCache {
				hasLocalCache = true
				localCache = f.engine.GetLocalCache(requestCacheKey)
			}
			for id, dbData := range deleteBinds {
				bind := f.convertDBDataToMap(schema, dbData)
				f.addDirtyQueues(bind, schema, id, "d")
				f.addToLogQueue(schema, id, bind, nil, nil)
				if hasLocalCache {
					f.addLocalCacheSet(localCacheSets, db.GetPoolCode(), localCache.code, schema.getCacheKey(id), "nil")
					keys := f.getCacheQueriesKeys(schema, bind, dbData, true)
					f.addLocalCacheDeletes(localCacheDeletes, localCache.code, keys...)
				} else if f.engine.dataLoader != nil {
					f.addToDataLoader(dataLoaderSets, schema, id, nil)
				}
				if hasRedis {
					f.getRedisFlusher().Del(redisCache.code, schema.getCacheKey(id))
					keys := f.getCacheQueriesKeys(schema, bind, dbData, true)
					f.getRedisFlusher().Del(redisCache.code, keys...)
				}
				if schema.hasSearchCache {
					key := schema.redisSearchPrefix + strconv.FormatUint(id, 10)
					f.getRedisFlusher().Del(schema.searchCacheName, key)
				}
			}
		}
	}
	for _, values := range localCacheSets {
		for cacheCode, keys := range values {
			cache := f.engine.GetLocalCache(cacheCode)
			if !isInTransaction {
				cache.MSet(keys...)
			} else {
				if f.engine.afterCommitLocalCacheSets == nil {
					f.engine.afterCommitLocalCacheSets = make(map[string][]interface{})
				}
				f.engine.afterCommitLocalCacheSets[cacheCode] = append(f.engine.afterCommitLocalCacheSets[cacheCode], keys...)
			}
		}
	}
	for cacheCode, allKeys := range localCacheDeletes {
		keys := make([]string, len(allKeys))
		i := 0
		for key := range allKeys {
			keys[i] = key
			i++
		}
		f.engine.GetLocalCache(cacheCode).Remove(keys...)
	}
	if lazy {
		deletesRedisCache, has := lazyMap["cr"].(map[string][]string)
		if !has {
			deletesRedisCache = make(map[string][]string)
			lazyMap["cr"] = deletesRedisCache
		}
		for cacheCode, commands := range f.redisFlusher.pipelines {
			if commands.deletes != nil {
				deletesRedisCache[cacheCode] = commands.deletes
			}
		}
	} else if isInTransaction {
		f.engine.afterCommitRedisFlusher = f.redisFlusher
	}
	for schema, rows := range dataLoaderSets {
		if !isInTransaction {
			for id, value := range rows {
				f.engine.dataLoader.Prime(schema, id, value)
			}
		} else {
			if f.engine.afterCommitDataLoaderSets == nil {
				f.engine.afterCommitDataLoaderSets = make(map[*tableSchema]map[uint64][]interface{})
			}
			if f.engine.afterCommitDataLoaderSets[schema] == nil {
				f.engine.afterCommitDataLoaderSets[schema] = make(map[uint64][]interface{})
			}
			for id, value := range rows {
				f.engine.afterCommitDataLoaderSets[schema][id] = value
			}
		}
	}
	if len(lazyMap) > 0 {
		f.getRedisFlusher().Publish(lazyChannelName, lazyMap)
	}
	if f.redisFlusher != nil && !isInTransaction && root {
		f.redisFlusher.Flush()
	}
}

func (f *flusher) updateCacheForInserted(entity Entity, lazy bool, id uint64,
	bind map[string]interface{}, localCacheSets map[string]map[string][]interface{}, localCacheDeletes map[string]map[string]bool,
	dataLoaderSets dataLoaderSets) {
	schema := entity.getORM().tableSchema
	localCache, hasLocalCache := schema.GetLocalCache(f.engine)
	if !hasLocalCache && f.engine.hasRequestCache {
		hasLocalCache = true
		localCache = f.engine.GetLocalCache(requestCacheKey)
	}
	if hasLocalCache {
		if !lazy {
			f.addLocalCacheSet(localCacheSets, schema.GetMysql(f.engine).GetPoolCode(), localCache.code, schema.getCacheKey(id), buildLocalCacheValue(entity))
		} else {
			f.addLocalCacheDeletes(localCacheDeletes, localCache.code, schema.getCacheKey(id))
		}
		keys := f.getCacheQueriesKeys(schema, bind, entity.getORM().dBData, true)
		f.addLocalCacheDeletes(localCacheDeletes, localCache.code, keys...)
	} else if !lazy && f.engine.dataLoader != nil {
		f.addToDataLoader(dataLoaderSets, schema, id, buildLocalCacheValue(entity))
	}
	redisCache, hasRedis := schema.GetRedisCache(f.engine)
	if hasRedis {
		f.getRedisFlusher().Del(redisCache.code, schema.getCacheKey(id))
		keys := f.getCacheQueriesKeys(schema, bind, entity.getORM().dBData, true)
		f.getRedisFlusher().Del(redisCache.code, keys...)
	}
	f.fillRedisSearchFromBind(schema, bind, id)

	f.addDirtyQueues(bind, schema, id, "i")
	f.addToLogQueue(schema, id, nil, bind, entity.getORM().logMeta)
}

func (f *flusher) getRedisFlusher() *redisFlusher {
	if f.redisFlusher == nil {
		f.redisFlusher = f.engine.afterCommitRedisFlusher
		if f.redisFlusher == nil {
			f.redisFlusher = &redisFlusher{engine: f.engine}
		}
	}
	return f.redisFlusher
}

func (f *flusher) updateCacheAfterUpdate(dbData []interface{}, entity Entity, bind map[string]interface{},
	schema *tableSchema, localCacheSets map[string]map[string][]interface{}, localCacheDeletes map[string]map[string]bool,
	db *DB, currentID uint64, dataLoaderSets dataLoaderSets) {
	old := make([]interface{}, len(dbData))
	copy(old, dbData)
	f.injectBind(entity, bind)
	localCache, hasLocalCache := schema.GetLocalCache(f.engine)
	if !hasLocalCache && f.engine.hasRequestCache {
		hasLocalCache = true
		localCache = f.engine.GetLocalCache(requestCacheKey)
	}
	redisCache, hasRedis := schema.GetRedisCache(f.engine)
	if hasLocalCache {
		cacheKey := schema.getCacheKey(currentID)
		f.addLocalCacheSet(localCacheSets, db.GetPoolCode(), localCache.code, cacheKey, buildLocalCacheValue(entity))
		keys := f.getCacheQueriesKeys(schema, bind, dbData, false)
		f.addLocalCacheDeletes(localCacheDeletes, localCache.code, keys...)
		keys = f.getCacheQueriesKeys(schema, bind, old, false)
		f.addLocalCacheDeletes(localCacheDeletes, localCache.code, keys...)
	} else if f.engine.dataLoader != nil {
		f.addToDataLoader(dataLoaderSets, schema, currentID, buildLocalCacheValue(entity))
	}
	if hasRedis {
		redisFlusher := f.getRedisFlusher()
		redisFlusher.Del(redisCache.code, schema.getCacheKey(currentID))
		keys := f.getCacheQueriesKeys(schema, bind, dbData, false)
		redisFlusher.Del(redisCache.code, keys...)
		keys = f.getCacheQueriesKeys(schema, bind, old, false)
		redisFlusher.Del(redisCache.code, keys...)
	}
	f.fillRedisSearchFromBind(schema, bind, entity.GetID())
	f.addDirtyQueues(bind, schema, currentID, "u")
	f.addToLogQueue(schema, currentID, f.convertDBDataToMap(schema, old), bind, entity.getORM().logMeta)
}

func (f *flusher) addDirtyQueues(bind map[string]interface{}, schema *tableSchema, id uint64, action string) {
	key := EventAsMap{"E": schema.t.String(), "I": id, "A": action}
	for column, tags := range schema.tags {
		queues, has := tags["dirty"]
		if !has {
			continue
		}
		isDirty := column == "ORM"
		if !isDirty {
			_, isDirty = bind[column]
		}
		if !isDirty {
			continue
		}
		queueNames := strings.Split(queues, ",")
		for _, queueName := range queueNames {
			f.getRedisFlusher().PublishMap(queueName, key)
		}
	}
}

func (f *flusher) addToLogQueue(tableSchema *tableSchema, id uint64,
	before map[string]interface{}, changes map[string]interface{}, entityMeta map[string]interface{}) {
	if !tableSchema.hasLog {
		return
	}
	if changes != nil && len(tableSchema.skipLogs) > 0 {
		skipped := 0
		for _, skip := range tableSchema.skipLogs {
			_, has := changes[skip]
			if has {
				skipped++
			}
		}
		if skipped == len(changes) {
			return
		}
	}
	val := &LogQueueValue{TableName: tableSchema.logTableName, ID: id,
		PoolName: tableSchema.logPoolName, Before: before,
		Changes: changes, Updated: time.Now(), Meta: entityMeta}
	if val.Meta == nil {
		val.Meta = f.engine.logMetaData
	} else {
		for k, v := range f.engine.logMetaData {
			val.Meta[k] = v
		}
	}
	f.getRedisFlusher().Publish(logChannelName, val)
}

func (f *flusher) fillRedisSearchFromBind(schema *tableSchema, bind map[string]interface{}, id uint64) {
	if schema.hasSearchCache {
		if schema.hasFakeDelete {
			val, has := bind["FakeDelete"]
			if has && val.(uint64) > 0 {
				f.getRedisFlusher().Del(schema.searchCacheName, schema.redisSearchPrefix+strconv.FormatUint(id, 10))
			}
		}
		values := make([]interface{}, 0)
		for k, f := range schema.mapBindToRedisSearch {
			v, has := bind[k]
			if has {
				values = append(values, k, f(v))
			}
		}
		if len(values) > 0 {
			f.getRedisFlusher().HSet(schema.searchCacheName, schema.redisSearchPrefix+strconv.FormatUint(id, 10), values...)
		}
	}
}

func (f *flusher) convertDBDataToMap(schema *tableSchema, data []interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	for _, name := range schema.columnNames[1:] {
		m[name] = data[schema.columnMapping[name]]
	}
	return m
}

func (f *flusher) injectBind(entity Entity, bind map[string]interface{}) {
	orm := entity.getORM()
	mapping := orm.tableSchema.columnMapping
	orm.initDBData()
	for key, value := range bind {
		orm.dBData[mapping[key]] = value
	}
	orm.loaded = true
	orm.inDB = true
}

func (f *flusher) getCacheQueriesKeys(schema *tableSchema, bind map[string]interface{}, data []interface{}, addedDeleted bool) (keys []string) {
	keys = make([]string, 0)

	for indexName, definition := range schema.cachedIndexesAll {
		if !addedDeleted && schema.hasFakeDelete {
			_, addedDeleted = bind["FakeDelete"]
		}
		if addedDeleted && len(definition.TrackedFields) == 0 {
			keys = append(keys, getCacheKeySearch(schema, indexName))
		}
		for _, trackedField := range definition.TrackedFields {
			_, has := bind[trackedField]
			if has {
				attributes := make([]interface{}, 0)
				for _, trackedFieldSub := range definition.QueryFields {
					val := data[schema.columnMapping[trackedFieldSub]]
					if !schema.hasFakeDelete || trackedFieldSub != "FakeDelete" {
						attributes = append(attributes, val)
					}
				}
				keys = append(keys, getCacheKeySearch(schema, indexName, attributes...))
				break
			}
		}
	}
	return
}

func (f *flusher) addLocalCacheSet(localCacheSets map[string]map[string][]interface{}, dbCode string, cacheCode string, keys ...interface{}) {
	if localCacheSets[dbCode] == nil {
		localCacheSets[dbCode] = make(map[string][]interface{})
	}
	localCacheSets[dbCode][cacheCode] = append(localCacheSets[dbCode][cacheCode], keys...)
}

func (f *flusher) addToDataLoader(values map[*tableSchema]map[uint64][]interface{}, schema *tableSchema, id uint64, value []interface{}) {
	if values[schema] == nil {
		values[schema] = make(map[uint64][]interface{})
	}
	values[schema][id] = value
}

func (f *flusher) addLocalCacheDeletes(cacheDeletes map[string]map[string]bool, cacheCode string, keys ...string) {
	if len(keys) == 0 {
		return
	}
	if cacheDeletes[cacheCode] == nil {
		cacheDeletes[cacheCode] = make(map[string]bool)
	}
	for _, key := range keys {
		cacheDeletes[cacheCode][key] = true
	}
}

func (f *flusher) fillLazyQuery(lazyMap map[string]interface{}, dbCode string, sql string, values []interface{}) {
	updatesMap := lazyMap["q"]
	if updatesMap == nil {
		updatesMap = make([]interface{}, 0)
		lazyMap["q"] = updatesMap
	}
	lazyValue := make([]interface{}, 3)
	lazyValue[0] = dbCode
	lazyValue[1] = sql
	lazyValue[2] = values
	lazyMap["q"] = append(updatesMap.([]interface{}), lazyValue)
}
