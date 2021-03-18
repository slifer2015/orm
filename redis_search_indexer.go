package orm

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type RedisSearchIndexer struct {
	engine            *Engine
	disableLoop       bool
	heartBeat         func()
	heartBeatDuration time.Duration
}

func NewRedisSearchIndexer(engine *Engine) *RedisSearchIndexer {
	return &RedisSearchIndexer{engine: engine}
}

func (r *RedisSearchIndexer) DisableLoop() {
	r.disableLoop = true
}

func (r *RedisSearchIndexer) SetHeartBeat(duration time.Duration, beat func()) {
	r.heartBeatDuration = duration
	r.heartBeat = beat
}

func (r *RedisSearchIndexer) Run(ctx context.Context) {
	for {
		valid := r.consume(ctx)
		if valid || r.disableLoop {
			break
		}
		time.Sleep(time.Second * 10)
	}
}

func (r *RedisSearchIndexer) consume(ctx context.Context) bool {
	canceled := false
	go func() {
		<-ctx.Done()
		canceled = true
	}()
	for {
		for pool, defs := range r.engine.registry.redisSearchIndexes {
			search := r.engine.GetRedisSearch(pool)
			stamps := search.redis.HGetAll(redisSearchForceIndexKey)
			for index, def := range defs {
				if canceled {
					return true
				}
				stamp, has := stamps[index]
				if !has {
					stamp = "0:" + strconv.FormatInt(time.Now().UnixNano(), 10)
				}
				if stamp[0:3] == "ok:" {
					continue
				}
				parts := strings.Split(stamp, ":")
				id, _ := strconv.ParseUint(parts[0], 10, 64)
				indexID, _ := strconv.ParseUint(parts[1], 10, 64)
				search.createIndex(def, indexID)
				indexName := def.Name + ":" + strconv.FormatUint(indexID, 10)

				pusher := &redisSearchIndexPusher{pipeline: search.redis.PipeLine()}
				for {
					if canceled {
						return true
					}
					hasMore := false
					nextID := uint64(0)
					if def.Indexer != nil {
						newID, hasNext := def.Indexer(r.engine, id, pusher)
						hasMore = hasNext
						nextID = newID
						if pusher.pipeline.commands > 0 {
							pusher.Flush()
						}
						if hasMore {
							search.redis.HSet(redisSearchForceIndexKey, index, strconv.FormatUint(nextID, 10)+":"+parts[1])
						}
					}

					if !hasMore {
						search.aliasUpdate(def.Name, indexName)
						search.redis.HSet(redisSearchForceIndexKey, index, "ok:"+parts[1])
						for _, oldName := range search.ListIndices() {
							if strings.HasPrefix(oldName, def.Name+":") {
								parts := strings.Split(oldName, ":")
								oldID, _ := strconv.ParseUint(parts[1], 10, 64)
								if oldID < indexID {
									search.dropIndex(oldName, false)
								}
							}
						}
						break
					}
					if nextID <= id {
						panic(errors.Errorf("loop detected in indxer for index %s in pool %s", index, pool))
					}
					id = nextID
				}
			}
			for index := range stamps {
				_, has := defs[index]
				if !has {
					search.redis.HDel(redisSearchForceIndexKey, index)
				}
			}
		}
		if r.disableLoop {
			break
		}
		time.Sleep(time.Second * 15)
	}
	return true
}

type RedisSearchIndexPusher interface {
	NewDocument(key string)
	SetField(key string, value interface{})
	PushDocument()
	Flush()
}

type RedisSearchIndexerFunc func(engine *Engine, lastID uint64, pusher RedisSearchIndexPusher) (newID uint64, hasMore bool)

type redisSearchIndexPusher struct {
	pipeline *RedisPipeLine
	key      string
	fields   []interface{}
}

func (e *Engine) NewRedisSearchIndexPusher(pool string) RedisSearchIndexPusher {
	return &redisSearchIndexPusher{pipeline: e.GetRedis(pool).PipeLine()}
}

func (p *redisSearchIndexPusher) NewDocument(key string) {
	p.key = key
}

func (p *redisSearchIndexPusher) SetField(key string, value interface{}) {
	p.fields = append(p.fields, key, value)
}

func (p *redisSearchIndexPusher) PushDocument() {
	p.pipeline.HSet(p.key, p.fields...)
	p.key = ""
	p.fields = p.fields[:0]
	if p.pipeline.commands > 10000 {
		p.Flush()
	}
}

func (p *redisSearchIndexPusher) Flush() {
	if p.pipeline.commands > 0 {
		p.pipeline.Exec()
		p.pipeline = p.pipeline.engine.GetRedis(p.pipeline.pool).PipeLine()
	}
}
