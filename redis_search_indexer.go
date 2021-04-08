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
			for index, def := range defs {
				if canceled {
					return true
				}
				stamps := search.redis.HGetAll(redisSearchForceIndexKeyPrefix + index)
				for k, v := range stamps {
					id, _ := strconv.ParseUint(v, 10, 64)
					indexID, _ := strconv.ParseUint(k, 10, 64)
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
								search.redis.HSet(redisSearchForceIndexKeyPrefix+index, k, strconv.FormatUint(nextID, 10))
							}
						}

						if !hasMore {
							search.redis.HDel(redisSearchForceIndexKeyPrefix+index, k)
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
	DeleteDocuments(key ...string)
	SetString(key string, value string)
	SetTag(key string, tag ...string)
	SetUint(key string, value uint64)
	SetInt(key string, value int64)
	SetFloat(key string, value float64)
	SetGeo(key string, lon float64, lat float64)
	PushDocument()
	Flush()
	setField(key string, value interface{})
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

func (p *redisSearchIndexPusher) DeleteDocuments(key ...string) {
	p.pipeline.Del(key...)
}

func (p *redisSearchIndexPusher) SetString(key string, value string) {
	p.fields = append(p.fields, key, EscapeRedisSearchString(value))
}

func (p *redisSearchIndexPusher) setField(key string, value interface{}) {
	p.fields = append(p.fields, key, value)
}

func (p *redisSearchIndexPusher) SetTag(key string, tag ...string) {
	for i, val := range tag {
		if val == "" {
			tag[i] = "NULL"
		} else {
			tag[i] = EscapeRedisSearchString(val)
		}
	}
	p.fields = append(p.fields, key, strings.Join(tag, ","))
}

func (p *redisSearchIndexPusher) SetUint(key string, value uint64) {
	p.fields = append(p.fields, key, value)
}

func (p *redisSearchIndexPusher) SetInt(key string, value int64) {
	p.fields = append(p.fields, key, value)
}

func (p *redisSearchIndexPusher) SetFloat(key string, value float64) {
	p.fields = append(p.fields, key, value)
}

func (p *redisSearchIndexPusher) SetGeo(key string, lon float64, lat float64) {
	lonS := strconv.FormatFloat(lon, 'f', 6, 64)
	latS := strconv.FormatFloat(lat, 'f', 6, 64)
	p.fields = append(p.fields, key, lonS+","+latS)
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
