package tools

import (
	"testing"

	"github.com/latolukasz/orm"
	"github.com/stretchr/testify/assert"
)

func TestRedisStatistics(t *testing.T) {
	registry := &orm.Registry{}
	registry.RegisterRedis("localhost:6382", 15)
	registry.RegisterRedis("localhost:6382", 14, "another")
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRedis()
	r.FlushDB()

	stats := GetRedisStatistics(engine)
	assert.Len(t, stats, 1)
}
