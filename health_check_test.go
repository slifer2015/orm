package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHealthCheck(t *testing.T) {
	registry := &Registry{}
	engine := PrepareTables(t, registry, 8)
	errors, warnings, valid := engine.HealthCheck()
	assert.NotNil(t, valid)
	assert.Nil(t, errors)
	assert.Len(t, warnings, 1)
	assert.Len(t, valid, 3)
}
