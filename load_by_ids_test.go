package orm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type loadByIdsEntity struct {
	ORM          `orm:"localCache;redisCache"`
	ID           uint
	Name         string `orm:"max=100"`
	ReferenceOne *loadByIdsReference
}

type loadByIdsReference struct {
	ORM          `orm:"localCache;redisCache"`
	ID           uint
	Name         string
	ReferenceTwo *loadByIdsSubReference
}

type loadByIdsSubReference struct {
	ORM  `orm:"localCache;redisCache"`
	ID   uint
	Name string
}

func TestLoadByIds(t *testing.T) {
	var entity *loadByIdsEntity
	var reference *loadByIdsReference
	var subReference *loadByIdsSubReference
	engine := PrepareTables(t, &Registry{}, 5, entity, reference, subReference)

	engine.FlushMany(&loadByIdsEntity{Name: "a", ReferenceOne: &loadByIdsReference{Name: "r1", ReferenceTwo: &loadByIdsSubReference{Name: "s1"}}},
		&loadByIdsEntity{Name: "b", ReferenceOne: &loadByIdsReference{Name: "r2", ReferenceTwo: &loadByIdsSubReference{Name: "s2"}}},
		&loadByIdsEntity{Name: "c"})

	var rows []*loadByIdsEntity
	missing := engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows, "*")
	assert.Len(t, missing, 1)
	assert.Equal(t, uint64(4), missing[0])
	assert.Len(t, rows, 3)
	assert.Equal(t, "a", rows[0].Name)
	assert.Equal(t, "r1", rows[0].ReferenceOne.Name)
	assert.Equal(t, "b", rows[1].Name)
	assert.Equal(t, "r2", rows[1].ReferenceOne.Name)
	assert.Equal(t, "c", rows[2].Name)
	missing = engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows, "*")
	assert.Len(t, missing, 1)

	missing = engine.LoadByIDsLazy([]uint64{1, 2, 3, 4}, &rows, "*")
	assert.Len(t, missing, 1)
	assert.Equal(t, uint64(4), missing[0])
	assert.Len(t, rows, 3)
	assert.Equal(t, "", rows[0].Name)
	assert.False(t, rows[0].IsInitialised())
	assert.False(t, rows[1].IsInitialised())
	assert.False(t, rows[2].IsInitialised())
	assert.Equal(t, "a", rows[0].GetFieldLazy("Name"))
	assert.Equal(t, "", rows[0].ReferenceOne.Name)
	assert.Equal(t, "r1", rows[0].ReferenceOne.GetFieldLazy("Name"))
	assert.Equal(t, "", rows[1].Name)
	assert.Equal(t, "b", rows[1].GetFieldLazy("Name"))
	assert.Equal(t, "", rows[1].ReferenceOne.Name)
	assert.Equal(t, "r2", rows[1].ReferenceOne.GetFieldLazy("Name"))
	assert.Equal(t, "", rows[2].Name)
	assert.Equal(t, "c", rows[2].GetFieldLazy("Name"))
	missing = engine.LoadByIDsLazy([]uint64{1, 2, 3, 4}, &rows, "*")
	assert.Len(t, missing, 1)

	missing = engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows, "ReferenceOne/ReferenceTwo")
	assert.Len(t, missing, 1)
	assert.Len(t, rows, 3)
	assert.Equal(t, "a", rows[0].Name)
	assert.Equal(t, "r1", rows[0].ReferenceOne.Name)
	assert.Equal(t, "b", rows[1].Name)
	assert.Equal(t, "r2", rows[1].ReferenceOne.Name)
	assert.Equal(t, "c", rows[2].Name)

	missing = engine.LoadByIDs([]uint64{3}, &rows, "ReferenceOne/ReferenceTwo")
	assert.Len(t, missing, 0)

	assert.PanicsWithError(t, "reference invalid in loadByIdsEntity is not valid", func() {
		engine.LoadByIDs([]uint64{1}, &rows, "invalid")
	})

	assert.PanicsWithError(t, "reference tag Name is not valid", func() {
		engine.LoadByIDs([]uint64{1}, &rows, "Name")
	})

	engine = PrepareTables(t, &Registry{}, 5)
	assert.PanicsWithError(t, "entity 'orm.loadByIdsEntity' is not registered", func() {
		engine.LoadByIDs([]uint64{1}, &rows)
	})
}

// BenchmarkLoadByIDsdLocalCache-12    	  484204	      2144 ns/op	    1272 B/op	      11 allocs/op
func BenchmarkLoadByIDsdLocalCache(b *testing.B) {
	benchmarkLoadByIDsLocalCache(b, false)
}

// BenchmarkLoadByIDsLocalCacheLazy-12    	  943807	      1136 ns/op	    1032 B/op	       7 allocs/op
func BenchmarkLoadByIDsLocalCacheLazy(b *testing.B) {
	benchmarkLoadByIDsLocalCache(b, true)
}

func benchmarkLoadByIDsLocalCache(b *testing.B, lazy bool) {
	entity := &schemaEntity{}
	ref := &schemaEntityRef{}
	registry := &Registry{}
	registry.RegisterEnumStruct("orm.TestEnum", TestEnum)
	registry.RegisterLocalCache(10000)
	engine := PrepareTables(nil, registry, 5, entity, ref)

	ids := make([]uint64, 0)
	for i := 1; i <= 1; i++ {
		e := &schemaEntity{}
		e.Name = fmt.Sprintf("Name %d", i)
		e.Uint32 = uint32(i)
		e.Int32 = int32(i)
		e.Int8 = 1
		e.Enum = TestEnum.A
		e.RefOne = &schemaEntityRef{}
		engine.Flush(e)
		_ = engine.LoadByID(uint64(i), e)
		ids = append(ids, uint64(i))
	}
	rows := make([]*schemaEntity, 0)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		if lazy {
			_ = engine.LoadByIDsLazy(ids, &rows)
		} else {
			_ = engine.LoadByIDs(ids, &rows)
		}
	}
}
