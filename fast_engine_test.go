package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type fastEngineEntity struct {
	ORM       `orm:"localCache;redisCache"`
	ID        uint
	Name      string `orm:"max=100;unique=FirstIndex"`
	Reference *fastEngineReferenceEntity
	IndexAll  *CachedQuery `query:""`
	IndexName *CachedQuery `queryOne:":Name = ?"`
}

type fastEngineReferenceEntity struct {
	ORM  `orm:"localCache;redisCache"`
	ID   uint
	Name string `orm:"max=100;unique=FirstIndex"`
}

func TestFastEngine(t *testing.T) {
	var entity *fastEngineEntity
	var reference *fastEngineReferenceEntity
	engine := PrepareTables(t, &Registry{}, 5, entity, reference)
	engine.FlushMany(&fastEngineReferenceEntity{Name: "a1"}, &fastEngineReferenceEntity{Name: "a2"})
	engine.FlushMany(
		&fastEngineEntity{Name: "a", Reference: &fastEngineReferenceEntity{ID: 1}},
		&fastEngineEntity{Name: "b", Reference: &fastEngineReferenceEntity{ID: 2}},
		&fastEngineEntity{Name: "c"})

	fastEngine := engine.NewFastEngine()
	has, fastEntity := fastEngine.LoadByID(1, entity)
	assert.True(t, has)
	assert.NotNil(t, fastEntity)
	assert.Equal(t, uint64(1), fastEntity.GetID())
	assert.Equal(t, "a", fastEntity.Get("Name"))
	assert.True(t, fastEntity.Is(entity))
	assert.PanicsWithError(t, "unknown field Invalid", func() {
		fastEntity.Get("Invalid")
	})
	entity = fastEntity.Entity().(*fastEngineEntity)
	assert.Equal(t, "a", entity.Name)

	has, fastEntity = fastEngine.LoadByID(1, entity, "Reference")
	assert.True(t, has)
	assert.NotNil(t, fastEntity)
	assert.Equal(t, uint64(1), fastEntity.GetID())
	assert.Equal(t, "a", fastEntity.Get("Name"))
	assert.NotNil(t, fastEntity.Get("Reference"))
	fastE, is := fastEntity.Get("Reference").(FastEntity)
	assert.True(t, is)
	assert.Equal(t, uint64(1), fastE.GetID())
	assert.Equal(t, "a1", fastE.Get("Name"))

	engine.GetLocalCache().Clear()
	has, fastEntity = fastEngine.LoadByID(3, entity, "Reference")
	assert.True(t, has)
	assert.NotNil(t, fastEntity)
	assert.Equal(t, uint64(3), fastEntity.GetID())
	assert.Equal(t, "c", fastEntity.Get("Name"))
	assert.Nil(t, fastEntity.Get("Reference"))

	results, missing := fastEngine.LoadByIDs([]uint64{1, 2, 3, 4}, entity)
	assert.NotNil(t, results)
	assert.NotNil(t, missing)
	assert.Len(t, results, 3)
	assert.Len(t, missing, 1)
	assert.Equal(t, uint64(4), missing[0])
	assert.Equal(t, uint64(1), results[0].GetID())
	assert.Equal(t, uint64(2), results[1].GetID())
	assert.Equal(t, uint64(3), results[2].GetID())
	assert.Equal(t, "a", results[0].Get("Name"))
	assert.Equal(t, "b", results[1].Get("Name"))
	assert.Equal(t, "c", results[2].Get("Name"))

	results, missing = fastEngine.LoadByIDs([]uint64{1, 2, 3, 4}, entity, "Reference")
	assert.NotNil(t, results)
	assert.NotNil(t, missing)
	assert.Len(t, results, 3)
	assert.Len(t, missing, 1)
	assert.Equal(t, uint64(4), missing[0])
	assert.Equal(t, uint64(1), results[0].GetID())
	assert.Equal(t, uint64(2), results[1].GetID())
	assert.Equal(t, uint64(3), results[2].GetID())
	assert.Equal(t, "a", results[0].Get("Name"))
	assert.Equal(t, "b", results[1].Get("Name"))
	assert.Equal(t, "c", results[2].Get("Name"))
	assert.NotNil(t, results[0].Get("Reference"))
	assert.Equal(t, uint64(1), results[0].Get("Reference").(FastEntity).GetID())
	assert.Equal(t, "a1", results[0].Get("Reference").(FastEntity).Get("Name"))
	assert.Equal(t, uint64(2), results[1].Get("Reference").(FastEntity).GetID())
	assert.Equal(t, "a2", results[1].Get("Reference").(FastEntity).Get("Name"))
	assert.Nil(t, results[2].Get("Reference"))

	results = fastEngine.Search(NewWhere("ID < 50 ORDER BY ID DESC"), NewPager(1, 10), entity)
	assert.NotNil(t, results)
	assert.Len(t, results, 3)
	assert.Equal(t, uint64(3), results[0].GetID())
	assert.Equal(t, uint64(2), results[1].GetID())
	assert.Equal(t, uint64(1), results[2].GetID())
	assert.Equal(t, "c", results[0].Get("Name"))
	assert.Equal(t, "b", results[1].Get("Name"))
	assert.Equal(t, "a", results[2].Get("Name"))

	total, results := fastEngine.SearchWithCount(NewWhere("ID < 3 ORDER BY ID DESC"), NewPager(1, 10), entity)
	assert.Equal(t, 2, total)
	assert.NotNil(t, results)
	assert.Len(t, results, 2)
	assert.Equal(t, uint64(2), results[0].GetID())
	assert.Equal(t, uint64(1), results[1].GetID())
	assert.Equal(t, "b", results[0].Get("Name"))
	assert.Equal(t, "a", results[1].Get("Name"))

	found, result := fastEngine.SearchOne(NewWhere("ID = 3 ORDER BY ID DESC"), entity)
	assert.True(t, found)
	assert.NotNil(t, result)
	assert.Equal(t, uint64(3), result.GetID())
	assert.Equal(t, "c", result.Get("Name"))

	total, results = fastEngine.CachedSearch(entity, "IndexAll", NewPager(1, 10))
	assert.Equal(t, 3, total)
	assert.Len(t, results, 3)
	assert.Equal(t, uint64(1), results[0].GetID())
	assert.Equal(t, uint64(2), results[1].GetID())
	assert.Equal(t, uint64(3), results[2].GetID())
	assert.Equal(t, "a", results[0].Get("Name"))
	assert.Equal(t, "b", results[1].Get("Name"))
	assert.Equal(t, "c", results[2].Get("Name"))

	total, results = fastEngine.CachedSearchWithReferences(entity, "IndexAll", NewPager(1, 10), nil, []string{})
	assert.Equal(t, 3, total)
	assert.Len(t, results, 3)
	assert.Equal(t, uint64(1), results[0].GetID())
	assert.Equal(t, uint64(2), results[1].GetID())
	assert.Equal(t, uint64(3), results[2].GetID())
	assert.Equal(t, "a", results[0].Get("Name"))
	assert.Equal(t, "b", results[1].Get("Name"))
	assert.Equal(t, "c", results[2].Get("Name"))

	found, result = fastEngine.CachedSearchOne(entity, "IndexName", "b")
	assert.True(t, found)
	assert.NotNil(t, result)
	assert.Equal(t, uint64(2), result.GetID())
	assert.Equal(t, "b", result.Get("Name"))

	found, result = fastEngine.CachedSearchOneWithReferences(entity, "IndexName", []interface{}{"c"}, []string{})
	assert.True(t, found)
	assert.NotNil(t, result)
	assert.Equal(t, uint64(3), result.GetID())
	assert.Equal(t, "c", result.Get("Name"))
}

func BenchmarkLoadByIdLocalCacheFastEngine(b *testing.B) {
	entity := &schemaEntity{}
	ref := &schemaEntityRef{}
	registry := &Registry{}
	registry.RegisterEnumStruct("orm.TestEnum", TestEnum)
	registry.RegisterLocalCache(10000)
	engine := PrepareTables(nil, registry, 5, entity, ref)
	e := &schemaEntity{}
	e.Name = "Name"
	e.Uint32 = 1
	e.Int32 = 1
	e.Int8 = 1
	e.Enum = TestEnum.A
	e.RefOne = &schemaEntityRef{}
	engine.Flush(e)
	fastEngine := engine.NewFastEngine()
	_, _ = fastEngine.LoadByID(1, e)
	b.ResetTimer()
	b.ReportAllocs()
	// BenchmarkLoadByIdLocalCache-12    	  906270	      1296 ns/op	     296 B/op	       6 allocs/op
	// BenchmarkLoadByIdLocalCacheFastEngine-12    	 4452970	       274 ns/op	      56 B/op	       2 allocs/op
	for n := 0; n < b.N; n++ {
		_, _ = fastEngine.LoadByID(1, e)
	}
}

func BenchmarkLoadByIdsLocalCacheFastEngine(b *testing.B) {
	entity := &schemaEntity{}
	ref := &schemaEntityRef{}
	registry := &Registry{}
	registry.RegisterEnumStruct("orm.TestEnum", TestEnum)
	registry.RegisterLocalCache(10000)
	engine := PrepareTables(nil, registry, 5, entity, ref)
	e := &schemaEntity{}
	e.Name = "Name"
	e.Uint32 = 1
	e.Int32 = 1
	e.Int8 = 1
	e.Enum = TestEnum.A
	e.RefOne = &schemaEntityRef{}
	engine.Flush(e)
	fastEngine := engine.NewFastEngine()
	_, _ = fastEngine.LoadByID(1, e)
	b.ResetTimer()
	b.ReportAllocs()
	// BenchmarkLoadByIdsLocalCache-12    	  477350	      2232 ns/op	    1280 B/op	      12 allocs/op
	// BenchmarkLoadByIdsLocalCacheFastEngine-12    	 1613864	       703.0 ns/op	     432 B/op	       7 allocs/op
	for n := 0; n < b.N; n++ {
		fastEngine.LoadByIDs([]uint64{1}, e)
	}
}
