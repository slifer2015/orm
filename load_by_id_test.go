package orm

import (
	"testing"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type loadByIDEntity struct {
	ORM             `orm:"localCache;redisCache"`
	ID              uint
	Name            string `orm:"max=100"`
	ReferenceOne    *loadByIDReference
	ReferenceSecond *loadByIDReference
	ReferenceMany   []*loadByIDReference
}

type loadByIDRedisEntity struct {
	ORM `orm:"redisCache"`
	ID  uint
}

type loadByIDNoCacheEntity struct {
	ORM
	ID   uint
	Name string
}

type loadByIDReference struct {
	ORM            `orm:"localCache;redisCache"`
	ID             uint
	Name           string
	ReferenceTwo   *loadByIDSubReference
	ReferenceThree *loadByIDSubReference2
}

type loadByIDSubReference struct {
	ORM  `orm:"localCache;redisCache"`
	ID   uint
	Name string
}

type loadByIDSubReference2 struct {
	ORM          `orm:"localCache"`
	ID           uint
	Name         string
	ReferenceTwo *loadByIDSubReference
}

func TestLoadById(t *testing.T) {
	var entity *loadByIDEntity
	var entityRedis *loadByIDRedisEntity
	var entityNoCache *loadByIDNoCacheEntity
	var reference *loadByIDReference
	var subReference *loadByIDSubReference
	var subReference2 *loadByIDSubReference2
	engine := PrepareTables(t, &Registry{}, 5, entity, entityRedis, entityNoCache, reference, subReference, subReference2)

	e := &loadByIDEntity{Name: "a", ReferenceOne: &loadByIDReference{Name: "r1", ReferenceTwo: &loadByIDSubReference{Name: "s1"}}}
	e.ReferenceSecond = &loadByIDReference{Name: "r11", ReferenceTwo: &loadByIDSubReference{Name: "s1"},
		ReferenceThree: &loadByIDSubReference2{Name: "s11", ReferenceTwo: &loadByIDSubReference{Name: "hello"}}}
	engine.FlushMany(e,
		&loadByIDEntity{Name: "b", ReferenceOne: &loadByIDReference{Name: "r2", ReferenceTwo: &loadByIDSubReference{Name: "s2"}}},
		&loadByIDEntity{Name: "c"}, &loadByIDNoCacheEntity{Name: "a"})

	engine.FlushMany(&loadByIDReference{Name: "rm1", ID: 100}, &loadByIDReference{Name: "rm2", ID: 101}, &loadByIDReference{Name: "rm3", ID: 102})
	engine.FlushMany(&loadByIDEntity{Name: "eMany", ID: 200, ReferenceMany: []*loadByIDReference{{ID: 100}, {ID: 101}, {ID: 102}}})

	entity = &loadByIDEntity{}
	localLogger := memory.New()
	engine.AddQueryLogger(localLogger, apexLog.InfoLevel, QueryLoggerSourceLocalCache)
	found := engine.LoadByID(1, entity, "ReferenceOne/ReferenceTwo",
		"ReferenceSecond/ReferenceTwo", "ReferenceSecond/ReferenceThree/ReferenceTwo")
	assert.True(t, found)
	assert.Len(t, localLogger.Entries, 5)
	assert.True(t, entity.IsLoaded())
	assert.True(t, entity.IsInitialised())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.True(t, entity.ReferenceOne.IsInitialised())
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsInitialised())
	assert.True(t, entity.ReferenceSecond.IsLoaded())
	assert.True(t, entity.ReferenceSecond.IsInitialised())
	assert.True(t, entity.ReferenceSecond.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceTwo.IsInitialised())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.IsInitialised())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsInitialised())

	entity = &loadByIDEntity{}
	found = engine.LoadByIDLazy(1, entity, "ReferenceOne/ReferenceTwo",
		"ReferenceSecond/ReferenceTwo", "ReferenceSecond/ReferenceThree/ReferenceTwo")
	assert.True(t, found)
	assert.True(t, entity.IsLoaded())
	assert.False(t, entity.IsInitialised())
	assert.Equal(t, "", entity.Name)
	assert.Equal(t, "a", entity.GetFieldLazy("Name"))
	entity.Init(engine)
	assert.Equal(t, "a", entity.Name)
	assert.IsType(t, reference, entity.ReferenceOne)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.False(t, entity.ReferenceOne.IsInitialised())
	assert.Equal(t, "r1", entity.ReferenceOne.GetFieldLazy("Name"))
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.IsLoaded())
	assert.False(t, entity.ReferenceSecond.IsInitialised())
	assert.Equal(t, "r11", entity.ReferenceSecond.GetFieldLazy("Name"))
	assert.True(t, entity.ReferenceSecond.ReferenceTwo.IsLoaded())
	assert.False(t, entity.ReferenceSecond.ReferenceTwo.IsInitialised())
	assert.Equal(t, "s1", entity.ReferenceSecond.ReferenceTwo.GetFieldLazy("Name"))
	assert.True(t, entity.ReferenceSecond.ReferenceThree.IsLoaded())
	assert.False(t, entity.ReferenceSecond.ReferenceThree.IsInitialised())
	assert.Equal(t, "s11", entity.ReferenceSecond.ReferenceThree.GetFieldLazy("Name"))
	assert.True(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLoaded())
	assert.False(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsInitialised())
	assert.Equal(t, "hello", entity.ReferenceSecond.ReferenceThree.ReferenceTwo.GetFieldLazy("Name"))

	entity = &loadByIDEntity{}
	found = engine.LoadByID(1, entity, "ReferenceOne/ReferenceTwo")
	assert.True(t, found)
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())

	entity = &loadByIDEntity{ID: 1}
	engine.Load(entity, "ReferenceOne/ReferenceTwo")
	assert.Equal(t, "a", entity.Name)
	assert.True(t, entity.IsInitialised())
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.IsInitialised())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())

	entity = &loadByIDEntity{ID: 1}
	engine.LoadLazy(entity, "ReferenceOne/ReferenceTwo")
	assert.Equal(t, "", entity.Name)
	assert.Equal(t, "a", entity.GetFieldLazy("Name"))
	assert.False(t, entity.IsInitialised())
	assert.Equal(t, "r1", entity.ReferenceOne.GetFieldLazy("Name"))
	assert.False(t, entity.ReferenceOne.IsInitialised())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.GetFieldLazy("Name"))
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	assert.False(t, entity.ReferenceOne.ReferenceTwo.IsInitialised())

	entityNoCache = &loadByIDNoCacheEntity{}
	found = engine.LoadByID(1, entityNoCache, "*")
	assert.True(t, found)
	assert.Equal(t, uint(1), entityNoCache.ID)
	assert.Equal(t, "a", entityNoCache.Name)

	found = engine.LoadByID(100, entity, "*")
	assert.False(t, found)
	found = engine.LoadByID(100, entity, "*")
	assert.False(t, found)
	entityRedis = &loadByIDRedisEntity{}
	found = engine.LoadByID(100, entityRedis, "*")
	assert.False(t, found)
	found = engine.LoadByID(100, entityRedis, "*")
	assert.False(t, found)

	entity = &loadByIDEntity{}
	found = engine.LoadByID(200, entity, "ReferenceMany")
	assert.True(t, found)
	assert.Len(t, entity.ReferenceMany, 3)
	assert.Equal(t, uint(100), entity.ReferenceMany[0].ID)
	assert.Equal(t, uint(101), entity.ReferenceMany[1].ID)
	assert.Equal(t, uint(102), entity.ReferenceMany[2].ID)
	assert.Equal(t, "rm1", entity.ReferenceMany[0].Name)
	assert.Equal(t, "rm2", entity.ReferenceMany[1].Name)
	assert.Equal(t, "rm3", entity.ReferenceMany[2].Name)
	assert.True(t, entity.ReferenceMany[0].IsLoaded())
	assert.True(t, entity.ReferenceMany[1].IsLoaded())
	assert.True(t, entity.ReferenceMany[2].IsLoaded())

	entity = &loadByIDEntity{}
	found = engine.LoadByIDLazy(200, entity, "ReferenceMany")
	assert.True(t, found)
	assert.Len(t, entity.ReferenceMany, 3)
	assert.Equal(t, uint(100), entity.ReferenceMany[0].ID)
	assert.Equal(t, uint(101), entity.ReferenceMany[1].ID)
	assert.Equal(t, uint(102), entity.ReferenceMany[2].ID)
	assert.Equal(t, "", entity.ReferenceMany[0].Name)
	assert.Equal(t, "", entity.ReferenceMany[1].Name)
	assert.Equal(t, "", entity.ReferenceMany[2].Name)
	assert.False(t, entity.ReferenceMany[0].IsInitialised())
	assert.False(t, entity.ReferenceMany[1].IsInitialised())
	assert.False(t, entity.ReferenceMany[2].IsInitialised())
	assert.True(t, entity.ReferenceMany[0].IsLoaded())
	assert.True(t, entity.ReferenceMany[1].IsLoaded())
	assert.True(t, entity.ReferenceMany[2].IsLoaded())
	assert.Equal(t, "rm1", entity.ReferenceMany[0].GetFieldLazy("Name"))
	assert.Equal(t, "rm2", entity.ReferenceMany[1].GetFieldLazy("Name"))
	assert.Equal(t, "rm3", entity.ReferenceMany[2].GetFieldLazy("Name"))
	entity.ReferenceMany[0].Init(engine)
	assert.Equal(t, "rm1", entity.ReferenceMany[0].Name)
	entity.ReferenceMany[0].Init(engine)
	assert.True(t, entity.ReferenceMany[0].IsInitialised())

	engine = PrepareTables(t, &Registry{}, 5)
	entity = &loadByIDEntity{}
	assert.PanicsWithError(t, "entity 'orm.loadByIDEntity' is not registered", func() {
		engine.LoadByID(1, entity)
	})
}

// BenchmarkLoadByIDdLocalCache-12    	  913500	      1288 ns/op	     248 B/op	       5 allocs/op
func BenchmarkLoadByIDdLocalCache(b *testing.B) {
	benchmarkLoadByIDLocalCache(b, false)
}

// BenchmarkLoadByIDLocalCacheLazy-12    	 5608262	       206.7 ns/op	       8 B/op	       1 allocs/op
func BenchmarkLoadByIDLocalCacheLazy(b *testing.B) {
	benchmarkLoadByIDLocalCache(b, true)
}

func benchmarkLoadByIDLocalCache(b *testing.B, lazy bool) {
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
	_ = engine.LoadByID(1, e)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		if lazy {
			_ = engine.LoadByIDLazy(1, e)
		} else {
			_ = engine.LoadByID(1, e)
		}
	}
}
