package orm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

func searchIDsWithCount(skipFakeDelete bool, engine *Engine, where *Where, pager *Pager, entityType reflect.Type) (results []uint64, totalRows int) {
	return searchIDs(skipFakeDelete, engine, where, pager, true, entityType)
}

func prepareScan(schema *tableSchema) (pointers []interface{}) {
	count := len(schema.columnNames)
	pointers = make([]interface{}, count)
	prepareScanForFields(schema.fields, 0, pointers)
	return pointers
}

func prepareScanForFields(fields *tableFields, start int, pointers []interface{}) int {
	for i := 0; i < len(fields.uintegers); i++ {
		v := uint64(0)
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.uintegersNullable); i++ {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.integers); i++ {
		v := int64(0)
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.integersNullable); i++ {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.strings); i++ {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.sliceStrings); i++ {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.bytes); i++ {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	if fields.fakeDelete > 0 {
		v := uint64(0)
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.booleans); i++ {
		v := false
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.booleansNullable); i++ {
		v := sql.NullBool{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.floats); i++ {
		v := float64(0)
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.floatsNullable); i++ {
		v := sql.NullFloat64{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.timesNullable); i++ {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.times); i++ {
		v := ""
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.jsons); i++ {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.refs); i++ {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.refsMany); i++ {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, subFields := range fields.structs {
		start = prepareScanForFields(subFields, start, pointers)
	}
	return start
}

func convertScan(fields *tableFields, start int, pointers []interface{}) int {
	for i := 0; i < len(fields.uintegers); i++ {
		pointers[start] = *pointers[start].(*uint64)
		start++
	}
	for i := 0; i < len(fields.uintegersNullable); i++ {
		v := pointers[start].(*sql.NullInt64)
		if v.Valid {
			pointers[start] = uint64(v.Int64)
		} else {
			pointers[start] = nil
		}
		start++
	}
	for i := 0; i < len(fields.integers); i++ {
		pointers[start] = *pointers[start].(*int64)
		start++
	}
	for i := 0; i < len(fields.integersNullable); i++ {
		v := pointers[start].(*sql.NullInt64)
		if v.Valid {
			pointers[start] = v.Int64
		} else {
			pointers[start] = nil
		}
		start++
	}
	for i := 0; i < len(fields.strings); i++ {
		v := pointers[start].(*sql.NullString)
		if v.Valid {
			pointers[start] = v.String
		} else {
			pointers[start] = nil
		}
		start++
	}
	for i := 0; i < len(fields.sliceStrings); i++ {
		v := pointers[start].(*sql.NullString)
		if v.Valid {
			pointers[start] = v.String
		} else {
			pointers[start] = nil
		}
		start++
	}
	for i := 0; i < len(fields.bytes); i++ {
		v := pointers[start].(*sql.NullString)
		if v.Valid {
			pointers[start] = v.String
		} else {
			pointers[start] = nil
		}
		start++
	}
	if fields.fakeDelete > 0 {
		pointers[start] = *pointers[start].(*uint64)
		start++
	}
	for i := 0; i < len(fields.booleans); i++ {
		pointers[start] = *pointers[start].(*bool)
		start++
	}
	for i := 0; i < len(fields.booleansNullable); i++ {
		v := pointers[start].(*sql.NullBool)
		if v.Valid {
			pointers[start] = v.Bool
		} else {
			pointers[start] = nil
		}
		start++
	}
	for i := 0; i < len(fields.floats); i++ {
		pointers[start] = *pointers[start].(*float64)
		start++
	}
	for i := 0; i < len(fields.floatsNullable); i++ {
		v := pointers[start].(*sql.NullFloat64)
		if v.Valid {
			pointers[start] = v.Float64
		} else {
			pointers[start] = nil
		}
		start++
	}
	for i := 0; i < len(fields.timesNullable); i++ {
		v := pointers[start].(*sql.NullString)
		if v.Valid {
			pointers[start] = v.String
		} else {
			pointers[start] = nil
		}
		start++
	}
	for i := 0; i < len(fields.times); i++ {
		pointers[start] = *pointers[start].(*string)
		start++
	}
	for i := 0; i < len(fields.jsons); i++ {
		v := pointers[start].(*sql.NullString)
		if v.Valid {
			pointers[start] = v.String
		} else {
			pointers[start] = nil
		}
		start++
	}
	for i := 0; i < len(fields.refs); i++ {
		v := pointers[start].(*sql.NullInt64)
		if v.Valid {
			pointers[start] = uint64(v.Int64)
		} else {
			pointers[start] = nil
		}
		start++
	}
	for i := 0; i < len(fields.refsMany); i++ {
		v := pointers[start].(*sql.NullString)
		if v.Valid {
			pointers[start] = v.String
		} else {
			pointers[start] = nil
		}
		start++
	}
	for _, subFields := range fields.structs {
		start = convertScan(subFields, start, pointers)
	}
	return start
}

func searchRow(skipFakeDelete bool, engine *Engine, where *Where, entity Entity, lazy bool, references []string) (bool, *tableSchema, []interface{}) {
	orm := initIfNeeded(engine.registry, entity)
	schema := orm.tableSchema
	whereQuery := where.String()
	if skipFakeDelete && schema.hasFakeDelete {
		whereQuery = "`FakeDelete` = 0 AND " + whereQuery
	}
	/* #nosec */
	query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE " + whereQuery + " LIMIT 1"

	pool := schema.GetMysql(engine)
	results, def := pool.Query(query, where.GetParameters()...)
	defer def()
	if !results.Next() {
		return false, schema, nil
	}
	pointers := prepareScan(schema)
	results.Scan(pointers...)
	def()
	convertScan(schema.fields, 0, pointers)
	id := pointers[0].(uint64)
	fillFromDBRow(id, engine, pointers, entity, true, lazy)
	if len(references) > 0 {
		warmUpReferences(engine, schema, entity.getORM().value, references, false, lazy)
	}
	return true, schema, pointers
}

func search(skipFakeDelete bool, engine *Engine, where *Where, pager *Pager, withCount, lazy, checkIsSlice bool, entities reflect.Value, references ...string) (totalRows int) {
	if pager == nil {
		pager = NewPager(1, 50000)
	}
	entities.SetLen(0)
	entityType, has, name := getEntityTypeForSlice(engine.registry, entities.Type(), checkIsSlice)
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}
	schema := getTableSchema(engine.registry, entityType)
	whereQuery := where.String()
	if skipFakeDelete && schema.hasFakeDelete {
		whereQuery = "`FakeDelete` = 0 AND " + whereQuery
	}
	/* #nosec */
	pageStart := strconv.Itoa((pager.CurrentPage - 1) * pager.PageSize)
	pageEnd := strconv.Itoa(pager.PageSize)
	query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE " + whereQuery + " LIMIT " + pageStart + "," + pageEnd
	pool := schema.GetMysql(engine)
	results, def := pool.Query(query, where.GetParameters()...)
	defer def()

	valOrigin := entities
	val := valOrigin
	i := 0
	for results.Next() {
		pointers := prepareScan(schema)
		results.Scan(pointers...)
		convertScan(schema.fields, 0, pointers)
		value := reflect.New(entityType)
		id := pointers[0].(uint64)
		fillFromDBRow(id, engine, pointers, value.Interface().(Entity), true, lazy)
		val = reflect.Append(val, value)
		i++
	}
	def()
	totalRows = getTotalRows(engine, withCount, pager, where, schema, i)
	if len(references) > 0 && i > 0 {
		warmUpReferences(engine, schema, val, references, true, lazy)
	}
	valOrigin.Set(val)
	return totalRows
}

func searchOne(skipFakeDelete bool, engine *Engine, where *Where, entity Entity, lazy bool, references []string) (bool, *tableSchema, []interface{}) {
	return searchRow(skipFakeDelete, engine, where, entity, lazy, references)
}

func searchIDs(skipFakeDelete bool, engine *Engine, where *Where, pager *Pager, withCount bool, entityType reflect.Type) (ids []uint64, total int) {
	if pager == nil {
		pager = NewPager(1, 50000)
	}
	schema := getTableSchema(engine.registry, entityType)
	whereQuery := where.String()
	if skipFakeDelete && schema.hasFakeDelete {
		/* #nosec */
		whereQuery = "`FakeDelete` = 0 AND " + whereQuery
	}
	/* #nosec */
	startPage := strconv.Itoa((pager.CurrentPage - 1) * pager.PageSize)
	endPage := strconv.Itoa(pager.PageSize)
	query := "SELECT `ID` FROM `" + schema.tableName + "` WHERE " + whereQuery + " LIMIT " + startPage + "," + endPage
	pool := schema.GetMysql(engine)
	results, def := pool.Query(query, where.GetParameters()...)
	defer def()
	result := make([]uint64, 0)
	for results.Next() {
		var row uint64
		results.Scan(&row)
		result = append(result, row)
	}
	def()
	totalRows := getTotalRows(engine, withCount, pager, where, schema, len(result))
	return result, totalRows
}

func getTotalRows(engine *Engine, withCount bool, pager *Pager, where *Where, schema *tableSchema, foundRows int) int {
	totalRows := 0
	if withCount {
		totalRows = foundRows
		if totalRows == pager.GetPageSize() || (foundRows == 0 && pager.CurrentPage > 1) {
			/* #nosec */
			query := "SELECT count(1) FROM `" + schema.tableName + "` WHERE " + where.String()
			var foundTotal string
			pool := schema.GetMysql(engine)
			pool.QueryRow(NewWhere(query, where.GetParameters()...), &foundTotal)
			totalRows, _ = strconv.Atoi(foundTotal)
		} else {
			totalRows += (pager.GetCurrentPage() - 1) * pager.GetPageSize()
		}
	}
	return totalRows
}

func fillFromDBRow(id uint64, engine *Engine, data []interface{}, entity Entity, fillDataLoader bool, lazy bool) {
	orm := initIfNeeded(engine.registry, entity)
	elem := orm.elem
	orm.idElem.SetUint(id)
	data[0] = id
	if !lazy {
		_ = fillStruct(engine.registry, 0, data, orm.tableSchema.fields, orm, elem)
	}
	orm.inDB = true
	orm.loaded = true
	orm.lazy = lazy
	orm.dBData = data
	if !fillDataLoader {
		return
	}
	schema := entity.getORM().tableSchema
	if !schema.hasLocalCache && engine.dataLoader != nil {
		engine.dataLoader.Prime(schema, id, data)
	}
}

func fillStruct(registry *validatedRegistry, index uint16, data []interface{}, fields *tableFields, orm *ORM, value reflect.Value) uint16 {
	for _, i := range fields.uintegers {
		value.Field(i).SetUint(data[index].(uint64))
		index++
	}
	for _, i := range fields.uintegersNullable {
		field := value.Field(i)
		if data[index] == nil {
			if !field.IsZero() {
				field.Set(reflect.Zero(field.Type()))
			}
		} else {
			val := data[index].(uint64)
			switch field.Type().String() {
			case "*uint":
				v := uint(val)
				field.Set(reflect.ValueOf(&v))
			case "*uint8":
				v := uint8(val)
				field.Set(reflect.ValueOf(&v))
			case "*uint16":
				v := uint16(val)
				field.Set(reflect.ValueOf(&v))
			case "*uint32":
				v := uint32(val)
				field.Set(reflect.ValueOf(&v))
			default:
				field.Set(reflect.ValueOf(&val))
			}
		}
		index++
	}
	for _, i := range fields.integers {
		value.Field(i).SetInt(data[index].(int64))
		index++
	}
	for _, i := range fields.integersNullable {
		field := value.Field(i)
		if data[index] == nil {
			if !field.IsZero() {
				field.Set(reflect.Zero(field.Type()))
			}
		} else {
			val := data[index].(int64)
			switch field.Type().String() {
			case "*int":
				v := int(val)
				field.Set(reflect.ValueOf(&v))
			case "*int8":
				v := int8(val)
				field.Set(reflect.ValueOf(&v))
			case "*int16":
				v := int16(val)
				field.Set(reflect.ValueOf(&v))
			case "*int32":
				v := int32(val)
				field.Set(reflect.ValueOf(&v))
			default:
				field.Set(reflect.ValueOf(&val))
			}
		}
		index++
	}
	for _, i := range fields.strings {
		field := value.Field(i)
		if data[index] == nil {
			field.SetString("")
		} else {
			field.SetString(data[index].(string))
		}
		index++
	}
	for _, i := range fields.sliceStrings {
		field := value.Field(i)
		if data[index] != nil {
			if data[index] == "" {
				if !field.IsZero() {
					field.Set(reflect.Zero(field.Type()))
				}
			} else {
				var values = strings.Split(data[index].(string), ",")
				field.Set(reflect.ValueOf(values))
			}
		} else if !field.IsZero() {
			field.Set(reflect.Zero(field.Type()))
		}
		index++
	}
	for _, i := range fields.bytes {
		bytes := data[index]
		field := value.Field(i)
		if bytes != nil {
			field.SetBytes([]byte(bytes.(string)))
		} else if !field.IsZero() {
			field.Set(reflect.Zero(field.Type()))
		}
		index++
	}
	if fields.fakeDelete > 0 {
		value.Field(fields.fakeDelete).SetBool(data[index].(uint64) > 0)
		index++
	}
	for _, i := range fields.booleans {
		value.Field(i).SetBool(data[index].(bool))
		index++
	}
	for _, i := range fields.booleansNullable {
		field := value.Field(i)
		if data[index] == nil {
			if !field.IsZero() {
				field.Set(reflect.Zero(field.Type()))
			}
		} else {
			v := data[index].(bool)
			field.Set(reflect.ValueOf(&v))
		}
		index++
	}
	for _, i := range fields.floats {
		value.Field(i).SetFloat(data[index].(float64))
		index++
	}
	for _, i := range fields.floatsNullable {
		field := value.Field(i)
		if data[index] == nil {
			field.Set(reflect.Zero(field.Type()))
		} else {
			val, _ := data[index].(float64)
			switch field.Type().String() {
			case "*float32":
				v := float32(val)
				field.Set(reflect.ValueOf(&v))
			default:
				field.Set(reflect.ValueOf(&val))
			}
		}
		index++
	}
	for _, i := range fields.timesNullable {
		field := value.Field(i)
		if data[index] == nil {
			if !field.IsZero() {
				field.Set(reflect.Zero(field.Type()))
			}
		} else {
			v := data[index].(string)
			layout := "2006-01-02"
			if len(v) == 19 {
				layout += " 15:04:05"
			}
			value, _ := time.ParseInLocation(layout, v, time.Local)
			field.Set(reflect.ValueOf(&value))
		}
		index++
	}
	for _, i := range fields.times {
		field := value.Field(i)
		layout := "2006-01-02"
		v := data[index].(string)
		if len(v) == 19 {
			if v == "0001-01-01 00:00:00" && field.IsZero() {
				index++
				continue
			}
			layout += " 15:04:05"
		} else if v == "0001-01-01" && field.IsZero() {
			index++
			continue
		}
		val, _ := time.ParseInLocation(layout, v, time.Local)
		field.Set(reflect.ValueOf(val))
		index++
	}
	for _, i := range fields.jsons {
		field := value.Field(i)
		if data[index] != nil {
			f := reflect.New(field.Type()).Interface()
			_ = jsoniter.ConfigFastest.UnmarshalFromString(data[index].(string), f)
			field.Set(reflect.ValueOf(f).Elem())
		} else if !field.IsZero() {
			field.Set(reflect.Zero(field.Type()))
		}
		index++
	}
	for k, i := range fields.refs {
		field := value.Field(i)
		integer := uint64(0)
		if data[index] != nil {
			integer = data[index].(uint64)
		}
		refType := fields.refsTypes[k]
		if integer > 0 {
			if orm.lazy && !field.IsZero() {
				index++
				continue
			}
			n := reflect.New(refType.Elem())
			orm := initIfNeeded(registry, n.Interface().(Entity))
			orm.idElem.SetUint(integer)
			orm.inDB = true
			field.Set(n)
		} else if !field.IsZero() {
			field.Set(reflect.Zero(refType))
		}
		index++
	}
	for k, i := range fields.refsMany {
		field := value.Field(i)
		var f []uint64
		length := 0
		if data[index] != nil {
			f = make([]uint64, 0)
			_ = jsoniter.ConfigFastest.UnmarshalFromString(data[index].(string), &f)
			length = len(f)
		}
		refType := fields.refsManyTypes[k]
		slice := reflect.MakeSlice(reflect.SliceOf(refType), length, length)
		if f != nil {
			if orm.lazy && !field.IsZero() {
				index++
				continue
			}
			for i, id := range f {
				n := reflect.New(refType.Elem())
				orm := initIfNeeded(registry, n.Interface().(Entity))
				orm.idElem.SetUint(id)
				orm.inDB = true
				slice.Index(i).Set(n)
			}
			field.Set(slice)
		} else if !field.IsZero() {
			field.Set(reflect.Zero(slice.Type()))
		}
		index++
	}
	for i, subFields := range fields.structs {
		field := value.Field(i)
		newVal := reflect.New(field.Type())
		value := newVal.Elem()
		newIndex := fillStruct(registry, index, data, subFields, orm, value)
		field.Set(value)
		index = newIndex
	}
	return index
}

func getEntityTypeForSlice(registry *validatedRegistry, sliceType reflect.Type, checkIsSlice bool) (reflect.Type, bool, string) {
	name := sliceType.String()
	if name[0] == 42 {
		name = name[1:]
	}
	if name[0] == 91 {
		name = name[3:]
	} else if checkIsSlice {
		panic(fmt.Errorf("interface %s is no slice of orm.Entity", sliceType.String()))
	}
	e, has := registry.entities[name]
	return e, has, name
}
