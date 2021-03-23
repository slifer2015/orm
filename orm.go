package orm

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	jsoniter "github.com/json-iterator/go"

	"github.com/pkg/errors"
)

type Entity interface {
	getORM() *ORM
	GetID() uint64
	markToDelete()
	forceMarkToDelete()
	Loaded() bool
	IsDirty() bool
	GetDirtyBind() (bind Bind, has bool)
	SetOnDuplicateKeyUpdate(bind Bind)
	SetEntityLogMeta(key string, value interface{})
	SetField(field string, value interface{}) error
}

type ORM struct {
	dBData               []interface{}
	tableSchema          *tableSchema
	onDuplicateKeyUpdate map[string]interface{}
	initialised          bool
	loaded               bool
	inDB                 bool
	delete               bool
	fakeDelete           bool
	value                reflect.Value
	elem                 reflect.Value
	idElem               reflect.Value
	logMeta              map[string]interface{}
}

func (orm *ORM) getORM() *ORM {
	return orm
}

func (orm *ORM) GetID() uint64 {
	if !orm.idElem.IsValid() {
		return 0
	}
	return orm.idElem.Uint()
}

func (orm *ORM) initDBData() {
	if orm.dBData == nil {
		orm.dBData = make([]interface{}, len(orm.tableSchema.columnNames))
	}
}

func (orm *ORM) markToDelete() {
	orm.fakeDelete = true
}

func (orm *ORM) forceMarkToDelete() {
	orm.delete = true
}

func (orm *ORM) Loaded() bool {
	return orm.loaded
}

func (orm *ORM) SetOnDuplicateKeyUpdate(bind Bind) {
	orm.onDuplicateKeyUpdate = bind
}

func (orm *ORM) SetEntityLogMeta(key string, value interface{}) {
	if orm.logMeta == nil {
		orm.logMeta = make(map[string]interface{})
	}
	orm.logMeta[key] = value
}

func (orm *ORM) IsDirty() bool {
	if !orm.loaded {
		return true
	}
	_, is := orm.GetDirtyBind()
	return is
}

func (orm *ORM) GetDirtyBind() (bind Bind, has bool) {
	bind, _, has = orm.getDirtyBind()
	return bind, has
}

func (orm *ORM) getDirtyBind() (bind Bind, updateBind map[string]string, has bool) {
	if orm.delete {
		return nil, nil, true
	}
	if orm.fakeDelete {
		if orm.tableSchema.hasFakeDelete {
			orm.elem.FieldByName("FakeDelete").SetBool(true)
		} else {
			orm.delete = true
			return nil, nil, true
		}
	}
	id := orm.GetID()
	t := orm.elem.Type()
	orm.initDBData()
	bind = make(Bind)
	if orm.inDB && !orm.delete {
		updateBind = make(map[string]string)
	}
	orm.fillBind(id, bind, updateBind, orm.tableSchema, t, orm.elem, orm.dBData, "")
	has = id == 0 || len(bind) > 0
	return bind, updateBind, has
}

func (orm *ORM) SetField(field string, value interface{}) error {
	asString, isString := value.(string)
	if isString {
		asString = strings.ToLower(asString)
		if asString == "nil" || asString == "null" {
			value = nil
		}
	}
	if !orm.elem.IsValid() {
		return errors.New("entity is not loaded")
	}
	f := orm.elem.FieldByName(field)
	if !f.IsValid() {
		return fmt.Errorf("field %s not found", field)
	}
	if !f.CanSet() {
		return fmt.Errorf("field %s is not public", field)
	}
	typeName := f.Type().String()
	switch typeName {
	case "uint",
		"uint8",
		"uint16",
		"uint32",
		"uint64":
		val := uint64(0)
		if value != nil {
			parsed, err := strconv.ParseUint(fmt.Sprintf("%v", value), 10, 64)
			if err != nil {
				return fmt.Errorf("%s value %v not valid", field, value)
			}
			val = parsed
		}
		f.SetUint(val)
	case "*uint",
		"*uint8",
		"*uint16",
		"*uint32",
		"*uint64":
		if value != nil {
			val := uint64(0)
			parsed, err := strconv.ParseUint(fmt.Sprintf("%v", reflect.Indirect(reflect.ValueOf(value)).Interface()), 10, 64)
			if err != nil {
				return fmt.Errorf("%s value %v not valid", field, value)
			}
			val = parsed
			switch typeName {
			case "*uint":
				v := uint(val)
				f.Set(reflect.ValueOf(&v))
			case "*uint8":
				v := uint8(val)
				f.Set(reflect.ValueOf(&v))
			case "*uint16":
				v := uint16(val)
				f.Set(reflect.ValueOf(&v))
			case "*uint32":
				v := uint32(val)
				f.Set(reflect.ValueOf(&v))
			default:
				f.Set(reflect.ValueOf(&val))
			}
		} else {
			f.Set(reflect.Zero(f.Type()))
		}
	case "int",
		"int8",
		"int16",
		"int32",
		"int64":
		val := int64(0)
		if value != nil {
			parsed, err := strconv.ParseInt(fmt.Sprintf("%v", value), 10, 64)
			if err != nil {
				return fmt.Errorf("%s value %v not valid", field, value)
			}
			val = parsed
		}
		f.SetInt(val)
	case "*int",
		"*int8",
		"*int16",
		"*int32",
		"*int64":
		if value != nil {
			val := int64(0)
			parsed, err := strconv.ParseInt(fmt.Sprintf("%v", reflect.Indirect(reflect.ValueOf(value)).Interface()), 10, 64)
			if err != nil {
				return fmt.Errorf("%s value %v not valid", field, value)
			}
			val = parsed
			switch typeName {
			case "*int":
				v := int(val)
				f.Set(reflect.ValueOf(&v))
			case "*int8":
				v := int8(val)
				f.Set(reflect.ValueOf(&v))
			case "*int16":
				v := int16(val)
				f.Set(reflect.ValueOf(&v))
			case "*int32":
				v := int32(val)
				f.Set(reflect.ValueOf(&v))
			default:
				f.Set(reflect.ValueOf(&val))
			}
		} else {
			f.Set(reflect.Zero(f.Type()))
		}
	case "string":
		if value == nil {
			f.SetString("")
		} else {
			f.SetString(fmt.Sprintf("%v", value))
		}
	case "[]string":
		_, ok := value.([]string)
		if !ok {
			return fmt.Errorf("%s value %v not valid", field, value)
		}
		f.Set(reflect.ValueOf(value))
	case "[]uint8":
		_, ok := value.([]uint8)
		if !ok {
			return fmt.Errorf("%s value %v not valid", field, value)
		}
		f.Set(reflect.ValueOf(value))
	case "bool":
		val := false
		asString := strings.ToLower(fmt.Sprintf("%v", value))
		if asString == "true" || asString == "1" {
			val = true
		}
		f.SetBool(val)
	case "*bool":
		if value == nil {
			f.Set(reflect.Zero(f.Type()))
		} else {
			val := false
			asString := strings.ToLower(fmt.Sprintf("%v", reflect.Indirect(reflect.ValueOf(value)).Interface()))
			if asString == "true" || asString == "1" {
				val = true
			}
			f.Set(reflect.ValueOf(&val))
		}
	case "float32",
		"float64":
		val := float64(0)
		if value != nil {
			valueString := fmt.Sprintf("%v", value)
			valueString = strings.ReplaceAll(valueString, ",", ".")
			parsed, err := strconv.ParseFloat(valueString, 64)
			if err != nil {
				return fmt.Errorf("%s value %v is not valid", field, value)
			}
			val = parsed
		}
		f.SetFloat(val)
	case "*float32",
		"*float64":
		if value == nil {
			f.Set(reflect.Zero(f.Type()))
		} else {
			val := float64(0)
			valueString := fmt.Sprintf("%v", reflect.Indirect(reflect.ValueOf(value)).Interface())
			valueString = strings.ReplaceAll(valueString, ",", ".")
			parsed, err := strconv.ParseFloat(valueString, 64)
			if err != nil {
				return fmt.Errorf("%s value %v is not valid", field, value)
			}
			val = parsed
			f.Set(reflect.ValueOf(&val))
		}
	case "*time.Time":
		if value == nil {
			f.Set(reflect.Zero(f.Type()))
		} else {
			_, ok := value.(*time.Time)
			if !ok {
				return fmt.Errorf("%s value %v is not valid", field, value)
			}
			f.Set(reflect.ValueOf(value))
		}
	case "time.Time":
		_, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("%s value %v is not valid", field, value)
		}
		f.Set(reflect.ValueOf(value))
	default:
		k := f.Type().Kind().String()
		if k == "struct" || k == "slice" {
			f.Set(reflect.ValueOf(value))
		} else if k == "ptr" {
			modelType := reflect.TypeOf((*Entity)(nil)).Elem()
			if f.Type().Implements(modelType) {
				if value == nil || (isString && (value == "" || value == "0")) {
					f.Set(reflect.Zero(f.Type()))
				} else {
					asEntity, ok := value.(Entity)
					if ok {
						f.Set(reflect.ValueOf(asEntity))
					} else {
						id, err := strconv.ParseUint(fmt.Sprintf("%v", value), 10, 64)
						if err != nil {
							return fmt.Errorf("%s value %v is not valid", field, value)
						}
						if id == 0 {
							f.Set(reflect.Zero(f.Type()))
						} else {
							val := reflect.New(f.Type().Elem())
							val.Elem().FieldByName("ID").SetUint(id)
							f.Set(val)
						}
					}
				}
			} else {
				return fmt.Errorf("field %s is not supported", field)
			}
		} else {
			return fmt.Errorf("field %s is not supported", field)
		}
	}
	return nil
}

func (orm *ORM) fillBind(id uint64, bind Bind, updateBind map[string]string, tableSchema *tableSchema,
	t reflect.Type, value reflect.Value,
	oldData []interface{}, prefix string) {
	var hasOld = orm.inDB
	hasUpdate := updateBind != nil
	// TODO remove t.Field(i), use cached
	for i := 0; i < t.NumField(); i++ {
		fieldType := t.Field(i)
		name := prefix + fieldType.Name
		if prefix == "" && i <= 1 {
			continue
		}
		var old interface{}
		if hasOld {
			old = oldData[tableSchema.columnMapping[name]]
		}
		field := value.Field(i)
		attributes := tableSchema.tags[name]
		_, has := attributes["ignore"]
		if has {
			continue
		}
		fieldTypeString := field.Type().String()
		required, hasRequired := attributes["required"]
		isRequired := hasRequired && required == "true"
		switch fieldTypeString {
		case "uint", "uint8", "uint16", "uint32", "uint64":
			val := field.Uint()
			if attributes["year"] == "true" {
				if hasOld && old == val {
					continue
				}
				bind[name] = val
				continue
			}
			if hasOld && old == val {
				continue
			}
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatUint(val, 10)
			}
		case "*uint", "*uint8", "*uint16", "*uint32", "*uint64":
			if attributes["year"] == "true" {
				isNil := field.IsZero()
				if isNil {
					if hasOld && old == nil {
						continue
					}
					bind[name] = nil
					if hasUpdate {
						updateBind[name] = "NULL"
					}
					continue
				}
				val := field.Elem().Uint()
				if hasOld && old == val {
					continue
				}
				bind[name] = val
				if hasUpdate {
					updateBind[name] = strconv.FormatUint(val, 10)
				}
				continue
			}
			isNil := field.IsZero()
			if isNil {
				if hasOld && old == nil {
					continue
				}
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
				continue
			}
			val := field.Elem().Uint()
			if hasOld && old == val {
				continue
			}
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatUint(val, 10)
			}
		case "int", "int8", "int16", "int32", "int64":
			val := field.Int()
			if hasOld && old == val {
				continue
			}
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatInt(val, 10)
			}
		case "*int", "*int8", "*int16", "*int32", "*int64":
			isNil := field.IsZero()
			if isNil {
				if hasOld && old == nil {
					continue
				}
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
				continue
			}
			val := field.Elem().Int()
			if hasOld && old == val {
				continue
			}
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatInt(val, 10)
			}
		case "string":
			value := field.String()
			if hasOld && (old == value || (old == nil && value == "")) {
				continue
			}
			if isRequired || value != "" {
				bind[name] = value
				if hasUpdate {
					updateBind[name] = orm.escapeSQLParam(value)
				}
			} else if value == "" {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			}
		case "[]uint8":
			value := field.Bytes()
			valueAsString := string(value)
			if hasOld && ((old != nil && old.(string) == valueAsString) || (old == nil && valueAsString == "")) {
				continue
			}
			if valueAsString == "" {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			} else {
				bind[name] = valueAsString
				if hasUpdate {
					updateBind[name] = orm.escapeSQLParam(valueAsString)
				}
			}
		case "bool":
			if name == "FakeDelete" {
				value := uint64(0)
				if field.Bool() {
					value = id
				}
				if hasOld && old == value {
					continue
				}
				bind[name] = value
				if hasUpdate {
					updateBind[name] = strconv.FormatUint(value, 10)
				}
				continue
			}
			value := field.Bool()
			if hasOld && old == value {
				continue
			}
			bind[name] = value
			if hasUpdate {
				if value {
					updateBind[name] = "1"
				} else {
					updateBind[name] = "0"
				}
			}
		case "*bool":
			if field.IsZero() {
				if hasOld && old == nil {
					continue
				}
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
				continue
			}
			value := field.Elem().Bool()
			if hasOld && old == value {
				continue
			}
			bind[name] = value
			if hasUpdate {
				if value {
					updateBind[name] = "1"
				} else {
					updateBind[name] = "0"
				}
			}
		case "float32", "float64":
			val := field.Float()
			precision := 8
			if field.Type().String() == "float64" {
				precision = 16
			}
			fieldAttributes := tableSchema.tags[name]
			precisionAttribute, has := fieldAttributes["precision"]
			if has {
				userPrecision, _ := strconv.Atoi(precisionAttribute)
				precision = userPrecision
			}
			decimal, has := attributes["decimal"]
			if has {
				decimalArgs := strings.Split(decimal, ",")
				size, _ := strconv.ParseFloat(decimalArgs[1], 64)
				sizeNumber := math.Pow(10, size)
				val = math.Round(val*sizeNumber) / sizeNumber
				if hasOld {
					valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
					if val == valOld {
						continue
					}
				}
			} else {
				sizeNumber := math.Pow(10, float64(precision))
				val = math.Round(val*sizeNumber) / sizeNumber
				if hasOld {
					valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
					if valOld == val {
						continue
					}
				}
			}
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
			}
		case "*float32", "*float64":
			var val float64
			isZero := field.IsZero()
			if !isZero {
				val = field.Elem().Float()
			}
			precision := 5
			if field.Type().String() == "*float64" {
				precision = 10
			}
			fieldAttributes := tableSchema.tags[name]
			precisionAttribute, has := fieldAttributes["precision"]
			if has {
				userPrecision, _ := strconv.Atoi(precisionAttribute)
				precision = userPrecision
			}
			decimal, has := attributes["decimal"]
			if has {
				if isZero {
					if hasOld && old == nil {
						continue
					}
					bind[name] = nil
					if hasUpdate {
						updateBind[name] = "NULL"
					}
					continue
				}
				decimalArgs := strings.Split(decimal, ",")
				size, _ := strconv.ParseFloat(decimalArgs[1], 64)
				sizeNumber := math.Pow(10, size)
				val = math.Round(val*sizeNumber) / sizeNumber
				if hasOld && old != nil {
					valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
					if val == valOld {
						continue
					}
				}
				bind[name] = val
				if hasUpdate {
					updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
				}
			} else {
				if isZero {
					if hasOld && old == nil {
						continue
					}
					bind[name] = nil
					if hasUpdate {
						updateBind[name] = "NULL"
					}
					continue
				}
				sizeNumber := math.Pow(10, float64(precision))
				val = math.Round(val*sizeNumber) / sizeNumber
				if hasOld && old != nil {
					valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
					if valOld == val {
						continue
					}
				}
				bind[name] = val
				if hasUpdate {
					updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
				}
			}
		case "*orm.CachedQuery":
			continue
		case "time.Time":
			value := field.Interface().(time.Time)
			layout := "2006-01-02"
			var valueAsString string
			if tableSchema.tags[name]["time"] == "true" {
				if value.Year() == 1 {
					valueAsString = "0001-01-01 00:00:00"
				} else {
					layout += " 15:04:05"
				}
			} else if value.Year() == 1 {
				valueAsString = "0001-01-01"
			}
			if valueAsString == "" {
				valueAsString = value.Format(layout)
			}
			if hasOld && old == valueAsString {
				continue
			}
			bind[name] = valueAsString
			if hasUpdate {
				updateBind[name] = "'" + valueAsString + "'"
			}
			continue
		case "*time.Time":
			value := field.Interface().(*time.Time)
			layout := "2006-01-02"
			var valueAsString string
			if tableSchema.tags[name]["time"] == "true" {
				if value != nil {
					layout += " 15:04:05"
				}
			}
			if value != nil {
				valueAsString = value.Format(layout)
			}
			if hasOld && (old == valueAsString || (valueAsString == "" && (old == nil || old == "nil"))) {
				continue
			}
			if valueAsString == "" {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			} else {
				bind[name] = valueAsString
				if hasUpdate {
					updateBind[name] = "'" + valueAsString + "'"
				}
			}
		case "[]string":
			value := field.Interface().([]string)
			var valueAsString string
			if value != nil {
				valueAsString = strings.Join(value, ",")
			}
			if hasOld && (old == valueAsString || (valueAsString == "" && old == nil)) {
				continue
			}
			if isRequired || valueAsString != "" {
				bind[name] = valueAsString
				if hasUpdate {
					updateBind[name] = orm.escapeSQLParam(valueAsString)
				}
			} else if valueAsString == "" {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			}
		default:
			k := field.Kind().String()
			if k == "struct" {
				orm.fillBind(0, bind, updateBind, tableSchema, field.Type(), reflect.ValueOf(field.Interface()), oldData, fieldType.Name)
				continue
			} else if k == "ptr" {
				value := uint64(0)
				if !field.IsNil() {
					value = field.Elem().Field(1).Uint()
				}
				if hasOld && (old == value || ((old == nil || old == 0) && value == 0)) {
					continue
				}
				if value == 0 {
					bind[name] = nil
					if hasUpdate {
						updateBind[name] = "NULL"
					}
				} else {
					bind[name] = value
					if hasUpdate {
						updateBind[name] = strconv.FormatUint(value, 10)
					}
				}
				continue
			} else {
				value := field.Interface()
				var valString string
				if !field.IsZero() {
					if fieldTypeString[0:3] == "[]*" {
						length := field.Len()
						if length > 0 {
							ids := make([]uint64, length)
							for i := 0; i < length; i++ {
								ids[i] = field.Index(i).Interface().(Entity).GetID()
							}
							encoded, _ := jsoniter.ConfigFastest.Marshal(ids)
							valString = string(encoded)
						}
						if hasOld && (old == valString || ((old == nil || old == "0") && valString == "")) {
							continue
						}
						if valString == "" {
							bind[name] = nil
							if hasUpdate {
								updateBind[name] = "NULL"
							}
						} else {
							bind[name] = valString
							if hasUpdate {
								updateBind[name] = "'" + valString + "'"
							}
						}
						continue
					} else {
						var encoded []byte
						if hasOld && old != nil && old != "" {
							oldMap := reflect.New(field.Type()).Interface()
							newMap := reflect.New(field.Type()).Interface()
							_ = jsoniter.ConfigFastest.Unmarshal([]byte(old.(string)), oldMap)
							oldValue := reflect.ValueOf(oldMap).Elem().Interface()
							encoded, _ = jsoniter.ConfigFastest.Marshal(value)
							_ = jsoniter.ConfigFastest.Unmarshal(encoded, newMap)
							newValue := reflect.ValueOf(newMap).Elem().Interface()
							if cmp.Equal(newValue, oldValue) {
								continue
							}
						} else {
							encoded, _ = jsoniter.ConfigFastest.Marshal(value)
						}
						valString = string(encoded)
					}
				} else if hasOld && old == nil {
					continue
				}
				if isRequired || valString != "" {
					bind[name] = valString
					if hasUpdate {
						updateBind[name] = "'" + valString + "'"
					}
				} else if valString == "" {
					bind[name] = nil
					if hasUpdate {
						updateBind[name] = "NULL"
					}
				}
			}
		}
	}
}

func (orm *ORM) escapeSQLParam(val string) string {
	dest := make([]byte, 0, 2*len(val))
	var escape byte
	for i := 0; i < len(val); i++ {
		c := val[i]
		escape = 0
		switch c {
		case 0:
			escape = '0'
		case '\n':
			escape = 'n'
		case '\r':
			escape = 'r'
		case '\\':
			escape = '\\'
		case '\'':
			escape = '\''
		case '"':
			escape = '"'
		case '\032':
			escape = 'Z'
		}
		if escape != 0 {
			dest = append(dest, '\\', escape)
		} else {
			dest = append(dest, c)
		}
	}
	return "'" + string(dest) + "'"
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
