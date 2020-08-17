package doclite

import (
	"os"
	"reflect"
	"sync"
)

var readWriteMutex sync.Mutex

func binarySearch(key int64, nodes []*Node, nodesLen int) int {
	l := 0
	r := nodesLen
	mid := nodesLen

	for r-l > 0 {
		mid = l + (r-l)/2
		if nodes[mid].document.id == key {
			return mid
		} else if key < nodes[mid].document.id {
			r = mid
		} else {
			l = mid + 1
		}
	}
	for mid-1 > 0 {
		if nodes[mid].document.id < key {
			return mid
		} else if nodes[mid].document.id == key {
			return mid
		} else if nodes[mid-1].document.id < key {
			return mid - 1
		}
		mid--
	}

	return mid
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func minInt64(a, b int64) int64 {
	if a > b {
		return b
	}
	return a
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func maxInt64(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}

func toMap(model interface{}) map[string]interface{} {
	ret := make(map[string]interface{})

	modelReflect := reflect.ValueOf(model)

	if modelReflect.Kind() == reflect.Map {
		iter := modelReflect.MapRange()
		for iter.Next() {
			ret[iter.Key().String()] = iter.Value().Interface()
		}
		return ret
	}
	if modelReflect.Kind() == reflect.Ptr {
		modelReflect = modelReflect.Elem()
	}

	modelRefType := modelReflect.Type()
	fieldsCount := modelReflect.NumField()

	var fieldData interface{}

loop:
	for i := 0; i < fieldsCount; i++ {
		field := modelReflect.Field(i)
		if field.IsZero() {
			switch field.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			default:
				continue loop
			}
		}
		switch field.Kind() {
		case reflect.Struct:
			fallthrough
		case reflect.Ptr:
			fieldData = toMap(field.Interface())
		default:
			fieldData = field.Interface()
		}

		ret[modelRefType.Field(i).Name] = fieldData
	}
	return ret
}

func read(f *os.File, offset int64, buf []byte) (int, error) {
	readWriteMutex.Lock()
	defer readWriteMutex.Unlock()
	return f.ReadAt(buf, offset)
}

func write(f *os.File, offset int64, data []byte) error {
	readWriteMutex.Lock()
	defer readWriteMutex.Unlock()
	_, err := f.WriteAt(data, offset)
	if err != nil {
		return err
	}
	return nil
}
