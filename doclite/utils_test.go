package doclite

import (
	"testing"
)

// toMapTests holds shared setup for toMap filter tests.
type toMapTestDoc struct {
	Name    string
	ValF32  float32
	ValF64  float64
	ValU    uint
	ValU8   uint8
	ValU16  uint16
	ValU32  uint32
	ValU64  uint64
	ValI    int
	ValI64  int64
}

// TestToMapIncludesZeroFloat32 verifies that a zero-valued float32 field is
// included in the map produced by toMap.
func TestToMapIncludesZeroFloat32(t *testing.T) {
	filter := toMapTestDoc{ValF32: 0.0}
	m := toMap(filter)

	if _, ok := m["ValF32"]; !ok {
		t.Error("zero-valued float32 field ValF32 is missing from toMap output")
	}
}

// TestToMapIncludesZeroFloat64 verifies that a zero-valued float64 field is
// included in the map produced by toMap.
func TestToMapIncludesZeroFloat64(t *testing.T) {
	filter := toMapTestDoc{ValF64: 0.0}
	m := toMap(filter)

	if _, ok := m["ValF64"]; !ok {
		t.Error("zero-valued float64 field ValF64 is missing from toMap output")
	}
}

// TestToMapIncludesZeroUint verifies that a zero-valued uint field is
// included in the map produced by toMap.
func TestToMapIncludesZeroUint(t *testing.T) {
	filter := toMapTestDoc{ValU: 0}
	m := toMap(filter)

	if _, ok := m["ValU"]; !ok {
		t.Error("zero-valued uint field ValU is missing from toMap output")
	}
}

// TestToMapIncludesZeroUint8 verifies that a zero-valued uint8 field is
// included in the map produced by toMap.
func TestToMapIncludesZeroUint8(t *testing.T) {
	filter := toMapTestDoc{ValU8: 0}
	m := toMap(filter)

	if _, ok := m["ValU8"]; !ok {
		t.Error("zero-valued uint8 field ValU8 is missing from toMap output")
	}
}

// TestToMapIncludesZeroUint16 verifies that a zero-valued uint16 field is
// included in the map produced by toMap.
func TestToMapIncludesZeroUint16(t *testing.T) {
	filter := toMapTestDoc{ValU16: 0}
	m := toMap(filter)

	if _, ok := m["ValU16"]; !ok {
		t.Error("zero-valued uint16 field ValU16 is missing from toMap output")
	}
}

// TestToMapIncludesZeroUint32 verifies that a zero-valued uint32 field is
// included in the map produced by toMap.
func TestToMapIncludesZeroUint32(t *testing.T) {
	filter := toMapTestDoc{ValU32: 0}
	m := toMap(filter)

	if _, ok := m["ValU32"]; !ok {
		t.Error("zero-valued uint32 field ValU32 is missing from toMap output")
	}
}

// TestToMapIncludesZeroUint64 verifies that a zero-valued uint64 field is
// included in the map produced by toMap.
func TestToMapIncludesZeroUint64(t *testing.T) {
	filter := toMapTestDoc{ValU64: 0}
	m := toMap(filter)

	if _, ok := m["ValU64"]; !ok {
		t.Error("zero-valued uint64 field ValU64 is missing from toMap output")
	}
}

// TestToMapSkipsZeroString verifies that zero-valued (empty) string fields
// are still skipped (out-of-scope: this behavior must not change).
func TestToMapSkipsZeroString(t *testing.T) {
	filter := toMapTestDoc{Name: ""}
	m := toMap(filter)

	if _, ok := m["Name"]; ok {
		t.Error("empty string field Name should be excluded from toMap output")
	}
}

// TestToMapIncludesAllZeroNumericFields verifies that all numeric zero-valued
// fields (int, uint, float) are included in the map simultaneously.
func TestToMapIncludesAllZeroNumericFields(t *testing.T) {
	filter := toMapTestDoc{
		ValF32: 0.0,
		ValF64: 0.0,
		ValU:   0,
		ValU8:  0,
		ValU16: 0,
		ValU32: 0,
		ValU64: 0,
		ValI:   0,
		ValI64: 0,
	}
	m := toMap(filter)

	expectedFields := []string{"ValF32", "ValF64", "ValU", "ValU8", "ValU16", "ValU32", "ValU64", "ValI", "ValI64"}
	for _, field := range expectedFields {
		if _, ok := m[field]; !ok {
			t.Errorf("zero-valued numeric field %s is missing from toMap output", field)
		}
	}

	// String should still be excluded
	if _, ok := m["Name"]; ok {
		t.Error("empty string field Name should be excluded from toMap output")
	}
}

// TestToMapPointerStruct verifies that toMap correctly handles a pointer to
// a struct, including zero-valued float/uint fields.
func TestToMapPointerStruct(t *testing.T) {
	filter := &toMapTestDoc{ValF32: 0.0, ValU: 0}
	m := toMap(filter)

	if _, ok := m["ValF32"]; !ok {
		t.Error("zero-valued float32 field ValF32 is missing from toMap output (pointer struct)")
	}
	if _, ok := m["ValU"]; !ok {
		t.Error("zero-valued uint field ValU is missing from toMap output (pointer struct)")
	}
}

// TestToMapAllZeroTypes tests that a struct with all fields at zero values
// still includes float and uint fields while excluding strings.
func TestToMapAllZeroTypes(t *testing.T) {
	type allZero struct {
		S    string
		F32  float32
		F64  float64
		U    uint
		U8   uint8
		U16  uint16
		U32  uint32
		U64  uint64
		I    int
		I8   int8
		I16  int16
		I32  int32
		I64  int64
	}

	filter := allZero{}
	m := toMap(filter)

	// All numeric types should be present
	numericFields := []string{"F32", "F64", "U", "U8", "U16", "U32", "U64", "I", "I8", "I16", "I32", "I64"}
	for _, f := range numericFields {
		if _, ok := m[f]; !ok {
			t.Errorf("zero-valued numeric field %s should be included", f)
		}
	}

	// String should be excluded
	if _, ok := m["S"]; ok {
		t.Error("empty string field S should be excluded from toMap output")
	}
}
