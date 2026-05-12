// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"bytes"
	"context"
	"testing"

	"github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowToCSV_Basic(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int", Type: arrow.PrimitiveTypes.Int32},
		{Name: "str", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b"}, nil)

	rec := b.NewRecordBatch()
	defer rec.Release()

	rr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	defer rr.Release()

	tc := &mySQLTypeConverter{
		DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{VendorName: "MySQL"},
	}
	it, err := sqlwrapper.NewRowBufferIterator(rr, 5, tc)
	require.NoError(t, err)

	config := CSVConfig{
		FieldDelimiter:  '\t',
		LineTerminator:  '\n',
		NullValue:       "\\N",
		EscapeBackslash: true,
	}

	var buf bytes.Buffer
	err = arrowToCSV(context.Background(), &buf, it, 2, config)
	require.NoError(t, err)

	expected := "1\ta\n2\tb\n"
	assert.Equal(t, expected, buf.String())
}

func TestArrowToCSV_Escaping(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "str", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	// Testing backslash, newline, tab, and carriage return
	b.Field(0).(*array.StringBuilder).AppendValues([]string{
		"back\\slash",
		"new\nline",
		"ta\bt",
		"car\rriage",
		"null\x00byte",
	}, nil)

	rec := b.NewRecordBatch()
	defer rec.Release()

	rr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	defer rr.Release()

	tc := &mySQLTypeConverter{
		DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{VendorName: "MySQL"},
	}
	it, err := sqlwrapper.NewRowBufferIterator(rr, 5, tc)
	require.NoError(t, err)

	config := CSVConfig{
		FieldDelimiter:  '\t',
		LineTerminator:  '\n',
		NullValue:       "\\N",
		EscapeBackslash: true,
	}

	var buf bytes.Buffer
	err = arrowToCSV(context.Background(), &buf, it, 1, config)
	require.NoError(t, err)

	// In TSV, backslash is escaped as \\, newline as \n, tab as \t
	// We handle \, \n, \t, \r, \b, \Z (Ctrl+Z), and \0 (Null byte)
	expected := "back\\\\slash\nnew\\nline\nta\\bt\ncar\\rriage\nnull\\0byte\n"
	assert.Equal(t, expected, buf.String())
}

func TestArrowToCSV_Nulls(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int", Type: arrow.PrimitiveTypes.Int32},
		{Name: "str", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{0, 1}, []bool{false, true})
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"", "val"}, []bool{false, true})

	rec := b.NewRecord()
	defer rec.Release()

	rr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	defer rr.Release()

	tc := &mySQLTypeConverter{
		DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{VendorName: "MySQL"},
	}
	it, err := sqlwrapper.NewRowBufferIterator(rr, 5, tc)
	require.NoError(t, err)

	config := CSVConfig{
		FieldDelimiter:  '\t',
		LineTerminator:  '\n',
		NullValue:       "\\N",
		EscapeBackslash: true,
	}

	var buf bytes.Buffer
	err = arrowToCSV(context.Background(), &buf, it, 2, config)
	require.NoError(t, err)

	// Row 1: Null int, Null string -> \N\t\N\n
	// Row 2: 1 int, val string -> 1\tval\n
	expected := "\\N\t\\N\n1\tval\n"
	assert.Equal(t, expected, buf.String())
}

func TestArrowToCSV_Batching(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	for i := 1; i <= 10; i++ {
		b.Field(0).(*array.Int32Builder).Append(int32(i))
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	rr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	defer rr.Release()

	tc := &mySQLTypeConverter{
		DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{VendorName: "MySQL"},
	}
	// Set batch size to 3 to test multiple writes
	it, err := sqlwrapper.NewRowBufferIterator(rr, 3, tc)
	require.NoError(t, err)

	config := CSVConfig{
		FieldDelimiter:  ',',
		LineTerminator:  '\n',
		NullValue:       "NULL",
		EscapeBackslash: false,
	}

	var buf bytes.Buffer
	err = arrowToCSV(context.Background(), &buf, it, 1, config)
	require.NoError(t, err)

	expected := "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n"
	assert.Equal(t, expected, buf.String())
}

func TestArrowToCSV_Types(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "float", Type: arrow.PrimitiveTypes.Float64},
		{Name: "bin", Type: arrow.BinaryTypes.Binary},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.BooleanBuilder).AppendValues([]bool{true, false}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{1.23, 4.56}, nil)
	b.Field(2).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("bin1"), []byte("bin2")}, nil)

	rec := b.NewRecordBatch()
	defer rec.Release()

	rr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	defer rr.Release()

	tc := &mySQLTypeConverter{
		DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{VendorName: "MySQL"},
	}
	it, err := sqlwrapper.NewRowBufferIterator(rr, 5, tc)
	require.NoError(t, err)

	config := CSVConfig{
		FieldDelimiter:  ',',
		LineTerminator:  '\n',
		NullValue:       "\\N",
		EscapeBackslash: true,
	}

	var buf bytes.Buffer
	err = arrowToCSV(context.Background(), &buf, it, 3, config)
	require.NoError(t, err)

	// Boolean true/false might be formatted as 1/0 or true/false depending on TypeConverter.
	// MySQL driver usually prefers 1/0 for boolean.
	expected := "1,1.23,bin1\n0,4.56,bin2\n"
	assert.Equal(t, expected, buf.String())
}
