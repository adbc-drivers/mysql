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
	"context"
	"fmt"
	"strings"

	// register the "mysql" driver with database/sql
	_ "github.com/go-sql-driver/mysql"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/go-ext/variant"
)

// MySQLTypeConverter provides MySQL-specific type conversion enhancements
type mySQLTypeConverter struct {
	sqlwrapper.DefaultTypeConverter
}

// ConvertRawColumnType implements TypeConverter with MySQL-specific enhancements
func (m *mySQLTypeConverter) ConvertRawColumnType(colType sqlwrapper.ColumnType) (arrow.DataType, bool, arrow.Metadata, error) {
	typeName := strings.ToUpper(colType.DatabaseTypeName)
	nullable := colType.Nullable

	switch typeName {
	case "JSON":
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName,
			"sql.column_name":        colType.Name,
		}

		// Add length if available
		if colType.Length != nil {
			metadataMap["sql.length"] = fmt.Sprintf("%d", *colType.Length)
		}

		metadata := arrow.MetadataFrom(metadataMap)

		// Convert MySQL JSON to Arrow JSON extension type
		jsonType, err := extensions.NewJSONType(arrow.BinaryTypes.String)
		if err != nil {
			return nil, false, metadata, fmt.Errorf("failed to create JSON extension type: %w", err)
		}

		return jsonType, nullable, metadata, nil

	case "BIT":
		// Handle BIT type as binary data
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName,
			"sql.column_name":        colType.Name,
		}

		if colType.Length != nil {
			metadataMap["sql.length"] = fmt.Sprintf("%d", *colType.Length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.Binary, nullable, metadata, nil

	case "GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON":
		// Convert MySQL spatial types to binary with spatial metadata
		// TODO: we should use geoarrow extension types if applicable
		metadata := arrow.MetadataFrom(map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName,
			"sql.column_name":        colType.Name,
			"mysql.is_spatial":       "true",
		})
		return arrow.BinaryTypes.Binary, nullable, metadata, nil

	case "ENUM", "SET":
		// Handle ENUM/SET as string with special metadata
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName,
			"sql.column_name":        colType.Name,
			"mysql.is_enum_set":      "true",
		}

		if colType.Length != nil {
			metadataMap["sql.length"] = fmt.Sprintf("%d", *colType.Length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, nullable, metadata, nil

	case "TINYINT":
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName,
			"sql.column_name":        colType.Name,
		}

		metadata := arrow.MetadataFrom(metadataMap)

		return arrow.PrimitiveTypes.Int8, nullable, metadata, nil

	default:
		// Fall back to default conversion for standard types
		return m.DefaultTypeConverter.ConvertRawColumnType(colType)
	}
}

// CreateInserter creates MySQL-specific inserters bound to builders for enhanced performance
func (m *mySQLTypeConverter) CreateInserter(field *arrow.Field, builder array.Builder) (sqlwrapper.Inserter, error) {
	// Check for MySQL-specific types first
	switch field.Type.(type) {
	case *extensions.JSONType:
		return &mysqlJSONInserter{builder: builder}, nil
	case *arrow.BinaryType:
		if dbTypeName, ok := field.Metadata.GetValue("sql.database_type_name"); ok && dbTypeName == "BIT" {
			return &mysqlBitInserter{builder: builder.(array.BinaryLikeBuilder)}, nil
		}
		// Handle MySQL spatial types
		if isSpatial, ok := field.Metadata.GetValue("mysql.is_spatial"); ok && isSpatial == "true" {
			return &mysqlSpatialInserter{builder: builder.(array.BinaryLikeBuilder)}, nil
		}
		// Fall through to default for non-spatial binary
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	default:
		// For all other types, use default inserter
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	}
}

// MySQL-specific inserters
type mysqlJSONInserter struct {
	builder array.Builder
}

func (ins *mysqlJSONInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	var val string
	switch v := sqlValue.(type) {
	case []byte:
		val = string(v)
	case string:
		val = v
	default:
		val = fmt.Sprintf("%v", sqlValue)
	}

	// For extension types, we need to use AppendValueFromString
	// since the ExtensionBuilder doesn't implement StringLikeBuilder.Append
	return ins.builder.AppendValueFromString(val)
}

type mysqlBitInserter struct {
	builder array.BinaryLikeBuilder
}

func (ins *mysqlBitInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	var val []byte
	switch v := sqlValue.(type) {
	case []byte:
		val = v
	case string:
		val = []byte(v)
	default:
		val = []byte(fmt.Sprintf("%v", sqlValue))
	}

	ins.builder.Append(val)
	return nil
}

type mysqlSpatialInserter struct {
	builder array.BinaryLikeBuilder
}

func (ins *mysqlSpatialInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	var val []byte
	switch v := sqlValue.(type) {
	case []byte:
		val = v
	case string:
		val = []byte(v)
	default:
		val = []byte(fmt.Sprintf("%v", sqlValue))
	}

	ins.builder.Append(val)
	return nil
}

// ConvertArrowToGo implements MySQL-specific Arrow value to Go value conversion
func (m *mySQLTypeConverter) ConvertArrowToGo(arrowArray arrow.Array, index int, field *arrow.Field) (any, error) {
	if arrowArray.IsNull(index) {
		return nil, nil
	}

	// Handle MySQL-specific Arrow to Go conversions
	switch a := arrowArray.(type) {
	case *extensions.JSONArray:
		// Handle JSON extension type arrays
		jsonStr := a.ValueStr(index)
		v := variant.New(jsonStr)
		return v, nil

	case *array.Binary:
		if dbTypeName, ok := field.Metadata.GetValue("sql.database_type_name"); ok && dbTypeName == "BIT" {
			// For BIT fields, return the binary data as-is
			// This preserves the exact bit representation from the database
			bitData := a.Value(index)
			return bitData, nil
		}

		// Check if this is a spatial column
		if isSpatial, ok := field.Metadata.GetValue("mysql.is_spatial"); ok && isSpatial == "true" {
			// For spatial fields, return the binary data as-is
			// In a full implementation, we might parse WKB to geometry objects
			spatialData := a.Value(index)
			return spatialData, nil
		}
		// Fall through to default binary handling
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)

	case *array.Time32:
		// For MySQL driver, always convert Time32 arrays to time-only format strings
		// This handles both explicit TIME column metadata and parameter binding scenarios
		timeType := a.DataType().(*arrow.Time32Type)
		t := a.Value(index).ToTime(timeType.Unit)
		return t.Format("15:04:05.000000"), nil

	case *array.Time64:
		// For MySQL driver, always convert Time64 arrays to time-only format strings
		// This handles both explicit TIME column metadata and parameter binding scenarios
		timeType := a.DataType().(*arrow.Time64Type)
		t := a.Value(index).ToTime(timeType.Unit)
		return t.Format("15:04:05.000000"), nil

	default:
		// For all other types, use default conversion
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)
	}
}

// mysqlConnectionImpl extends sqlwrapper connection with DbObjectsEnumerator
type mysqlConnectionImpl struct {
	*sqlwrapper.ConnectionImplBase // Embed sqlwrapper connection for all standard functionality

	version string
}

// implements BulkIngester interface
var _ sqlwrapper.BulkIngester = (*mysqlConnectionImpl)(nil)

// implements DbObjectsEnumerator interface
var _ driverbase.DbObjectsEnumerator = (*mysqlConnectionImpl)(nil)

// implements CurrentNameSpacer interface
var _ driverbase.CurrentNamespacer = (*mysqlConnectionImpl)(nil)

// implements TableTypeLister interface
var _ driverbase.TableTypeLister = (*mysqlConnectionImpl)(nil)

// mysqlConnectionFactory creates MySQL connections
type mysqlConnectionFactory struct{}

// CreateConnection implements sqlwrapper.ConnectionFactory
func (f *mysqlConnectionFactory) CreateConnection(
	ctx context.Context,
	conn *sqlwrapper.ConnectionImplBase,
) (sqlwrapper.ConnectionImpl, error) {
	// Wrap the pre-built sqlwrapper connection with MySQL-specific functionality
	return &mysqlConnectionImpl{
		ConnectionImplBase: conn,
	}, nil
}

// NewDriver constructs the ADBC Driver for "mysql".
func NewDriver(alloc memory.Allocator) adbc.Driver {
	typeConverter := &mySQLTypeConverter{
		DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{},
	}

	driver := sqlwrapper.NewDriver(alloc, "mysql", "MySQL", typeConverter).
		WithConnectionFactory(&mysqlConnectionFactory{})
	driver.DriverInfo.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoDriverName:      "ADBC Driver Foundry Driver for MySQL",
		adbc.InfoVendorSql:       true,
		adbc.InfoVendorSubstrait: false,
	})

	return driver
}
