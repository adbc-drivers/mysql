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
	"time"

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

	case "TIMESTAMP":
		var timestampType arrow.DataType
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		if colType.Precision != nil {
			precision := *colType.Precision
			metadataMap[sqlwrapper.MetaKeyFractionalSecondsPrecision] = fmt.Sprintf("%d", precision)
			if precision > 6 {
				precision = 6
			}
			timeUnit := arrow.TimeUnit(precision / 3)
			// TODO: hardcoded to UTC for now. MySQL's TIMESTAMP retrieval converts the stored UTC value back to the SESSION timezone,
			// so we must retrieve and set that zone here. (https://dev.mysql.com/doc/refman/9.5/en/datetime.html)
			timestampType = &arrow.TimestampType{Unit: timeUnit, TimeZone: "UTC"}
		} else {
			// No precision info available, default to microseconds (most common)
			timestampType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return timestampType, colType.Nullable, metadata, nil

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

	t, ok := sqlValue.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte for mysql json inserter, got %T", sqlValue)
	}

	// For extension types, we need to use AppendValueFromString
	// since the ExtensionBuilder doesn't implement StringLikeBuilder.Append
	return ins.builder.AppendValueFromString(string(t))
}

type mysqlBitInserter struct {
	builder array.BinaryLikeBuilder
}

func (ins *mysqlBitInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := sqlValue.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte for mysql bit inserter, got %T", sqlValue)
	}

	ins.builder.Append(t)
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

	t, ok := sqlValue.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte for mysql spatial inserter, got %T", sqlValue)
	}

	ins.builder.Append(t)
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

	case *array.Timestamp:
		timestampType := a.DataType().(*arrow.TimestampType)
		rawValue := a.Value(index)
		t := rawValue.ToTime(timestampType.Unit)

		// For nanosecond precision, truncate to microseconds
		if timestampType.Unit == arrow.Nanosecond {
			microseconds := t.UnixMicro()
			converted := time.UnixMicro(microseconds).UTC()
			return converted, nil
		}

		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)

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
	vendorName := "MySQL"
	typeConverter := &mySQLTypeConverter{
		DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{VendorName: vendorName},
	}

	driver := sqlwrapper.NewDriver(alloc, "mysql", vendorName, NewMySQLDBFactory(), typeConverter).
		WithConnectionFactory(&mysqlConnectionFactory{}).
		WithErrorInspector(MySQLErrorInspector{})
	driver.DriverInfo.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoDriverName:      "ADBC Driver Foundry Driver for MySQL",
		adbc.InfoVendorSql:       true,
		adbc.InfoVendorSubstrait: false,
	})

	return driver
}
