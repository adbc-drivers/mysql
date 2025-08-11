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
	"database/sql"
	"fmt"
	"strings"

	// register the "mysql" driver with database/sql
	_ "github.com/go-sql-driver/mysql"

	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/go-ext/variant"
)

// MySQLTypeConverter provides MySQL-specific type conversion enhancements
type mySQLTypeConverter struct {
	sqlwrapper.DefaultTypeConverter
}

// ConvertColumnType implements TypeConverter with MySQL-specific enhancements
func (m *mySQLTypeConverter) ConvertColumnType(colType *sql.ColumnType) (arrow.DataType, bool, arrow.Metadata, error) {
	typeName := strings.ToUpper(colType.DatabaseTypeName())

	switch typeName {
	case "JSON":
		// Convert MySQL JSON to Arrow string with special metadata
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName(),
			"sql.column_name":        colType.Name(),
			"mysql.is_json":          "true",
		}

		// Add length if available
		if length, ok := colType.Length(); ok {
			metadataMap["sql.length"] = fmt.Sprintf("%d", length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, true, metadata, nil

	case "GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON":
		// Convert MySQL spatial types to binary with spatial metadata
		metadata := arrow.MetadataFrom(map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName(),
			"sql.column_name":        colType.Name(),
			"mysql.is_spatial":       "true",
		})
		return arrow.BinaryTypes.Binary, true, metadata, nil

	case "ENUM", "SET":
		// Handle ENUM/SET as string with special metadata
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName(),
			"sql.column_name":        colType.Name(),
			"mysql.is_enum_set":      "true",
		}

		if length, ok := colType.Length(); ok {
			metadataMap["sql.length"] = fmt.Sprintf("%d", length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, true, metadata, nil

	default:
		// Fall back to default conversion for standard types
		return m.DefaultTypeConverter.ConvertColumnType(colType)
	}
}

// ConvertSQLToArrow implements MySQL-specific SQL value to Arrow value conversion
func (m *mySQLTypeConverter) ConvertSQLToArrow(sqlValue any, field *arrow.Field) (any, error) {
	// Handle MySQL-specific type conversions
	switch field.Type.(type) {
	case *arrow.TimestampType:
		// MySQL TIMESTAMP is timezone-aware, but let the default converter handle the actual conversion
		// The default converter now properly handles []byte to time.Time conversion
		return m.DefaultTypeConverter.ConvertSQLToArrow(sqlValue, field)

	case *arrow.StringType:
		// Handle MySQL JSON types specially
		if isJSON, ok := field.Metadata.GetValue("mysql.is_json"); ok && isJSON == "true" {
			// For JSON columns, we might want to validate or pretty-format JSON
			switch v := sqlValue.(type) {
			case []byte:
				// MySQL returns JSON as []byte, convert to string
				return string(v), nil
			case string:
				return v, nil
			default:
				return fmt.Sprintf("%v", sqlValue), nil
			}
		}
		// Fall through to default for non-JSON strings
		return m.DefaultTypeConverter.ConvertSQLToArrow(sqlValue, field)

	case *arrow.BinaryType:
		// Handle MySQL spatial types
		if isSpatial, ok := field.Metadata.GetValue("mysql.is_spatial"); ok && isSpatial == "true" {
			// For spatial types, ensure we preserve binary data correctly
			switch v := sqlValue.(type) {
			case []byte:
				return v, nil
			case string:
				return []byte(v), nil
			default:
				return []byte(fmt.Sprintf("%v", sqlValue)), nil
			}
		}
		// Fall through to default for non-spatial binary
		return m.DefaultTypeConverter.ConvertSQLToArrow(sqlValue, field)

	default:
		// For all other types, use default conversion
		return m.DefaultTypeConverter.ConvertSQLToArrow(sqlValue, field)
	}
}

// ConvertArrowToGo implements MySQL-specific Arrow value to Go value conversion
func (m *mySQLTypeConverter) ConvertArrowToGo(arrowArray arrow.Array, index int, field *arrow.Field) (any, error) {
	if arrowArray.IsNull(index) {
		return nil, nil
	}

	// Handle MySQL-specific Arrow to Go conversions
	switch a := arrowArray.(type) {
	case *array.String:
		// Check if this is a JSON column by looking at field metadata
		if isJSON, ok := field.Metadata.GetValue("mysql.is_json"); ok && isJSON == "true" {
			// For JSON fields, parse to variant
			jsonStr := a.Value(index)
			v := variant.New(jsonStr)
			return v, nil
		}
		// Fall through to default string handling
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)

	case *array.Binary:
		// Check if this is a spatial column
		if isSpatial, ok := field.Metadata.GetValue("mysql.is_spatial"); ok && isSpatial == "true" {
			// For spatial fields, return the binary data as-is
			// In a full implementation, we might parse WKB to geometry objects
			spatialData := a.Value(index)
			return spatialData, nil
		}
		// Fall through to default binary handling
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)

	case *array.Timestamp:
		// For timestamp arrays, check if they need timezone conversion
		timestampType := a.DataType().(*arrow.TimestampType)
		value := a.Value(index)

		// Check if this is a MySQL TIMESTAMP vs DATETIME
		if dbType, ok := field.Metadata.GetValue("sql.database_type_name"); ok {
			if strings.ToUpper(dbType) == "TIMESTAMP" {
				// MySQL TIMESTAMP - ensure proper timezone handling
				time := value.ToTime(timestampType.Unit)
				// If the timestamp type has no timezone, assume UTC for MySQL TIMESTAMP
				if timestampType.TimeZone == "" {
					time = time.UTC()
				}
				return time, nil
			}
		}
		// Fall through to default timestamp handling
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)

	default:
		// For all other types, use default conversion
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)
	}
}

// NewDriver constructs the ADBC Driver for "mysql".
func NewDriver() adbc.Driver {
	// Create sqlwrapper driver with MySQL type converter and driver name
	return sqlwrapper.NewDriver("mysql", &mySQLTypeConverter{
		DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{},
	})
}
