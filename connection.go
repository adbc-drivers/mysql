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
	"errors"
	"fmt"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *mysqlConnectionImpl) GetCurrentCatalog() (string, error) {
	var database string
	err := c.Db.QueryRowContext(context.Background(), "SELECT DATABASE()").Scan(&database)
	if err != nil {
		return "", fmt.Errorf("failed to get current database: %w", err)
	}
	if database == "" {
		return "", fmt.Errorf("no current database set")
	}
	return database, nil
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *mysqlConnectionImpl) GetCurrentDbSchema() (string, error) {
	return "", nil
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *mysqlConnectionImpl) SetCurrentCatalog(catalog string) error {
	_, err := c.Db.ExecContext(context.Background(), "USE "+catalog)
	return err
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *mysqlConnectionImpl) SetCurrentDbSchema(schema string) error {
	if schema != "" {
		return fmt.Errorf("cannot set schema in MySQL: schemas are not supported")
	}
	return nil
}

func (c *mysqlConnectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if c.version == "" {
		var version, comment string
		if err := c.Conn.QueryRowContext(ctx, "SELECT @@version, @@version_comment").Scan(&version, &comment); err != nil {
			return c.ErrorHelper.Errorf(adbc.StatusInternal, "failed to get version: %v", err)
		}
		c.version = fmt.Sprintf("%s (%s)", version, comment)
	}
	return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorVersion, c.version)
}

// ExecuteBulkIngest performs MySQL bulk ingest using INSERT statements
func (c *mysqlConnectionImpl) ExecuteBulkIngest(ctx context.Context, options *driverbase.BulkIngestOptions, stream array.RecordReader) (rowCount int64, err error) {
	if stream == nil {
		return -1, c.Base().ErrorHelper.InvalidArgument("stream cannot be nil")
	}

	tableName := "target_table"
	if options.TableName != "" {
		tableName = options.TableName
	}

	var tableCreated bool
	var totalRowsInserted int64

	// Process each record batch in the stream
	for stream.Next() {
		record := stream.Record()
		schema := record.Schema()

		// Create table if needed (only on first batch)
		if !tableCreated {
			if err := c.createTableIfNeeded(ctx, tableName, schema, options); err != nil {
				return -1, c.Base().ErrorHelper.IO("failed to create table: %v", err)
			}
			tableCreated = true
		}

		// Build INSERT statement
		var placeholders []string
		for i := 0; i < int(schema.NumFields()); i++ {
			placeholders = append(placeholders, "?")
		}

		insertSQL := fmt.Sprintf("INSERT INTO `%s` VALUES (%s)",
			tableName,
			strings.Join(placeholders, ", "))

		// Prepare the statement
		stmt, err := c.Conn.PrepareContext(ctx, insertSQL)
		if err != nil {
			return -1, c.Base().ErrorHelper.IO("failed to prepare insert statement: %v", err)
		}
		defer func() {
			err = errors.Join(err, stmt.Close())
		}()

		// Insert each row
		rowsInBatch := int(record.NumRows())
		for rowIdx := 0; rowIdx < rowsInBatch; rowIdx++ {
			params := make([]any, record.NumCols())

			for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
				arr := record.Column(colIdx)
				field := schema.Field(colIdx)

				// Use type converter to get Go value
				value, err := c.TypeConverter.ConvertArrowToGo(arr, rowIdx, &field)
				if err != nil {
					return -1, c.Base().ErrorHelper.IO("failed to convert value at row %d, col %d: %v", rowIdx, colIdx, err)
				}
				params[colIdx] = value
			}

			// Execute the insert
			_, err := stmt.ExecContext(ctx, params...)
			if err != nil {
				return -1, c.Base().ErrorHelper.IO("failed to execute insert: %v", err)
			}
		}

		// Track rows inserted in this batch
		totalRowsInserted += int64(rowsInBatch)
	}

	// Check for stream errors
	if err := stream.Err(); err != nil {
		return -1, c.Base().ErrorHelper.IO("stream error: %v", err)
	}

	return totalRowsInserted, nil
}

// createTableIfNeeded creates the table based on the ingest mode
func (c *mysqlConnectionImpl) createTableIfNeeded(ctx context.Context, tableName string, schema *arrow.Schema, options *driverbase.BulkIngestOptions) error {
	switch options.Mode {
	case adbc.OptionValueIngestModeCreate:
		// Create the table (fail if exists)
		return c.createTable(ctx, tableName, schema, false)
	case adbc.OptionValueIngestModeCreateAppend:
		// Create the table if it doesn't exist
		return c.createTable(ctx, tableName, schema, true)
	case adbc.OptionValueIngestModeReplace:
		// Drop and recreate the table (ignore error if table doesn't exist)
		_ = c.dropTable(ctx, tableName)
		return c.createTable(ctx, tableName, schema, false)
	case adbc.OptionValueIngestModeAppend:
		// Table should already exist, do nothing
		return nil
	default:
		return fmt.Errorf("unsupported ingest mode: %s", options.Mode)
	}
}

// createTable creates a MySQL table from Arrow schema
func (c *mysqlConnectionImpl) createTable(ctx context.Context, tableName string, schema *arrow.Schema, ifNotExists bool) error {
	var queryBuilder strings.Builder
	queryBuilder.WriteString("CREATE TABLE ")
	if ifNotExists {
		queryBuilder.WriteString("IF NOT EXISTS ")
	}
	queryBuilder.WriteString("`")
	queryBuilder.WriteString(tableName)
	queryBuilder.WriteString("` (")

	for i, field := range schema.Fields() {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}

		queryBuilder.WriteString("`")
		queryBuilder.WriteString(field.Name)
		queryBuilder.WriteString("` ")

		// Convert Arrow type to MySQL type
		mysqlType := c.arrowToMySQLType(field.Type, field.Nullable)
		queryBuilder.WriteString(mysqlType)
	}

	queryBuilder.WriteString(")")

	_, err := c.Conn.ExecContext(ctx, queryBuilder.String())
	return err
}

// dropTable drops a MySQL table
func (c *mysqlConnectionImpl) dropTable(ctx context.Context, tableName string) error {
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName)
	_, err := c.Conn.ExecContext(ctx, dropSQL)
	return err
}

// arrowToMySQLType converts Arrow data type to MySQL column type
func (c *mysqlConnectionImpl) arrowToMySQLType(arrowType arrow.DataType, nullable bool) string {
	var mysqlType string

	switch arrowType.(type) {
	case *arrow.BooleanType:
		mysqlType = "BOOLEAN"
	case *arrow.Int8Type:
		mysqlType = "TINYINT"
	case *arrow.Int16Type:
		mysqlType = "SMALLINT"
	case *arrow.Int32Type:
		mysqlType = "INT"
	case *arrow.Int64Type:
		mysqlType = "BIGINT"
	case *arrow.Float32Type:
		mysqlType = "FLOAT"
	case *arrow.Float64Type:
		mysqlType = "DOUBLE"
	case *arrow.StringType:
		mysqlType = "TEXT"
	case *arrow.BinaryType:
		mysqlType = "BLOB"
	case *arrow.Date32Type:
		mysqlType = "DATE"
	case *arrow.TimestampType:
		mysqlType = "TIMESTAMP"
	case *arrow.Time32Type, *arrow.Time64Type:
		mysqlType = "TIME"
	case *arrow.Decimal32Type, *arrow.Decimal64Type, *arrow.Decimal128Type, *arrow.Decimal256Type:
		if decType, ok := arrowType.(arrow.DecimalType); ok {
			mysqlType = fmt.Sprintf("DECIMAL(%d,%d)", decType.GetPrecision(), decType.GetScale())
		} else {
			mysqlType = "DECIMAL(10,2)"
		}
	default:
		// Default to TEXT for unknown types
		mysqlType = "TEXT"
	}

	if nullable {
		mysqlType += " NULL"
	} else {
		mysqlType += " NOT NULL"
	}

	return mysqlType
}
