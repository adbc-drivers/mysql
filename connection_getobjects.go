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
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
)

func (c *mysqlConnectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	query := `SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA`
	args := []any{}

	if catalogFilter != nil {
		query += ` WHERE SCHEMA_NAME LIKE ?`
		args = append(args, *catalogFilter)
	}

	query += ` ORDER BY SCHEMA_NAME`

	rows, err := c.Db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query catalogs: %w", err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	catalogs := make([]string, 0)
	for rows.Next() {
		var catalog string
		if err := rows.Scan(&catalog); err != nil {
			return nil, fmt.Errorf("failed to scan catalog: %w", err)
		}
		catalogs = append(catalogs, catalog)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during catalog iteration: %w", err)
	}

	return catalogs, nil
}

func (c *mysqlConnectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	// In MySQL, catalog and schema are the same concept (database name)
	// Since there are no sub-schemas within a MySQL database, we return the database itself as the schema
	// if it exists and matches the filter

	// Build query to check if catalog exists and optionally apply filter
	query := "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?"
	args := []any{catalog}

	// Apply schema filter if provided (using SQL LIKE for consistency with GetCatalogs)
	if schemaFilter != nil {
		query += " AND SCHEMA_NAME LIKE ?"
		args = append(args, *schemaFilter)
	}

	var schemaName string
	err := c.Db.QueryRowContext(ctx, query, args...).Scan(&schemaName)

	if err != nil {
		if err == sql.ErrNoRows {
			return []string{}, nil // Catalog doesn't exist or doesn't match filter
		}
		return nil, fmt.Errorf("failed to check catalog existence: %w", err)
	}

	// Return the catalog name as the schema since they're the same in MySQL
	return []string{schemaName}, nil
}

func (c *mysqlConnectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	if includeColumns {
		return c.getTablesWithColumns(ctx, catalog, schema, tableFilter, columnFilter)
	}
	return c.getTablesOnly(ctx, catalog, schema, tableFilter)
}

// getTablesOnly retrieves table information without columns
func (c *mysqlConnectionImpl) getTablesOnly(ctx context.Context, catalog string, schema string, tableFilter *string) ([]driverbase.TableInfo, error) {
	// In MySQL, catalog and schema must be the same (database name)
	if catalog != schema {
		return nil, fmt.Errorf("in MySQL, catalog must equal schema (got catalog=%s, schema=%s)", catalog, schema)
	}

	query := `
		SELECT
			TABLE_NAME,
			TABLE_TYPE
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = ?`

	args := []any{schema}

	if tableFilter != nil {
		query += ` AND TABLE_NAME LIKE ?`
		args = append(args, *tableFilter)
	}

	query += ` ORDER BY TABLE_NAME`

	rows, err := c.Db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables for schema %s: %w", schema, err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	tables := make([]driverbase.TableInfo, 0)
	for rows.Next() {
		var tableName, tableType string
		if err := rows.Scan(&tableName, &tableType); err != nil {
			return nil, fmt.Errorf("failed to scan table info: %w", err)
		}

		tables = append(tables, driverbase.TableInfo{
			TableName: tableName,
			TableType: tableType,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during table iteration: %w", err)
	}

	return tables, nil
}

// getTablesWithColumns retrieves complete table and column information
func (c *mysqlConnectionImpl) getTablesWithColumns(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string) ([]driverbase.TableInfo, error) {
	// In MySQL, catalog and schema must be the same (database name)
	if catalog != schema {
		return nil, fmt.Errorf("in MySQL, catalog must equal schema (got catalog=%s, schema=%s)", catalog, schema)
	}

	type tableColumn struct {
		TableName       string
		TableType       string
		OrdinalPosition int32
		ColumnName      string
		ColumnComment   sql.NullString
		DataType        string
		IsNullable      string
		ColumnDefault   sql.NullString
	}

	query := `
		SELECT
			t.TABLE_NAME,
			t.TABLE_TYPE,
			c.ORDINAL_POSITION,
			c.COLUMN_NAME,
			c.COLUMN_COMMENT,
			c.DATA_TYPE,
			c.IS_NULLABLE,
			c.COLUMN_DEFAULT
		FROM INFORMATION_SCHEMA.TABLES t
		INNER JOIN INFORMATION_SCHEMA.COLUMNS c
			ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
			AND t.TABLE_NAME = c.TABLE_NAME
		WHERE t.TABLE_SCHEMA = ?`

	args := []any{schema}

	if tableFilter != nil {
		query += ` AND t.TABLE_NAME LIKE ?`
		args = append(args, *tableFilter)
	}
	if columnFilter != nil {
		query += ` AND c.COLUMN_NAME LIKE ?`
		args = append(args, *columnFilter)
	}

	query += ` ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION`

	rows, err := c.Db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables with columns for schema %s: %w", schema, err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	tables := make([]driverbase.TableInfo, 0)
	var currentTable *driverbase.TableInfo

	for rows.Next() {
		var tc tableColumn

		if err := rows.Scan(
			&tc.TableName, &tc.TableType,
			&tc.OrdinalPosition, &tc.ColumnName, &tc.ColumnComment,
			&tc.DataType, &tc.IsNullable, &tc.ColumnDefault,
		); err != nil {
			return nil, fmt.Errorf("failed to scan table with columns: %w", err)
		}

		// Check if we need to create a new table entry
		if currentTable == nil || currentTable.TableName != tc.TableName {
			tables = append(tables, driverbase.TableInfo{
				TableName: tc.TableName,
				TableType: tc.TableType,
			})
			currentTable = &tables[len(tables)-1]
		}

		// Process column data
		var radix sql.NullInt16
		var nullable sql.NullInt16

		// Set numeric precision radix (MySQL doesn't store this directly)
		dataType := strings.ToUpper(tc.DataType)
		switch dataType {
		// Binary radix (base 2)
		case "BIT":
			radix = sql.NullInt16{Int16: 2, Valid: true}

		// Decimal radix (base 10) - integer types
		case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT":
			radix = sql.NullInt16{Int16: 10, Valid: true}

		// Decimal radix (base 10) - decimal/numeric types
		case "DECIMAL", "DEC", "NUMERIC", "FIXED":
			radix = sql.NullInt16{Int16: 10, Valid: true}

		// Decimal radix (base 10) - floating point types
		case "FLOAT", "DOUBLE", "DOUBLE PRECISION", "REAL":
			radix = sql.NullInt16{Int16: 10, Valid: true}

		// Decimal radix (base 10) - year type
		case "YEAR":
			radix = sql.NullInt16{Int16: 10, Valid: true}

		// No radix for non-numeric types
		default:
			radix = sql.NullInt16{Valid: false}
		}

		// Set nullable information
		switch tc.IsNullable {
		case "YES":
			nullable = sql.NullInt16{Int16: int16(driverbase.XdbcColumnNullable), Valid: true}
		case "NO":
			nullable = sql.NullInt16{Int16: int16(driverbase.XdbcColumnNoNulls), Valid: true}
		}

		currentTable.TableColumns = append(currentTable.TableColumns, driverbase.ColumnInfo{
			ColumnName:       tc.ColumnName,
			OrdinalPosition:  &tc.OrdinalPosition,
			Remarks:          driverbase.NullStringToPtr(tc.ColumnComment),
			XdbcTypeName:     &tc.DataType,
			XdbcNumPrecRadix: driverbase.NullInt16ToPtr(radix),
			XdbcNullable:     driverbase.NullInt16ToPtr(nullable),
			XdbcIsNullable:   &tc.IsNullable,
			XdbcColumnDef:    driverbase.NullStringToPtr(tc.ColumnDefault),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during table with columns iteration: %w", err)
	}

	// TODO: Add constraint and foreign key metadata support

	return tables, nil
}
