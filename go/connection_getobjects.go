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
	"path/filepath"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func (c *mysqlConnectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	// MySQL has no real schema concept; we model it as a single empty-string schema.
	// If the caller filters on a schema that doesn't match "", return catalogs only.
	includeSchemas := true
	if dbSchema != nil {
		matches, err := filepath.Match(*dbSchema, "")
		if err != nil {
			return nil, c.ErrorHelper.WrapInvalidArgument(err, "invalid schema filter pattern")
		}
		if !matches {
			includeSchemas = false
		}
	}

	// Determine effective depth: if schema filter doesn't match, cap at catalogs.
	effectiveDepth := depth
	if !includeSchemas && effectiveDepth != adbc.ObjectDepthCatalogs {
		effectiveDepth = adbc.ObjectDepthCatalogs
	}

	// Build a single query: SCHEMATA LEFT JOIN TABLES LEFT JOIN COLUMNS.
	// Deeper levels are disabled with AND 1=0 in the join condition.
	includeTables := effectiveDepth == adbc.ObjectDepthTables || effectiveDepth == adbc.ObjectDepthColumns
	includeColumns := effectiveDepth == adbc.ObjectDepthColumns

	var queryBuilder strings.Builder
	args := []any{}

	queryBuilder.WriteString(`
		SELECT
			s.SCHEMA_NAME,
			t.TABLE_NAME,
			t.TABLE_TYPE,
			c.ORDINAL_POSITION,
			c.COLUMN_NAME,
			c.COLUMN_COMMENT,
			c.DATA_TYPE,
			c.COLUMN_TYPE,
			c.IS_NULLABLE,
			c.COLUMN_DEFAULT
		FROM INFORMATION_SCHEMA.SCHEMATA s
		LEFT JOIN INFORMATION_SCHEMA.TABLES t
			ON s.SCHEMA_NAME = t.TABLE_SCHEMA`)

	if !includeTables {
		queryBuilder.WriteString(` AND 1=0`)
	} else {
		if tableName != nil {
			queryBuilder.WriteString(` AND t.TABLE_NAME LIKE ?`)
			args = append(args, *tableName)
		}
		if len(tableType) > 0 {
			queryBuilder.WriteString(` AND t.TABLE_TYPE IN (` + placeholders(len(tableType)) + `)`)
			for _, tt := range tableType {
				args = append(args, tt)
			}
		}
	}

	queryBuilder.WriteString(`
		LEFT JOIN INFORMATION_SCHEMA.COLUMNS c
			ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
			AND t.TABLE_NAME = c.TABLE_NAME`)

	if !includeColumns {
		queryBuilder.WriteString(` AND 1=0`)
	} else if columnName != nil {
		queryBuilder.WriteString(` AND c.COLUMN_NAME LIKE ?`)
		args = append(args, *columnName)
	}

	if catalog != nil {
		queryBuilder.WriteString(` WHERE s.SCHEMA_NAME LIKE ?`)
		args = append(args, *catalog)
	}

	queryBuilder.WriteString(` ORDER BY s.SCHEMA_NAME, t.TABLE_NAME, c.ORDINAL_POSITION`)

	rows, err := c.Conn.QueryContext(ctx, queryBuilder.String(), args...)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to query objects")
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	// Group rows into the GetObjectsInfo hierarchy.
	var infos []driverbase.GetObjectsInfo
	var currentInfo *driverbase.GetObjectsInfo
	var currentTable *driverbase.TableInfo

	includeDbSchemas := effectiveDepth != adbc.ObjectDepthCatalogs

	for rows.Next() {
		var (
			schema          string
			tblName         sql.NullString
			tblType         sql.NullString
			ordinalPosition sql.NullInt32
			colName         sql.NullString
			colComment      sql.NullString
			dataType        sql.NullString
			colType         sql.NullString
			isNullable      sql.NullString
			colDefault      sql.NullString
		)

		if err := rows.Scan(
			&schema, &tblName, &tblType,
			&ordinalPosition, &colName, &colComment,
			&dataType, &colType, &isNullable, &colDefault,
		); err != nil {
			return nil, c.ErrorHelper.WrapIO(err, "failed to scan objects row")
		}

		// New catalog?
		if currentInfo == nil || *currentInfo.CatalogName != schema {
			info := driverbase.GetObjectsInfo{CatalogName: driverbase.Nullable(schema)}
			if includeDbSchemas {
				info.CatalogDbSchemas = []driverbase.DBSchemaInfo{{DbSchemaName: driverbase.Nullable("")}}
			}
			infos = append(infos, info)
			currentInfo = &infos[len(infos)-1]
			currentTable = nil
		}

		if !tblName.Valid {
			continue
		}

		// New table?
		tables := &currentInfo.CatalogDbSchemas[0].DbSchemaTables
		if currentTable == nil || currentTable.TableName != tblName.String {
			*tables = append(*tables, driverbase.TableInfo{
				TableName: tblName.String,
				TableType: tblType.String,
			})
			currentTable = &(*tables)[len(*tables)-1]
		}

		if !colName.Valid {
			continue
		}

		currentTable.TableColumns = append(currentTable.TableColumns,
			buildColumnInfo(dataType.String, colType.String, colName.String, isNullable.String,
				ordinalPosition.Int32, colComment, colDefault))
	}

	if err := rows.Err(); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "error during objects iteration")
	}

	return buildResult(c, infos)
}

// buildColumnInfo constructs a ColumnInfo from raw MySQL column metadata.
func buildColumnInfo(dataType, columnType, columnName, isNullable string, ordinalPosition int32, columnComment, columnDefault sql.NullString) driverbase.ColumnInfo {
	var radix sql.NullInt16
	var nullable sql.NullInt16

	// Build the full type name including UNSIGNED if applicable
	// Only check integer types to avoid false positives with enum/set value lists
	xdbcTypeName := dataType
	switch strings.ToUpper(dataType) {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT":
		if strings.Contains(strings.ToUpper(columnType), "UNSIGNED") {
			xdbcTypeName = dataType + " UNSIGNED"
		}
	}

	// Set numeric precision radix (MySQL doesn't store this directly)
	switch strings.ToUpper(dataType) {
	case "BIT":
		radix = sql.NullInt16{Int16: 2, Valid: true}
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT",
		"DECIMAL", "DEC", "NUMERIC", "FIXED",
		"FLOAT", "DOUBLE", "DOUBLE PRECISION", "REAL",
		"YEAR":
		radix = sql.NullInt16{Int16: 10, Valid: true}
	default:
		radix = sql.NullInt16{Valid: false}
	}

	// Set nullable information
	switch isNullable {
	case "YES":
		nullable = sql.NullInt16{Int16: int16(driverbase.XdbcColumnNullable), Valid: true}
	case "NO":
		nullable = sql.NullInt16{Int16: int16(driverbase.XdbcColumnNoNulls), Valid: true}
	}

	return driverbase.ColumnInfo{
		ColumnName:       columnName,
		OrdinalPosition:  &ordinalPosition,
		Remarks:          driverbase.NullStringToPtr(columnComment),
		XdbcTypeName:     &xdbcTypeName,
		XdbcNumPrecRadix: driverbase.NullInt16ToPtr(radix),
		XdbcNullable:     driverbase.NullInt16ToPtr(nullable),
		XdbcIsNullable:   &isNullable,
		XdbcColumnDef:    driverbase.NullStringToPtr(columnDefault),
	}
}

// placeholders returns a comma-separated string of n question marks.
func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat("?,", n-1) + "?"
}

// buildResult feeds GetObjectsInfo entries into BuildGetObjectsRecordReader.
func buildResult(c *mysqlConnectionImpl, infos []driverbase.GetObjectsInfo) (array.RecordReader, error) {
	ch := make(chan driverbase.GetObjectsInfo, len(infos))
	for _, info := range infos {
		ch <- info
	}
	close(ch)

	errCh := make(chan error, 1)
	close(errCh)

	return driverbase.BuildGetObjectsRecordReader(c.Alloc, ch, errCh)
}
