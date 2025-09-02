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
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/go-sql-driver/mysql"
)

// mysqlBulkIngestImpl implements driverbase.BulkIngestImpl for MySQL using LOAD DATA INFILE
type mysqlBulkIngestImpl struct {
	conn          *sql.Conn
	typeConverter sqlwrapper.TypeConverter
	errorHelper   *driverbase.ErrorHelper
	options       *driverbase.BulkIngestOptions
}

// mysqlTempFile represents a temporary file ready for MySQL LOAD DATA INFILE
type mysqlTempFile struct {
	filePath string
	rows     int64
}

func (m *mysqlTempFile) String() string {
	return m.filePath
}

func (m *mysqlTempFile) Rows() int64 {
	return m.rows
}


// CreateSink creates a sink for storing Parquet data
func (impl *mysqlBulkIngestImpl) CreateSink(ctx context.Context, options *driverbase.BulkIngestOptions) (driverbase.BulkIngestSink, error) {
	// Use driverbase's in-memory buffer sink to avoid file handle issues
	return &driverbase.BufferBulkIngestSink{}, nil
}

// Upload for MySQL writes the buffer data to a temporary file
func (impl *mysqlBulkIngestImpl) Upload(ctx context.Context, chunk driverbase.BulkIngestPendingUpload) (driverbase.BulkIngestPendingCopy, error) {
	// Get the buffer data
	sink := chunk.Data.(*driverbase.BufferBulkIngestSink)
	
	// Write buffer to temporary file
	tempFile, err := os.CreateTemp("", "mysql_bulk_*.parquet")
	if err != nil {
		return nil, impl.errorHelper.IO("failed to create temp file: %v", err)
	}
	
	// Write the buffer content to the file
	if _, err := tempFile.Write(sink.Bytes()); err != nil {
		tempFile.Close()
		return nil, impl.errorHelper.IO("failed to write to temp file: %v", err)
	}
	
	// Ensure data is written to disk before closing
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		return nil, impl.errorHelper.IO("failed to sync temp file: %v", err)
	}
	
	// Close the file so it can be read later
	if err := tempFile.Close(); err != nil {
		return nil, impl.errorHelper.IO("failed to close temp file: %v", err)
	}
	
	return &mysqlTempFile{
		filePath: tempFile.Name(),
		rows:     chunk.Rows,
	}, nil
}

// CreateTable creates the target table based on Arrow schema
func (impl *mysqlBulkIngestImpl) CreateTable(ctx context.Context, schema *arrow.Schema, ifTableExists driverbase.BulkIngestTableExistsBehavior, ifTableMissing driverbase.BulkIngestTableMissingBehavior) error {
	stmt, err := impl.createTableStatement(schema, ifTableExists, ifTableMissing)
	if err != nil {
		return err
	}
	
	if stmt != "" {
		_, err := impl.conn.ExecContext(ctx, stmt)
		if err != nil {
			return impl.errorHelper.IO("failed to create table: %v", err)
		}
	}
	
	return nil
}

// createTableStatement generates MySQL CREATE TABLE DDL from Arrow schema
func (impl *mysqlBulkIngestImpl) createTableStatement(schema *arrow.Schema, ifTableExists driverbase.BulkIngestTableExistsBehavior, ifTableMissing driverbase.BulkIngestTableMissingBehavior) (string, error) {
	var b strings.Builder
	
	// Handle table existence behavior
	switch ifTableExists {
	case driverbase.BulkIngestTableExistsError:
		// Do nothing - let CREATE TABLE fail if exists
	case driverbase.BulkIngestTableExistsIgnore:
		// Do nothing - we'll use CREATE TABLE IF NOT EXISTS
	case driverbase.BulkIngestTableExistsDrop:
		b.WriteString("DROP TABLE IF EXISTS ")
		tableName := "target_table"
		if impl.options != nil && impl.options.TableName != "" {
			tableName = impl.options.TableName
		}
		b.WriteString(impl.quoteIdentifier(tableName))
		b.WriteString("; ")
	}
	
	// Handle table creation
	switch ifTableMissing {
	case driverbase.BulkIngestTableMissingError:
		// Do nothing - assume table exists
		return b.String(), nil
	case driverbase.BulkIngestTableMissingCreate:
		b.WriteString("CREATE TABLE ")
		if ifTableExists == driverbase.BulkIngestTableExistsIgnore {
			b.WriteString("IF NOT EXISTS ")
		}
		
		tableName := "target_table"
		if impl.options != nil && impl.options.TableName != "" {
			tableName = impl.options.TableName
		}
		b.WriteString(impl.quoteIdentifier(tableName))
		b.WriteString(" (")
		
		// Convert Arrow fields to MySQL columns
		for i, field := range schema.Fields() {
			if i > 0 {
				b.WriteString(", ")
			}
			
			b.WriteString(impl.quoteIdentifier(field.Name))
			b.WriteString(" ")
			
			mysqlType, err := impl.arrowTypeToMySQL(&field)
			if err != nil {
				return "", err
			}
			b.WriteString(mysqlType)
			
			if !field.Nullable {
				b.WriteString(" NOT NULL")
			}
		}
		
		b.WriteString(")")
	}
	
	return b.String(), nil
}

// arrowTypeToMySQL converts Arrow data types to MySQL types, handling MySQL-specific metadata
func (impl *mysqlBulkIngestImpl) arrowTypeToMySQL(field *arrow.Field) (string, error) {
	arrowType := field.Type
	
	// Handle MySQL-specific types based on metadata first
	switch arrowType.ID() {
	case arrow.STRING:
		// Check for MySQL JSON columns
		if isJSON, ok := field.Metadata.GetValue("mysql.is_json"); ok && isJSON == "true" {
			return "JSON", nil
		}
		// Check for MySQL ENUM/SET columns  
		if isEnumSet, ok := field.Metadata.GetValue("mysql.is_enum_set"); ok && isEnumSet == "true" {
			if dbType, ok := field.Metadata.GetValue("sql.database_type_name"); ok {
				return strings.ToUpper(dbType), nil // ENUM or SET
			}
		}
		return "TEXT", nil
		
	case arrow.BINARY:
		// Check for MySQL spatial columns
		if isSpatial, ok := field.Metadata.GetValue("mysql.is_spatial"); ok && isSpatial == "true" {
			if dbType, ok := field.Metadata.GetValue("sql.database_type_name"); ok {
				return strings.ToUpper(dbType), nil // GEOMETRY, POINT, etc.
			}
			return "GEOMETRY", nil // Default spatial type
		}
		return "BLOB", nil
	}
	
	// Handle standard Arrow types
	switch arrowType.ID() {
	case arrow.BOOL:
		return "BOOLEAN", nil
	case arrow.INT8:
		return "TINYINT", nil
	case arrow.INT16:
		return "SMALLINT", nil
	case arrow.INT32:
		return "INT", nil
	case arrow.INT64:
		return "BIGINT", nil
	case arrow.UINT8:
		return "TINYINT UNSIGNED", nil
	case arrow.UINT16:
		return "SMALLINT UNSIGNED", nil
	case arrow.UINT32:
		return "INT UNSIGNED", nil
	case arrow.UINT64:
		return "BIGINT UNSIGNED", nil
	case arrow.FLOAT32:
		return "FLOAT", nil
	case arrow.FLOAT64:
		return "DOUBLE", nil
	case arrow.DATE32:
		return "DATE", nil
	case arrow.TIME32, arrow.TIME64:
		return "TIME", nil
	case arrow.TIMESTAMP:
		ts := arrowType.(*arrow.TimestampType)
		if ts.TimeZone != "" {
			return "TIMESTAMP", nil // MySQL TIMESTAMP is timezone-aware
		}
		return "DATETIME", nil // MySQL DATETIME is timezone-naive
	case arrow.DECIMAL128, arrow.DECIMAL256:
		dec := arrowType.(arrow.DecimalType)
		return fmt.Sprintf("DECIMAL(%d, %d)", dec.GetPrecision(), dec.GetScale()), nil
	default:
		return "", impl.errorHelper.Errorf(adbc.StatusNotImplemented, "unsupported Arrow type: %s", arrowType)
	}
}

// quoteIdentifier quotes MySQL identifiers using backticks
func (impl *mysqlBulkIngestImpl) quoteIdentifier(identifier string) string {
	// Escape existing backticks by doubling them
	escaped := strings.ReplaceAll(identifier, "`", "``")
	return fmt.Sprintf("`%s`", escaped)
}

// Copy uses MySQL's LOAD DATA INFILE to bulk load data
func (impl *mysqlBulkIngestImpl) Copy(ctx context.Context, chunk driverbase.BulkIngestPendingCopy) error {
	tempFile := chunk.(*mysqlTempFile)
	
	// Convert Parquet file to CSV
	csvFile, err := impl.convertParquetToCSV(ctx, tempFile.filePath)
	if err != nil {
		return err
	}
	defer os.Remove(csvFile) // Clean up original CSV file
	
	// For Docker setup, we need to copy the file to MySQL's secure directory
	// Since we can't directly access the container filesystem, we'll still use LOCAL INFILE
	// but with proper file registration for security
	
	// Register the file for LOAD DATA LOCAL INFILE
	mysql.RegisterLocalFile(csvFile)
	defer mysql.DeregisterLocalFile(csvFile)
	
	// Use LOAD DATA LOCAL INFILE to bulk load the CSV data (reads from client side)
	loadSQL := fmt.Sprintf(`
		LOAD DATA LOCAL INFILE '%s'
		INTO TABLE %s
		FIELDS TERMINATED BY ',' 
		ENCLOSED BY '"' 
		LINES TERMINATED BY '\n'
		IGNORE 1 ROWS`, // Skip CSV header row
		csvFile,
		impl.getTableName())
	
	_, err = impl.conn.ExecContext(ctx, loadSQL)
	if err != nil {
		return impl.errorHelper.IO("failed to execute LOAD DATA LOCAL INFILE: %v", err)
	}
	
	return nil
}

// convertParquetToCSV converts a Parquet file to CSV format
func (impl *mysqlBulkIngestImpl) convertParquetToCSV(ctx context.Context, parquetFile string) (string, error) {
	// Check if file exists and get its size for debugging
	fileInfo, err := os.Stat(parquetFile)
	if err != nil {
		return "", impl.errorHelper.IO("failed to stat Parquet file %s: %v", parquetFile, err)
	}
	if fileInfo.Size() == 0 {
		return "", impl.errorHelper.IO("Parquet file %s is empty (0 bytes)", parquetFile)
	}
	
	// Open the Parquet file
	parquetReader, err := file.OpenParquetFile(parquetFile, false)
	if err != nil {
		return "", impl.errorHelper.IO("failed to open Parquet file %s (size: %d bytes): %v", parquetFile, fileInfo.Size(), err)
	}
	defer parquetReader.Close()
	
	// Create Arrow reader from Parquet
	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return "", impl.errorHelper.IO("failed to create Arrow reader from Parquet file %s: %v", parquetFile, err)
	}
	
	// Check number of row groups for debugging
	numRowGroups := arrowReader.ParquetReader().NumRowGroups()
	if numRowGroups == 0 {
		return "", impl.errorHelper.IO("Parquet file %s has no row groups", parquetFile)
	}
	
	// Get the Arrow schema
	arrowSchema, err := arrowReader.Schema()
	if err != nil {
		return "", impl.errorHelper.IO("failed to get Arrow schema: %v", err)
	}
	
	// Create temporary CSV file
	csvFile, err := os.CreateTemp("", "mysql_csv_*.csv")
	if err != nil {
		return "", impl.errorHelper.IO("failed to create CSV file: %v", err)
	}
	defer csvFile.Close()
	
	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()
	
	// Write CSV header
	header := make([]string, arrowSchema.NumFields())
	for i, field := range arrowSchema.Fields() {
		header[i] = field.Name
	}
	if err := csvWriter.Write(header); err != nil {
		return "", impl.errorHelper.IO("failed to write CSV header: %v", err)
	}
	
	// Try reading all data at once instead of streaming
	table, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return "", impl.errorHelper.IO("failed to read Parquet table from %s: %v", parquetFile, err)
	}
	defer table.Release()
	
	if table.NumRows() == 0 {
		return "", impl.errorHelper.IO("Parquet file %s contains no rows", parquetFile)
	}
	
	// Convert table to record batches
	tableReader := array.NewTableReader(table, 0) // 0 = read all rows at once
	defer tableReader.Release()
	
	recordCount := 0
	totalRows := int64(0)
	
	for tableReader.Next() {
		record := tableReader.Record()
		recordCount++
		numRows := record.NumRows()
		totalRows += numRows
		
		// Convert each row in the record to CSV
		for rowIdx := 0; rowIdx < int(numRows); rowIdx++ {
			row := make([]string, record.NumCols())
			
			for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
				arr := record.Column(colIdx)
				field := arrowSchema.Field(colIdx)
				
				// Use our type converter-based conversion
				csvValue, err := impl.arrowValueToString(arr, rowIdx, &field)
				if err != nil {
					return "", impl.errorHelper.IO("failed to convert value at record %d, row %d, col %d (%s): %v", 
						recordCount, rowIdx, colIdx, field.Name, err)
				}
				row[colIdx] = csvValue
			}
			
			if err := csvWriter.Write(row); err != nil {
				return "", impl.errorHelper.IO("failed to write CSV row %d: %v", rowIdx, err)
			}
		}
	}
	
	if err := tableReader.Err(); err != nil {
		return "", impl.errorHelper.IO("error reading table records from %s (processed %d records, %d rows): %v", 
			parquetFile, recordCount, totalRows, err)
	}
	
	return csvFile.Name(), nil
}

// arrowValueToString converts Arrow array values to CSV string format using type converter
func (impl *mysqlBulkIngestImpl) arrowValueToString(arr arrow.Array, index int, field *arrow.Field) (string, error) {
	if arr.IsNull(index) {
		return "\\N", nil // MySQL NULL representation in CSV
	}
	
	// Use the type converter to get the Go value with MySQL-specific handling
	goValue, err := impl.typeConverter.ConvertArrowToGo(arr, index, field)
	if err != nil {
		return "", err
	}
	
	// Convert Go value to CSV string representation for MySQL
	return impl.goValueToCSVString(goValue, field)
}

// goValueToCSVString converts a Go value to MySQL CSV string format
func (impl *mysqlBulkIngestImpl) goValueToCSVString(value any, field *arrow.Field) (string, error) {
	if value == nil {
		return "\\N", nil // MySQL NULL representation
	}
	
	// Handle MySQL-specific types based on field metadata
	switch field.Type.(type) {
	case *arrow.StringType:
		// Check for MySQL JSON columns  
		if isJSON, ok := field.Metadata.GetValue("mysql.is_json"); ok && isJSON == "true" {
			// JSON values are already formatted correctly by type converter
			return fmt.Sprintf("%v", value), nil
		}
		return fmt.Sprintf("%v", value), nil
		
	case *arrow.BinaryType:
		// Check for MySQL spatial columns
		if isSpatial, ok := field.Metadata.GetValue("mysql.is_spatial"); ok && isSpatial == "true" {
			// Spatial data should be hex-encoded for CSV
			if bytes, ok := value.([]byte); ok {
				return fmt.Sprintf("0x%x", bytes), nil
			}
		}
		// Regular binary data
		if bytes, ok := value.([]byte); ok {
			return fmt.Sprintf("0x%x", bytes), nil
		}
		return fmt.Sprintf("0x%x", []byte(fmt.Sprintf("%v", value))), nil
		
	case *arrow.BooleanType:
		// MySQL boolean representation in CSV
		if b, ok := value.(bool); ok && b {
			return "1", nil
		}
		return "0", nil
		
	default:
		// For all other types (including timestamps handled by type converter), use string representation
		return fmt.Sprintf("%v", value), nil
	}
}

// Delete removes temporary files
func (impl *mysqlBulkIngestImpl) Delete(ctx context.Context, chunk driverbase.BulkIngestPendingCopy) error {
	tempFile := chunk.(*mysqlTempFile)
	return os.Remove(tempFile.filePath)
}

// SetOptions updates the bulk ingest options (called by driverbase)
func (impl *mysqlBulkIngestImpl) SetOptions(options *driverbase.BulkIngestOptions) {
	impl.options = options
}

// getTableName returns the target table name, with fallback
func (impl *mysqlBulkIngestImpl) getTableName() string {
	if impl.options != nil && impl.options.TableName != "" {
		return impl.quoteIdentifier(impl.options.TableName)
	}
	return impl.quoteIdentifier("target_table")
}

// mysqlBulkIngestFactory creates MySQL bulk ingest implementations
type mysqlBulkIngestFactory struct{}

// CreateBulkIngestImpl implements sqlwrapper.BulkIngestImplFactory
func (f *mysqlBulkIngestFactory) CreateBulkIngestImpl(conn *sql.Conn, typeConverter sqlwrapper.TypeConverter, errorHelper *driverbase.ErrorHelper, options *driverbase.BulkIngestOptions) driverbase.BulkIngestImpl {
	return &mysqlBulkIngestImpl{
		conn:          conn,
		typeConverter: typeConverter,
		errorHelper:   errorHelper,
		options:       options,
	}
}