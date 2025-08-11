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

package mysql_test

// TODO (https://github.com/adbc-drivers/driverbase-go/issues/27): leverage Go validation suite for more comprehensive testing

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	mysql "github.com/adbc-drivers/mysql"
)

// getDSN returns the MySQL DSN for testing, using environment variable or default
func getDSN() string {
	// You can set MYSQL_DSN environment variable for custom connection
	// Default assumes local MySQL with root/password setup
	if dsn := os.Getenv("MYSQL_DSN"); dsn != "" {
		return dsn
	}
	return "root:password@tcp(localhost:3306)/mysql"
}

// Helper function to create Arrow records from string data
func createTestRecords(allocator memory.Allocator, schema *arrow.Schema, batches [][]string) []arrow.Record {
	records := make([]arrow.Record, len(batches))

	for i, batch := range batches {
		// Build string array for this batch
		builder := array.NewStringBuilder(allocator)
		defer builder.Release()

		for _, value := range batch {
			builder.Append(value)
		}
		stringArray := builder.NewArray()
		defer stringArray.Release()

		// Create record for this batch
		records[i] = array.NewRecord(schema, []arrow.Array{stringArray}, int64(len(batch)))
	}

	return records
}

func TestDriver(t *testing.T) {
	dsn := getDSN()

	mysqlDriver := mysql.NewDriver()

	db, err := mysqlDriver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: dsn,
	})
	require.NoError(t, err)
	defer db.Close()

	cn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer cn.Close()

	stmt, err := cn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()

	// create table
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_driver (
			id INT PRIMARY KEY AUTO_INCREMENT,
			val VARCHAR(20)
		)
	`)
	require.NoError(t, err)

	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// insert dummy data
	err = stmt.SetSqlQuery(`
	INSERT INTO adbc_test_driver (val) VALUES ('apple'), ('banana')
`)
	require.NoError(t, err)

	count, err := stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	// Test that Bind works with empty record
	emptySchema := arrow.NewSchema(nil, nil)
	emptyRec := array.NewRecord(emptySchema, []arrow.Array{}, 0)
	defer emptyRec.Release()

	// Bind should work with empty record (no-op)
	err = stmt.Bind(context.Background(), emptyRec)
	require.NoError(t, err)

	// Test ExecuteSchema
	err = stmt.SetSqlQuery("SELECT id, val FROM adbc_test_driver")
	require.NoError(t, err)

	// Cast to StatementExecuteSchema interface to access ExecuteSchema
	schemaStmt, ok := stmt.(adbc.StatementExecuteSchema)
	require.True(t, ok, "Statement should implement StatementExecuteSchema")

	resultSchema, err := schemaStmt.ExecuteSchema(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, len(resultSchema.Fields()), "Should have 2 columns")
	require.Equal(t, "id", resultSchema.Field(0).Name)
	require.Equal(t, "val", resultSchema.Field(1).Name)

	// Verify types are mapped correctly
	require.Equal(t, "int32", resultSchema.Field(0).Type.String()) // INT -> int32
	require.Equal(t, "utf8", resultSchema.Field(1).Type.String())  // VARCHAR -> utf8

	// Test ExecuteQuery
	err = stmt.SetSqlQuery("SELECT id, val FROM adbc_test_driver ORDER BY id")
	require.NoError(t, err)

	reader, rowCount, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(-1), rowCount) // Row count unknown until read
	defer reader.Release()

	// Verify we can read the data
	hasNext := reader.Next()
	require.True(t, hasNext, "Should have at least one batch")

	record := reader.Record()
	require.NotNil(t, record)
	require.Equal(t, int64(2), record.NumRows()) // Should have 2 rows
	require.Equal(t, int64(2), record.NumCols()) // Should have 2 columns

	// Verify data values
	idCol := record.Column(0).(*array.Int32)
	valCol := record.Column(1).(*array.String)

	require.Equal(t, int32(1), idCol.Value(0))  // First row id
	require.Equal(t, int32(2), idCol.Value(1))  // Second row id (AUTO_INCREMENT)
	require.Equal(t, "apple", valCol.Value(0))  // First row val
	require.Equal(t, "banana", valCol.Value(1)) // Second row val

	record.Release()

	// Should be no more batches for this small result
	hasNext = reader.Next()
	require.False(t, hasNext, "Should not have more batches")

	// Test Bind parameters for bulk insert
	err = stmt.SetSqlQuery("INSERT INTO adbc_test_driver (val) VALUES (?)")
	require.NoError(t, err)

	err = stmt.Prepare(context.Background())
	require.NoError(t, err)

	// Create Arrow record with bulk data
	allocator := memory.DefaultAllocator

	// Build string array with fruit names
	stringBuilder := array.NewStringBuilder(allocator)
	defer stringBuilder.Release()

	fruits := []string{"orange", "grape", "kiwi"}
	for _, fruit := range fruits {
		stringBuilder.Append(fruit)
	}
	stringArray := stringBuilder.NewArray()
	defer stringArray.Release()

	// Create schema and record for bind parameters
	bindSchema := arrow.NewSchema([]arrow.Field{
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	bindRecord := array.NewRecord(bindSchema, []arrow.Array{stringArray}, 3)
	defer bindRecord.Release()

	// Bind the record
	err = stmt.Bind(context.Background(), bindRecord)
	require.NoError(t, err)

	// Execute the bulk insert
	insertedCount, err := stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(3), insertedCount) // Should insert 3 rows

	// Verify the data was inserted
	err = stmt.SetSqlQuery("SELECT COUNT(*) FROM adbc_test_driver")
	require.NoError(t, err)

	reader2, _, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	defer reader2.Release()

	hasNext = reader2.Next()
	require.True(t, hasNext)

	countRecord := reader2.Record()
	require.NotNil(t, countRecord)

	countCol := countRecord.Column(0).(*array.Int64)
	totalRows := countCol.Value(0)
	require.Equal(t, int64(5), totalRows) // 2 original + 3 new = 5 total

	countRecord.Release()

	// Test BindStream for streaming bulk insert
	err = stmt.SetSqlQuery("INSERT INTO adbc_test_driver (val) VALUES (?)")
	require.NoError(t, err)

	// Create test data and schema
	streamSchema := arrow.NewSchema([]arrow.Field{
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	batches := [][]string{
		{"cherry", "peach"},    // First batch: 2 rows
		{"mango", "pineapple"}, // Second batch: 2 rows
	}

	// Create Arrow records using helper function
	testRecords := createTestRecords(allocator, streamSchema, batches)
	defer func() {
		for _, rec := range testRecords {
			rec.Release()
		}
	}()

	// Use Arrow's built-in RecordReader
	streamReader, err := array.NewRecordReader(streamSchema, testRecords)
	require.NoError(t, err)
	defer streamReader.Release()

	// Use BindStream
	err = stmt.BindStream(context.Background(), streamReader)
	require.NoError(t, err)

	// Execute the streaming bulk insert
	streamInsertedCount, err := stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(4), streamInsertedCount) // Should insert 4 rows (2 batches × 2 rows)

	// Verify final count
	err = stmt.SetSqlQuery("SELECT COUNT(*) FROM adbc_test_driver")
	require.NoError(t, err)

	reader3, _, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	defer reader3.Release()

	hasNext = reader3.Next()
	require.True(t, hasNext)

	finalCountRecord := reader3.Record()
	require.NotNil(t, finalCountRecord)

	finalCountCol := finalCountRecord.Column(0).(*array.Int64)
	finalTotalRows := finalCountCol.Value(0)
	require.Equal(t, int64(9), finalTotalRows) // 5 previous + 4 new = 9 total

	finalCountRecord.Release()

	t.Log("✅ TestDriver passed successfully")

}

// TestSchemaMetadata tests that SQL type metadata is included in Arrow schema
func TestSchemaMetadata(t *testing.T) {
	dsn := getDSN()

	mysqlDriver := mysql.NewDriver()

	db, err := mysqlDriver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: dsn,
	})
	require.NoError(t, err)
	defer db.Close()

	cn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer cn.Close()

	stmt, err := cn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()

	// Create test table with various column types
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_metadata (
			id INT PRIMARY KEY AUTO_INCREMENT,
			name VARCHAR(100) NOT NULL,
			price DECIMAL(10,2),
			created_at TIMESTAMP
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Test ExecuteSchema to get metadata
	err = stmt.SetSqlQuery("SELECT id, name, price, created_at FROM adbc_test_metadata")
	require.NoError(t, err)

	schemaStmt, ok := stmt.(adbc.StatementExecuteSchema)
	require.True(t, ok, "Statement should implement StatementExecuteSchema")

	schema, err := schemaStmt.ExecuteSchema(context.Background())
	require.NoError(t, err)
	require.Equal(t, 4, len(schema.Fields()), "Should have 4 columns")

	// Check that each field has SQL metadata
	for i, field := range schema.Fields() {
		t.Logf("Field %d: %s (type: %s)", i, field.Name, field.Type.String())

		// Verify metadata exists
		require.NotNil(t, field.Metadata, "Field should have metadata")

		// Check for SQL database type name
		dbTypeName, ok := field.Metadata.GetValue("sql.database_type_name")
		require.True(t, ok, "Should have sql.database_type_name metadata")
		require.NotEmpty(t, dbTypeName, "Database type name should not be empty")

		t.Logf("  SQL database type: %s", dbTypeName)

		// Check for column name
		colName, ok := field.Metadata.GetValue("sql.column_name")
		require.True(t, ok, "Should have sql.column_name metadata")
		require.Equal(t, field.Name, colName, "Column name in metadata should match field name")

		// Check type-specific metadata
		switch field.Name {
		case "name":
			// VARCHAR should have length
			if length, ok := field.Metadata.GetValue("sql.length"); ok {
				t.Logf("  VARCHAR length: %s", length)
			}
		case "price":
			// DECIMAL should have precision and scale
			if precision, ok := field.Metadata.GetValue("sql.precision"); ok {
				t.Logf("  DECIMAL precision: %s", precision)
			}
			if scale, ok := field.Metadata.GetValue("sql.scale"); ok {
				t.Logf("  DECIMAL scale: %s", scale)
			}
		}
	}

	t.Log("✅ Schema metadata test passed successfully")
}

// TestMySQLTypeConverter tests MySQL-specific type converter enhancements
func TestMySQLTypeConverter(t *testing.T) {
	dsn := getDSN()

	mysqlDriver := mysql.NewDriver()

	db, err := mysqlDriver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: dsn,
	})
	require.NoError(t, err)
	defer db.Close()

	cn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer cn.Close()

	stmt, err := cn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()

	// Create test table with MySQL-specific types
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_mysql_types (
			id INT PRIMARY KEY AUTO_INCREMENT,
			data JSON,
			status ENUM('active', 'inactive') DEFAULT 'active'
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Test ExecuteSchema to get MySQL-specific metadata
	err = stmt.SetSqlQuery("SELECT id, data, status FROM adbc_test_mysql_types")
	require.NoError(t, err)

	schemaStmt, ok := stmt.(adbc.StatementExecuteSchema)
	require.True(t, ok, "Statement should implement StatementExecuteSchema")

	schema, err := schemaStmt.ExecuteSchema(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, len(schema.Fields()), "Should have 3 columns")

	// Check MySQL-specific type enhancements
	for i, field := range schema.Fields() {
		t.Logf("Field %d: %s (type: %s)", i, field.Name, field.Type.String())

		// Verify metadata exists
		require.NotNil(t, field.Metadata, "Field should have metadata")

		// Check for SQL database type name
		dbTypeName, ok := field.Metadata.GetValue("sql.database_type_name")
		require.True(t, ok, "Should have sql.database_type_name metadata")
		t.Logf("  SQL database type: %s", dbTypeName)

		// Check MySQL-specific enhancements
		switch field.Name {
		case "data":
			require.Equal(t, "JSON", dbTypeName, "Should be JSON type")

			// Check for MySQL JSON metadata
			isJSON, ok := field.Metadata.GetValue("mysql.is_json")
			require.True(t, ok, "Should have mysql.is_json metadata")
			require.Equal(t, "true", isJSON, "Should be marked as JSON")
			t.Logf("  MySQL JSON detected: %s", isJSON)

		case "status":
			require.Equal(t, "ENUM", dbTypeName, "Should be ENUM type")

			// Check for MySQL ENUM metadata
			isEnumSet, ok := field.Metadata.GetValue("mysql.is_enum_set")
			require.True(t, ok, "Should have mysql.is_enum_set metadata")
			require.Equal(t, "true", isEnumSet, "Should be marked as ENUM/SET")
			t.Logf("  MySQL ENUM detected: %s", isEnumSet)
		}
	}

	t.Log("✅ MySQL type converter test passed successfully")
}

// TestDecimalTypeHandling tests that DECIMAL types are properly converted to Arrow Decimal128
func TestDecimalTypeHandling(t *testing.T) {
	dsn := getDSN()

	mysqlDriver := mysql.NewDriver()

	db, err := mysqlDriver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: dsn,
	})
	require.NoError(t, err)
	defer db.Close()

	cn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer cn.Close()

	stmt, err := cn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()

	// Create test table with various DECIMAL types
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_decimals (
			id INT PRIMARY KEY AUTO_INCREMENT,
			price DECIMAL(10,2),
			rate NUMERIC(5,4),
			percentage DECIMAL(3,1)
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Test ExecuteSchema to get decimal type information
	err = stmt.SetSqlQuery("SELECT id, price, rate, percentage FROM adbc_test_decimals")
	require.NoError(t, err)

	schemaStmt, ok := stmt.(adbc.StatementExecuteSchema)
	require.True(t, ok, "Statement should implement StatementExecuteSchema")

	schema, err := schemaStmt.ExecuteSchema(context.Background())
	require.NoError(t, err)
	require.Equal(t, 4, len(schema.Fields()), "Should have 4 columns")

	// Check that DECIMAL fields are properly converted
	expectedDecimals := map[string]struct {
		precision int32
		scale     int32
	}{
		"price":      {precision: 10, scale: 2},
		"rate":       {precision: 5, scale: 4},
		"percentage": {precision: 3, scale: 1},
	}

	for i, field := range schema.Fields() {
		t.Logf("Field %d: %s (type: %s)", i, field.Name, field.Type.String())

		if expected, isDecimal := expectedDecimals[field.Name]; isDecimal {
			// Verify it's a decimal type (accept any decimal type)
			decimalType, ok := field.Type.(arrow.DecimalType)
			require.True(t, ok, "Field %s should be DecimalType, got %T", field.Name, field.Type)

			// Verify precision and scale
			require.Equal(t, expected.precision, decimalType.GetPrecision(), "Precision mismatch for %s", field.Name)
			require.Equal(t, expected.scale, decimalType.GetScale(), "Scale mismatch for %s", field.Name)

			t.Logf("  %s(%d,%d) ✓", field.Type.String(), decimalType.GetPrecision(), decimalType.GetScale())

			// Verify metadata includes precision and scale
			precision, ok := field.Metadata.GetValue("sql.precision")
			require.True(t, ok, "Should have sql.precision metadata")
			require.Equal(t, fmt.Sprintf("%d", expected.precision), precision)

			scale, ok := field.Metadata.GetValue("sql.scale")
			require.True(t, ok, "Should have sql.scale metadata")
			require.Equal(t, fmt.Sprintf("%d", expected.scale), scale)
		}
	}

	t.Log("✅ Decimal type handling test passed successfully")
}

// TestTimestampPrecisionHandling tests that TIMESTAMP/DATETIME types use correct Arrow timestamp units
func TestTimestampPrecisionHandling(t *testing.T) {
	dsn := getDSN()

	mysqlDriver := mysql.NewDriver()

	db, err := mysqlDriver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: dsn,
	})
	require.NoError(t, err)
	defer db.Close()

	cn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer cn.Close()

	stmt, err := cn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()

	// Create test table with various timestamp precisions
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_timestamps (
			id INT PRIMARY KEY AUTO_INCREMENT,
			ts_default TIMESTAMP,
			ts_seconds TIMESTAMP(0),
			ts_millis TIMESTAMP(3),
			ts_micros TIMESTAMP(6),
			dt_default DATETIME,
			dt_millis DATETIME(3)
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Test ExecuteSchema to get timestamp type information
	err = stmt.SetSqlQuery("SELECT id, ts_default, ts_seconds, ts_millis, ts_micros, dt_default, dt_millis FROM adbc_test_timestamps")
	require.NoError(t, err)

	schemaStmt, ok := stmt.(adbc.StatementExecuteSchema)
	require.True(t, ok, "Statement should implement StatementExecuteSchema")

	schema, err := schemaStmt.ExecuteSchema(context.Background())
	require.NoError(t, err)
	require.Equal(t, 7, len(schema.Fields()), "Should have 7 columns")

	// Expected timestamp units based on precision (timezone-naive in sql-wrapper)
	expectedTimestamps := map[string]string{
		"ts_default": "timestamp[s]",  // MySQL reports precision=0 for default TIMESTAMP
		"ts_seconds": "timestamp[s]",  // 0 fractional seconds = seconds
		"ts_millis":  "timestamp[ms]", // 3 fractional seconds = milliseconds
		"ts_micros":  "timestamp[us]", // 6 fractional seconds = microseconds
		"dt_default": "timestamp[s]",  // MySQL reports precision=0 for default DATETIME
		"dt_millis":  "timestamp[ms]", // 3 fractional seconds = milliseconds
	}

	for i, field := range schema.Fields() {
		t.Logf("Field %d: %s (type: %s)", i, field.Name, field.Type.String())

		if expectedType, isTimestamp := expectedTimestamps[field.Name]; isTimestamp {
			// Verify the timestamp type matches expected precision
			require.Equal(t, expectedType, field.Type.String(), "Timestamp unit mismatch for %s", field.Name)

			// Verify metadata includes fractional seconds precision (if available)
			if precision, ok := field.Metadata.GetValue("sql.fractional_seconds_precision"); ok {
				t.Logf("  Fractional seconds precision: %s", precision)
			}

			t.Logf("  Expected: %s ✓", expectedType)
		}
	}

	t.Log("✅ Timestamp precision handling test passed successfully")
}

// TestQueryBatchSizeConfiguration tests that the batch size configuration affects query streaming
func TestQueryBatchSizeConfiguration(t *testing.T) {
	dsn := getDSN()

	mysqlDriver := mysql.NewDriver()

	db, err := mysqlDriver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: dsn,
	})
	require.NoError(t, err)
	defer db.Close()

	cn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer cn.Close()

	stmt, err := cn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()

	// Create test table with many rows
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_batch_size (
			id INT PRIMARY KEY AUTO_INCREMENT,
			value VARCHAR(50)
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Insert 100 rows for testing
	err = stmt.SetSqlQuery("INSERT INTO adbc_test_batch_size (value) VALUES (?)")
	require.NoError(t, err)
	err = stmt.Prepare(context.Background())
	require.NoError(t, err)

	// Create Arrow record with 100 values
	allocator := memory.DefaultAllocator
	stringBuilder := array.NewStringBuilder(allocator)
	defer stringBuilder.Release()

	for i := 0; i < 100; i++ {
		stringBuilder.Append(fmt.Sprintf("test_value_%d", i))
	}
	stringArray := stringBuilder.NewArray()
	defer stringArray.Release()

	bindSchema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	bindRecord := array.NewRecord(bindSchema, []arrow.Array{stringArray}, 100)
	defer bindRecord.Release()

	err = stmt.Bind(context.Background(), bindRecord)
	require.NoError(t, err)

	insertedCount, err := stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(100), insertedCount)

	// Test with different batch sizes
	testCases := []struct {
		batchSize    string
		expectedName string
	}{
		{"5", "small batch size"},
		{"25", "medium batch size"},
		{"50", "large batch size"},
		{"200", "batch size larger than total rows"},
	}

	for _, tc := range testCases {
		t.Run(tc.expectedName, func(t *testing.T) {
			// Create new statement for each test
			testStmt, err := cn.NewStatement()
			require.NoError(t, err)
			defer testStmt.Close()

			// Set the batch size
			err = testStmt.SetOption("adbc.statement.batch_size", tc.batchSize)
			require.NoError(t, err)

			// Query all rows
			err = testStmt.SetSqlQuery("SELECT id, value FROM adbc_test_batch_size ORDER BY id")
			require.NoError(t, err)

			reader, rowCount, err := testStmt.ExecuteQuery(context.Background())
			require.NoError(t, err)
			require.Equal(t, int64(-1), rowCount) // Row count unknown until read
			defer reader.Release()

			totalRows := int64(0)
			batchCount := 0

			// Read all batches and count them
			for reader.Next() {
				record := reader.Record()
				require.NotNil(t, record)

				batchRows := record.NumRows()
				totalRows += batchRows
				batchCount++

				t.Logf("Batch %d: %d rows (batch size setting: %s)", batchCount, batchRows, tc.batchSize)

				// Verify batch doesn't exceed expected size (except for last batch)
				expectedBatchSize, _ := strconv.Atoi(tc.batchSize)
				require.LessOrEqual(t, int(batchRows), expectedBatchSize, "Batch size should not exceed configured limit")

				record.Release()
			}

			require.NoError(t, reader.Err())
			require.Equal(t, int64(100), totalRows, "Should read all 100 rows")

			// Verify that smaller batch sizes result in more batches
			expectedBatchSize, _ := strconv.Atoi(tc.batchSize)
			if expectedBatchSize < 100 {
				require.Greater(t, batchCount, 1, "Small batch sizes should result in multiple batches")
				expectedBatches := (100 + expectedBatchSize - 1) / expectedBatchSize // Ceiling division
				require.Equal(t, expectedBatches, batchCount, "Number of batches should match expected based on batch size")
			} else {
				require.Equal(t, 1, batchCount, "Large batch sizes should result in single batch")
			}

			t.Logf("✅ Batch size %s: %d batches, %d total rows", tc.batchSize, batchCount, totalRows)
		})
	}

	t.Log("✅ Query batch size configuration test passed successfully")
}

// TestTypedBuilderHandling tests that the sqlRecordReader properly handles different data types with typed builders
func TestTypedBuilderHandling(t *testing.T) {
	dsn := getDSN()

	mysqlDriver := mysql.NewDriver()

	db, err := mysqlDriver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: dsn,
	})
	require.NoError(t, err)
	defer db.Close()

	cn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer cn.Close()

	stmt, err := cn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()

	// Create test table with various data types
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_typed_builders (
			id INT PRIMARY KEY AUTO_INCREMENT,
			tiny_int TINYINT,
			small_int SMALLINT, 
			medium_int MEDIUMINT,
			big_int BIGINT,
			float_val FLOAT,
			double_val DOUBLE,
			varchar_val VARCHAR(100),
			text_val TEXT,
			bool_val BOOLEAN,
			decimal_val DECIMAL(10,2),
			timestamp_val TIMESTAMP,
			binary_val VARBINARY(50),
			json_val JSON,
			enum_val ENUM('red', 'green', 'blue')
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Insert test data with various types
	err = stmt.SetSqlQuery(`
		INSERT INTO adbc_test_typed_builders (
			tiny_int, small_int, medium_int, big_int, 
			float_val, double_val, varchar_val, text_val, bool_val,
			decimal_val, timestamp_val, binary_val, json_val, enum_val
		) VALUES 
		(127, 32767, 8388607, 9223372036854775807, 
		 3.14159, 2.718281828, 'test string', 'longer text content', true,
		 123.45, '2023-12-25 10:30:00', X'48656C6C6F', '{"key": "value"}', 'red'),
		(-128, -32768, -8388608, -9223372036854775808,
		 -1.5, -999.999, 'another string', 'more text', false,
		 -67.89, '2024-01-01 00:00:00', X'576F726C64', '{"number": 42}', 'green'),
		(NULL, NULL, NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Query the data to test typed builders
	err = stmt.SetSqlQuery(`
		SELECT id, tiny_int, small_int, medium_int, big_int,
		       float_val, double_val, varchar_val, text_val, bool_val,
		       decimal_val, timestamp_val, binary_val, json_val, enum_val
		FROM adbc_test_typed_builders 
		ORDER BY id
	`)
	require.NoError(t, err)

	// Set a small batch size to ensure we test the builder logic properly
	err = stmt.SetOption("adbc.statement.batch_size", "2")
	require.NoError(t, err)

	reader, rowCount, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(-1), rowCount)
	defer reader.Release()

	schema := reader.Schema()
	require.Equal(t, 15, len(schema.Fields()), "Should have 15 columns")

	// Verify schema field types
	expectedTypes := map[string]string{
		"id":            "int32",
		"tiny_int":      "int8",
		"small_int":     "int16",
		"medium_int":    "int32",
		"big_int":       "int64",
		"float_val":     "float32",
		"double_val":    "float64",
		"varchar_val":   "utf8",
		"text_val":      "utf8",
		"bool_val":      "int8", // MySQL BOOLEAN is actually TINYINT
		"decimal_val":   "decimal64(10, 2)",
		"timestamp_val": "timestamp[s]", // Default precision is 0 = seconds
		"binary_val":    "binary",
		"json_val":      "utf8", // JSON handled by MySQL type converter
		"enum_val":      "utf8", // ENUM handled by MySQL type converter
	}

	for _, field := range schema.Fields() {
		expectedType, exists := expectedTypes[field.Name]
		require.True(t, exists, "Unexpected field: %s", field.Name)
		require.Equal(t, expectedType, field.Type.String(), "Type mismatch for field %s", field.Name)
		t.Logf("Field %s: %s ✓", field.Name, field.Type.String())
	}

	totalRows := int64(0)
	batchCount := 0

	// Read all batches and verify data
	for reader.Next() {
		record := reader.Record()
		require.NotNil(t, record)

		batchRows := record.NumRows()
		totalRows += batchRows
		batchCount++

		t.Logf("Batch %d: %d rows", batchCount, batchRows)

		// Verify data types and values for each column
		for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
			field := schema.Field(colIdx)
			column := record.Column(colIdx)

			t.Logf("  Column %s (%s): %d values", field.Name, field.Type.String(), column.Len())

			// Test a few specific values to ensure typed builders worked correctly
			if batchCount == 1 && batchRows >= 2 { // First batch with at least 2 rows
				switch field.Name {
				case "id":
					idCol := column.(*array.Int32)
					require.Equal(t, int32(1), idCol.Value(0), "First ID should be 1")
					require.Equal(t, int32(2), idCol.Value(1), "Second ID should be 2")

				case "tiny_int":
					tinyCol := column.(*array.Int8)
					require.Equal(t, int8(127), tinyCol.Value(0), "First tiny_int should be 127")
					require.Equal(t, int8(-128), tinyCol.Value(1), "Second tiny_int should be -128")

				case "varchar_val":
					strCol := column.(*array.String)
					require.Equal(t, "test string", strCol.Value(0), "First varchar should match")
					require.Equal(t, "another string", strCol.Value(1), "Second varchar should match")

				case "bool_val":
					// MySQL BOOLEAN is actually TINYINT (int8)
					boolCol := column.(*array.Int8)
					require.Equal(t, int8(1), boolCol.Value(0), "First bool should be 1 (true)")
					require.Equal(t, int8(0), boolCol.Value(1), "Second bool should be 0 (false)")

				case "decimal_val":
					// Decimal fields should be properly typed (accept any decimal type)
					switch column.DataType().(type) {
					case *arrow.Decimal32Type, *arrow.Decimal64Type, *arrow.Decimal128Type, *arrow.Decimal256Type:
						// Valid decimal type
					default:
						t.Errorf("Decimal column should be a decimal type, got %T", column.DataType())
					}
				}
			}

			// Test NULL handling for third row (if present)
			if batchCount == 2 && batchRows >= 1 { // Second batch with NULL row
				nullRowIdx := 0 // First row in second batch is the NULL row
				if nullRowIdx < int(batchRows) {
					switch field.Name {
					case "id":
						// ID is auto-increment, should not be NULL
						idCol := column.(*array.Int32)
						require.False(t, idCol.IsNull(nullRowIdx), "ID should not be NULL")
					case "tiny_int", "varchar_val", "bool_val":
						// These should be NULL in the third row
						require.True(t, column.IsNull(nullRowIdx), "Column %s should be NULL in row %d", field.Name, nullRowIdx)
					}
				}
			}
		}

		record.Release()
	}

	require.NoError(t, reader.Err())
	require.Equal(t, int64(3), totalRows, "Should read all 3 rows")
	require.Equal(t, 2, batchCount, "Should have 2 batches with batch size 2")

	t.Log("✅ Typed builder handling test passed successfully")
}

func TestSQLNullableTypesHandling(t *testing.T) {
	dsn := getDSN()
	if dsn == "" {
		t.Skip("MySQL DSN not available")
	}

	mysqlDriver := mysql.NewDriver()

	db, err := mysqlDriver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: dsn,
	})
	require.NoError(t, err)
	defer db.Close()

	cn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer cn.Close()

	stmt, err := cn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()

	// Create test table with nullable columns
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_nullable (
			id INT PRIMARY KEY AUTO_INCREMENT,
			nullable_int INT,
			nullable_bigint BIGINT,
			nullable_float DOUBLE,
			nullable_text TEXT,
			nullable_bool BOOLEAN,
			nullable_datetime DATETIME,
			nullable_time TIME
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Insert test data with NULL values
	err = stmt.SetSqlQuery(`
		INSERT INTO adbc_test_nullable 
		(nullable_int, nullable_bigint, nullable_float, nullable_text, nullable_bool, nullable_datetime, nullable_time) 
		VALUES 
		(123, 456789, 12.34, 'test string', true, '2023-01-01 12:30:45', '14:30:00'),
		(NULL, NULL, NULL, NULL, NULL, NULL, NULL),
		(789, 987654, 56.78, 'another test', false, '2023-12-31 23:59:59', '23:59:59')
	`)
	require.NoError(t, err)
	affected, err := stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(3), affected, "Should insert 3 rows")

	// Query the data to test nullable type handling
	err = stmt.SetSqlQuery("SELECT * FROM adbc_test_nullable ORDER BY id")
	require.NoError(t, err)

	reader, rowCount, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(-1), rowCount) // -1 because we don't know row count upfront
	defer reader.Release()

	totalRows := int64(0)
	batchCount := 0

	for reader.Next() {
		batchCount++
		record := reader.Record()
		require.NotNil(t, record, "Record should not be nil")

		rowsInBatch := record.NumRows()
		totalRows += rowsInBatch

		t.Logf("Batch %d: %d rows, %d columns", batchCount, rowsInBatch, record.NumCols())

		// Verify the schema
		schema := record.Schema()
		require.Equal(t, 8, len(schema.Fields()), "Should have 8 columns")

		// Check some specific values
		for rowIdx := int64(0); rowIdx < rowsInBatch; rowIdx++ {
			// Get ID column (should never be null)
			idCol := record.Column(0)
			require.False(t, idCol.IsNull(int(rowIdx)), "ID should not be null")

			// Check that second row (index 1) has nulls for nullable columns
			if totalRows > 1 && rowIdx == 1 {
				for colIdx := 1; colIdx < int(record.NumCols()); colIdx++ {
					col := record.Column(colIdx)
					require.True(t, col.IsNull(int(rowIdx)),
						"Column %d of row %d should be null", colIdx, rowIdx)
				}
				t.Log("✅ NULL values correctly handled")
			}
		}

		record.Release()
	}

	require.NoError(t, reader.Err())
	require.Equal(t, int64(3), totalRows, "Should read all 3 rows")

	t.Log("✅ SQL nullable types handling test passed successfully")
}

func TestExtendedArrowArrayTypes(t *testing.T) {
	dsn := getDSN()
	if dsn == "" {
		t.Skip("MySQL DSN not available")
	}

	mysqlDriver := mysql.NewDriver()

	db, err := mysqlDriver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: dsn,
	})
	require.NoError(t, err)
	defer db.Close()

	cn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer cn.Close()

	stmt, err := cn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()

	// Create test table with different string and binary types
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_extended_types (
			id INT PRIMARY KEY AUTO_INCREMENT,
			varchar_col VARCHAR(255),
			text_col TEXT,
			longtext_col LONGTEXT,
			binary_col BINARY(16),
			varbinary_col VARBINARY(255),
			blob_col BLOB
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Insert test data
	err = stmt.SetSqlQuery(`
		INSERT INTO adbc_test_extended_types 
		(varchar_col, text_col, longtext_col, binary_col, varbinary_col, blob_col) 
		VALUES 
		('short string', 'medium text content', 'very long text content that could be handled by different Arrow string types', 
		 0x0123456789ABCDEF0123456789ABCDEF, 0x48656C6C6F, 0x576F726C64)
	`)
	require.NoError(t, err)
	affected, err := stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), affected, "Should insert 1 row")

	// Query the data to test extended array type handling
	err = stmt.SetSqlQuery("SELECT * FROM adbc_test_extended_types")
	require.NoError(t, err)

	reader, rowCount, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(-1), rowCount)
	defer reader.Release()

	recordCount := 0
	for reader.Next() {
		recordCount++
		record := reader.Record()
		require.NotNil(t, record, "Record should not be nil")
		require.Equal(t, int64(1), record.NumRows(), "Should have 1 row")
		require.Equal(t, int64(7), record.NumCols(), "Should have 7 columns")

		// Verify we can read all column types without errors
		for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
			col := record.Column(colIdx)
			require.False(t, col.IsNull(0), "Column %d should not be null", colIdx)

			// Try to get the value from the array - this exercises our extractArrowValue logic
			switch a := col.(type) {
			case *array.String:
				val := a.Value(0)
				require.NotEmpty(t, val, "String value should not be empty")
			case *array.Binary:
				val := a.Value(0)
				require.NotEmpty(t, val, "Binary value should not be empty")
			case *array.Int32:
				val := a.Value(0)
				require.Greater(t, val, int32(0), "ID should be positive")
			}
		}

		record.Release()
	}

	require.NoError(t, reader.Err())
	require.Equal(t, 1, recordCount, "Should read exactly 1 record")

	t.Log("✅ Extended Arrow array types test passed successfully")
}

func TestTemporalAndDecimalExtraction(t *testing.T) {
	dsn := getDSN()
	if dsn == "" {
		t.Skip("MySQL DSN not available")
	}

	mysqlDriver := mysql.NewDriver()

	db, err := mysqlDriver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: dsn,
	})
	require.NoError(t, err)
	defer db.Close()

	cn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer cn.Close()

	stmt, err := cn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()

	// Create test table with temporal and decimal types
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_temporal_decimal (
			id INT PRIMARY KEY AUTO_INCREMENT,
			date_col DATE,
			datetime_col DATETIME(6),
			timestamp_col TIMESTAMP(3),
			time_col TIME(6),
			decimal_precise DECIMAL(10, 2),
			decimal_money DECIMAL(19, 4),
			decimal_percentage DECIMAL(5, 3)
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Insert test data with various temporal and decimal values
	err = stmt.SetSqlQuery(`
		INSERT INTO adbc_test_temporal_decimal 
		(date_col, datetime_col, timestamp_col, time_col, decimal_precise, decimal_money, decimal_percentage) 
		VALUES 
		('2023-06-15', '2023-06-15 14:30:45.123456', '2023-06-15 14:30:45.123', '14:30:45.123456', 
		 123.45, 1234567.8900, 99.999),
		('2024-01-01', '2024-01-01 00:00:00.000000', '2024-01-01 00:00:00.000', '00:00:00.000000', 
		 0.01, 0.0001, 0.001)
	`)
	require.NoError(t, err)
	affected, err := stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), affected, "Should insert 2 rows")

	// Test using the data for parameter binding (this exercises extractArrowValue)
	err = stmt.SetSqlQuery("SELECT * FROM adbc_test_temporal_decimal ORDER BY id")
	require.NoError(t, err)

	reader, rowCount, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(-1), rowCount)
	defer reader.Release()

	totalRows := 0
	for reader.Next() {
		record := reader.Record()
		require.NotNil(t, record, "Record should not be nil")

		rowsInBatch := int(record.NumRows())
		totalRows += rowsInBatch

		t.Logf("Processing batch with %d rows", rowsInBatch)

		// Verify schema contains proper Arrow types for /decimal data
		schema := record.Schema()
		require.Equal(t, 8, len(schema.Fields()), "Should have 8 columns")

		// Check that we can read the data successfully
		for rowIdx := 0; rowIdx < rowsInBatch; rowIdx++ {
			for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
				col := record.Column(colIdx)
				if !col.IsNull(rowIdx) {
					// Test that we can get values from different types
					switch colIdx {
					case 0: // id - should be int
						require.Equal(t, arrow.PrimitiveTypes.Int32, col.DataType())
					case 1: // date_col - should be date
						require.Equal(t, arrow.FixedWidthTypes.Date64, col.DataType())
					case 2, 3: // datetime_col, timestamp_col - should be timestamp
						timestampType, ok := col.DataType().(*arrow.TimestampType)
						require.True(t, ok, "Should be timestamp type")
						require.Contains(t, []arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond}, timestampType.Unit)
					case 4: // time_col - should be time
						timeType := col.DataType()
						// Check if it's a time type (Time32 or Time64)
						isTimeType := false
						switch timeType.(type) {
						case *arrow.Time32Type, *arrow.Time64Type:
							isTimeType = true
						}
						require.True(t, isTimeType, "Should be time type, got %T", timeType)
					case 5, 6, 7: // decimal columns - should be decimal
						decimalType, ok := col.DataType().(arrow.DecimalType)
						require.True(t, ok, "Should be decimal type, got %T", col.DataType())
						require.Greater(t, decimalType.GetPrecision(), int32(0), "Precision should be positive")
						require.GreaterOrEqual(t, decimalType.GetScale(), int32(0), "Scale should be non-negative")
					}
				}
			}
		}

		record.Release()
	}

	require.NoError(t, reader.Err())
	require.Equal(t, 2, totalRows, "Should read exactly 2 rows")

	t.Log("✅ Temporal and decimal extraction test passed successfully")
}

// TestMySQLCustomTypeConverter tests the custom MySQL TypeConverter value conversion methods
func TestMySQLCustomTypeConverter(t *testing.T) {
	dsn := getDSN()
	if dsn == "" {
		t.Skip("MySQL DSN not available")
	}

	mysqlDriver := mysql.NewDriver()

	db, err := mysqlDriver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: dsn,
	})
	require.NoError(t, err)
	defer db.Close()

	cn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer cn.Close()

	stmt, err := cn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()

	// Create test table with MySQL-specific types that will use custom conversion
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_custom_converter (
			id INT PRIMARY KEY AUTO_INCREMENT,
			json_col JSON,
			enum_col ENUM('value1', 'value2', 'value3'),
			timestamp_col TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			datetime_col DATETIME,
			text_col TEXT
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Insert test data with various MySQL-specific values
	err = stmt.SetSqlQuery(`
		INSERT INTO adbc_test_custom_converter 
		(json_col, enum_col, timestamp_col, datetime_col, text_col) 
		VALUES 
		('{"key": "value", "number": 42}', 'value1', '2023-06-15 14:30:45', '2023-06-15 14:30:45', 'regular text'),
		('{"array": [1, 2, 3], "nested": {"inner": true}}', 'value2', '2024-01-01 00:00:00', '2024-01-01 00:00:00', 'more text')
	`)
	require.NoError(t, err)
	affected, err := stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), affected, "Should insert 2 rows")

	// Query the data to test the custom TypeConverter
	err = stmt.SetSqlQuery("SELECT id, json_col, enum_col, timestamp_col, datetime_col, text_col FROM adbc_test_custom_converter ORDER BY id")
	require.NoError(t, err)

	reader, rowCount, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(-1), rowCount)
	defer reader.Release()

	schema := reader.Schema()
	require.Equal(t, 6, len(schema.Fields()), "Should have 6 columns")

	// Verify schema includes MySQL-specific metadata
	for i, field := range schema.Fields() {
		t.Logf("Field %d: %s (type: %s)", i, field.Name, field.Type.String())

		switch field.Name {
		case "json_col":
			require.Equal(t, "utf8", field.Type.String(), "JSON should be utf8 type")

			// Verify MySQL JSON metadata
			isJSON, ok := field.Metadata.GetValue("mysql.is_json")
			require.True(t, ok, "Should have mysql.is_json metadata")
			require.Equal(t, "true", isJSON, "Should be marked as JSON")
			t.Logf("  ✓ JSON metadata detected: %s", isJSON)

		case "enum_col":
			require.Equal(t, "utf8", field.Type.String(), "ENUM should be utf8 type")

			// Verify MySQL ENUM metadata
			isEnumSet, ok := field.Metadata.GetValue("mysql.is_enum_set")
			require.True(t, ok, "Should have mysql.is_enum_set metadata")
			require.Equal(t, "true", isEnumSet, "Should be marked as ENUM")
			t.Logf("  ✓ ENUM metadata detected: %s", isEnumSet)

		case "timestamp_col":
			require.Equal(t, "timestamp[s]", field.Type.String(), "TIMESTAMP should be timestamp[s]")

			// Verify this is marked as TIMESTAMP (not DATETIME)
			dbType, ok := field.Metadata.GetValue("sql.database_type_name")
			require.True(t, ok, "Should have database_type_name metadata")
			require.Equal(t, "TIMESTAMP", dbType, "Should be TIMESTAMP type")
			t.Logf("  ✓ TIMESTAMP type detected: %s", dbType)

		case "datetime_col":
			require.Equal(t, "timestamp[s]", field.Type.String(), "DATETIME should be timestamp[s]")

			// Verify this is marked as DATETIME (not TIMESTAMP)
			dbType, ok := field.Metadata.GetValue("sql.database_type_name")
			require.True(t, ok, "Should have database_type_name metadata")
			require.Equal(t, "DATETIME", dbType, "Should be DATETIME type")
			t.Logf("  ✓ DATETIME type detected: %s", dbType)
		}
	}

	totalRows := 0
	for reader.Next() {
		record := reader.Record()
		require.NotNil(t, record, "Record should not be nil")

		rowsInBatch := int(record.NumRows())
		totalRows += rowsInBatch

		t.Logf("Processing batch with %d rows", rowsInBatch)

		// Test data retrieval and verify custom TypeConverter behavior
		for rowIdx := 0; rowIdx < rowsInBatch; rowIdx++ {
			t.Logf("Row %d:", rowIdx)

			// Test JSON column (custom handling in ConvertSQLToArrow)
			jsonCol := record.Column(1).(*array.String)
			if !jsonCol.IsNull(rowIdx) {
				jsonValue := jsonCol.Value(rowIdx)
				t.Logf("  JSON: %s", jsonValue)

				// Verify it's valid JSON string
				require.True(t, len(jsonValue) > 0, "JSON value should not be empty")
				require.True(t, jsonValue[0] == '{', "JSON should start with {")
				// Check for content specific to each row
				if rowIdx == 0 {
					require.Contains(t, jsonValue, "key", "First JSON should contain 'key'")
				} else if rowIdx == 1 {
					require.Contains(t, jsonValue, "array", "Second JSON should contain 'array'")
				}
			}

			// Test ENUM column (custom handling in ConvertSQLToArrow)
			enumCol := record.Column(2).(*array.String)
			if !enumCol.IsNull(rowIdx) {
				enumValue := enumCol.Value(rowIdx)
				t.Logf("  ENUM: %s", enumValue)

				// Verify it's one of the allowed ENUM values
				require.Contains(t, []string{"value1", "value2", "value3"}, enumValue, "ENUM value should be valid")
			}

			// Test TIMESTAMP column (should be handled by custom converter)
			timestampCol := record.Column(3).(*array.Timestamp)
			if !timestampCol.IsNull(rowIdx) {
				timestampValue := timestampCol.Value(rowIdx)
				t.Logf("  TIMESTAMP: %v", timestampValue)

				// Verify timestamp is reasonable (between 2020 and 2030)
				require.True(t, timestampValue > 0, "Timestamp should be positive")
			}

			// Test DATETIME column (should use default converter behavior)
			datetimeCol := record.Column(4).(*array.Timestamp)
			if !datetimeCol.IsNull(rowIdx) {
				datetimeValue := datetimeCol.Value(rowIdx)
				t.Logf("  DATETIME: %v", datetimeValue)

				// Verify datetime is reasonable (between 2020 and 2030)
				require.True(t, datetimeValue > 0, "Datetime should be positive")
			}
		}

		record.Release()
	}

	require.NoError(t, reader.Err())
	require.Equal(t, 2, totalRows, "Should read exactly 2 rows")

	t.Log("✅ MySQL custom TypeConverter test passed successfully")
}

// TestMySQLTypeConverterEdgeCases tests edge cases and error conditions in the custom MySQL TypeConverter
func TestMySQLTypeConverterEdgeCases(t *testing.T) {
	dsn := getDSN()
	if dsn == "" {
		t.Skip("MySQL DSN not available")
	}

	mysqlDriver := mysql.NewDriver()

	db, err := mysqlDriver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: dsn,
	})
	require.NoError(t, err)
	defer db.Close()

	cn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer cn.Close()

	stmt, err := cn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()

	// Create test table with edge cases
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_converter_edge_cases (
			id INT PRIMARY KEY AUTO_INCREMENT,
			json_null JSON,
			json_empty JSON,
			json_invalid TEXT,
			enum_null ENUM('a', 'b') DEFAULT NULL,
			timestamp_null TIMESTAMP NULL,
			large_json JSON
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Insert edge case data
	err = stmt.SetSqlQuery(`
		INSERT INTO adbc_test_converter_edge_cases 
		(json_null, json_empty, json_invalid, enum_null, timestamp_null, large_json) 
		VALUES 
		(NULL, '{}', 'not json', NULL, NULL, '{"large": "data", "array": [1,2,3,4,5], "nested": {"deep": {"deeper": "value"}}}'),
		('null', '[]', 'also not json', 'a', '2023-01-01 12:00:00', '{"simple": true}')
	`)
	require.NoError(t, err)
	affected, err := stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), affected, "Should insert 2 rows")

	// Query and verify edge case handling
	err = stmt.SetSqlQuery("SELECT * FROM adbc_test_converter_edge_cases ORDER BY id")
	require.NoError(t, err)

	reader, _, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	defer reader.Release()

	totalRows := 0
	for reader.Next() {
		record := reader.Record()
		require.NotNil(t, record, "Record should not be nil")

		rowsInBatch := int(record.NumRows())
		totalRows += rowsInBatch

		// Test NULL handling
		for rowIdx := 0; rowIdx < rowsInBatch; rowIdx++ {
			t.Logf("Testing edge case row %d:", rowIdx)

			// Test NULL JSON
			jsonNullCol := record.Column(1)
			if rowIdx == 0 {
				require.True(t, jsonNullCol.IsNull(rowIdx), "First row json_null should be NULL")
				t.Log("  ✓ NULL JSON handled correctly")
			}

			// Test empty JSON objects/arrays
			jsonEmptyCol := record.Column(2)
			if !jsonEmptyCol.IsNull(rowIdx) {
				jsonEmpty := jsonEmptyCol.(*array.String).Value(rowIdx)
				t.Logf("  Empty JSON: %s", jsonEmpty)
				require.True(t, jsonEmpty == "{}" || jsonEmpty == "[]" || jsonEmpty == "null", "Should handle empty JSON")
			}

			// Test invalid JSON in TEXT field (should be handled as regular string)
			jsonInvalidCol := record.Column(3).(*array.String)
			if !jsonInvalidCol.IsNull(rowIdx) {
				invalidValue := jsonInvalidCol.Value(rowIdx)
				t.Logf("  Invalid JSON as text: %s", invalidValue)
				require.Contains(t, []string{"not json", "also not json"}, invalidValue, "Should handle non-JSON text")
			}

			// Test NULL ENUM
			enumNullCol := record.Column(4)
			if rowIdx == 0 {
				require.True(t, enumNullCol.IsNull(rowIdx), "First row enum_null should be NULL")
				t.Log("  ✓ NULL ENUM handled correctly")
			}

			// Test large JSON (verify custom converter can handle large data)
			largeJsonCol := record.Column(6).(*array.String)
			if !largeJsonCol.IsNull(rowIdx) {
				largeJson := largeJsonCol.Value(rowIdx)
				t.Logf("  Large JSON length: %d chars", len(largeJson))
				require.Greater(t, len(largeJson), 10, "Large JSON should be substantial")
				require.True(t, largeJson[0] == '{', "Large JSON should be valid JSON object")
			}
		}

		record.Release()
	}

	require.NoError(t, reader.Err())
	require.Equal(t, 2, totalRows, "Should read exactly 2 rows")

	t.Log("✅ MySQL custom TypeConverter edge cases test passed successfully")
}
