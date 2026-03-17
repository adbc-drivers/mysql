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

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/suite"
)

type MySQLIngestTestSuite struct {
	suite.Suite

	Quirks *MySQLQuirks
	mem    *memory.CheckedAllocator
	ctx    context.Context
	driver adbc.Driver
	db     adbc.Database
	cnxn   adbc.Connection
}

func (s *MySQLIngestTestSuite) SetupSuite() {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		s.T().Skip("MYSQL_DSN not set")
	}
	s.Quirks = &MySQLQuirks{dsn: dsn}
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	s.ctx = context.Background()
	s.driver = s.Quirks.SetupDriver(s.T())

	var err error
	s.db, err = s.driver.NewDatabase(s.Quirks.DatabaseOptions())
	s.NoError(err)
	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)
}

func (s *MySQLIngestTestSuite) TearDownSuite() {
	if s.cnxn != nil {
		s.NoError(s.cnxn.Close())
	}
	if s.db != nil {
		s.NoError(s.db.Close())
	}
	s.Quirks.TearDownDriver(s.T(), s.driver)
}

func (s *MySQLIngestTestSuite) getTableNames(testName string) []string {
	switch testName {
	case "TestConcurrentIngest":
		var names []string
		for i := 0; i < 5; i++ {
			names = append(names, fmt.Sprintf("concurrent_ingest_%d", i))
		}
		return names
	case "TestLargeIngest":
		return []string{"large_ingest_test"}
	case "TestSchemaMismatch":
		return []string{"schema_mismatch_test"}
	case "TestIngestFallback":
		return []string{"fallback_ingest_test"}
	case "TestComplexTypes":
		return []string{"complex_types_ingest"}
	default:
		return nil
	}
}

func (s *MySQLIngestTestSuite) cleanupTables(testName string) {
	tables := s.getTableNames(testName)
	if len(tables) == 0 {
		return
	}

	stmt, err := s.cnxn.NewStatement()
	s.NoError(err)
	defer stmt.Close()

	for _, table := range tables {
		s.NoError(stmt.SetSqlQuery("DROP TABLE IF EXISTS " + table))
		_, _ = stmt.ExecuteUpdate(s.ctx)
	}
}

func (s *MySQLIngestTestSuite) BeforeTest(suiteName, testName string) {
	s.cleanupTables(testName)
}

func (s *MySQLIngestTestSuite) AfterTest(suiteName, testName string) {
	s.cleanupTables(testName)
}

// TestLargeIngest verifies that chunked ingestion works for datasets larger than BatchSize
func (s *MySQLIngestTestSuite) TestLargeIngest() {
	const numRows = 15000
	tableName := "large_ingest_test"

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "val", Type: arrow.BinaryTypes.String},
	}, nil)

	bldr := array.NewRecordBuilder(s.mem, schema)
	defer bldr.Release()

	for i := 0; i < numRows; i++ {
		bldr.Field(0).(*array.Int32Builder).Append(int32(i))
		bldr.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("value-%d", i))
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	stmt, err := s.cnxn.NewStatement()
	s.NoError(err)
	defer stmt.Close()

	s.NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, tableName))
	s.NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate))
	s.NoError(stmt.Bind(s.ctx, rec))

	_, err = stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	s.NoError(stmt.SetSqlQuery("SELECT COUNT(*) FROM " + tableName))
	rdr, _, err := stmt.ExecuteQuery(s.ctx)
	s.NoError(err)
	defer rdr.Release()

	s.True(rdr.Next())
	countRec := rdr.RecordBatch()
	s.Equal(int64(numRows), countRec.Column(0).(*array.Int64).Value(0))
}

// TestConcurrentIngest ensures that unique reader names prevent collisions during parallel ingestion
func (s *MySQLIngestTestSuite) TestConcurrentIngest() {
	const numThreads = 5
	const rowsPerThread = 100

	var wg sync.WaitGroup
	wg.Add(numThreads)

	for i := 0; i < numThreads; i++ {
		go func(id int) {
			defer wg.Done()

			tableName := fmt.Sprintf("concurrent_ingest_%d", id)

			// Use a fresh connection per thread to avoid state conflicts if any
			cnxn, err := s.db.Open(s.ctx)
			s.NoError(err)
			defer cnxn.Close()

			schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)
			bldr := array.NewRecordBuilder(s.mem, schema)
			defer bldr.Release()
			for r := 0; r < rowsPerThread; r++ {
				bldr.Field(0).(*array.Int32Builder).Append(int32(r))
			}
			rec := bldr.NewRecordBatch()
			defer rec.Release()

			stmt, err := cnxn.NewStatement()
			s.NoError(err)
			defer stmt.Close()

			s.NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, tableName))
			s.NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate))
			s.NoError(stmt.Bind(s.ctx, rec))

			_, err = stmt.ExecuteUpdate(s.ctx)
			s.NoError(err)

			s.NoError(stmt.SetSqlQuery("SELECT COUNT(*) FROM " + tableName))
			rdr, _, err := stmt.ExecuteQuery(s.ctx)
			s.NoError(err)
			defer rdr.Release()
			s.True(rdr.Next())
			s.Equal(int64(rowsPerThread), rdr.Record().Column(0).(*array.Int64).Value(0))
		}(i)
	}

	wg.Wait()
}

// TestSchemaMismatch verifies that appending data with extra columns returns an error
func (s *MySQLIngestTestSuite) TestSchemaMismatch() {
	tableName := "schema_mismatch_test"

	// Create table with 1 column
	schema1 := arrow.NewSchema([]arrow.Field{{Name: "col1", Type: arrow.PrimitiveTypes.Int32}}, nil)
	bldr1 := array.NewRecordBuilder(s.mem, schema1)
	defer bldr1.Release()
	bldr1.Field(0).(*array.Int32Builder).Append(1)
	rec1 := bldr1.NewRecordBatch()
	defer rec1.Release()

	stmt, err := s.cnxn.NewStatement()
	s.NoError(err)
	defer stmt.Close()

	s.NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, tableName))
	s.NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate))
	s.NoError(stmt.Bind(s.ctx, rec1))
	_, err = stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	// Try to append with 2 columns
	schema2 := arrow.NewSchema([]arrow.Field{
		{Name: "col1", Type: arrow.PrimitiveTypes.Int32},
		{Name: "col2", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	bldr2 := array.NewRecordBuilder(s.mem, schema2)
	defer bldr2.Release()
	bldr2.Field(0).(*array.Int32Builder).Append(2)
	bldr2.Field(1).(*array.Int32Builder).Append(3)
	rec2 := bldr2.NewRecordBatch()
	defer rec2.Release()

	s.NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend))
	s.NoError(stmt.Bind(s.ctx, rec2))
	_, err = stmt.ExecuteUpdate(s.ctx)

	s.Error(err)
	s.Contains(err.Error(), "Unknown column 'col2' in 'field list'")
}

// TestIngestFallback verifies that the driver falls back to INSERT statements when LOAD DATA is disabled
func (s *MySQLIngestTestSuite) TestIngestFallback() {
	db, err := s.driver.NewDatabase(s.Quirks.DatabaseOptions())
	s.NoError(err)
	defer db.Close()

	cnxn, err := db.Open(s.ctx)
	s.NoError(err)
	defer cnxn.Close()

	adminStmt, err := cnxn.NewStatement()
	s.NoError(err)
	defer adminStmt.Close()

	// Try to disable. Note: This might require SUPER privilege on the server.
	// If it fails, we might just skip the test or use a different approach.
	err = adminStmt.SetSqlQuery("SET GLOBAL local_infile = 0")
	s.NoError(err)
	_, err = adminStmt.ExecuteUpdate(s.ctx)
	if err != nil {
		s.T().Skip("Skipping fallback test: failed to disable global local_infile (requires SUPER privilege)")
		return
	}
	defer func() {
		backStmt, err := cnxn.NewStatement()
		if err == nil {
			_ = backStmt.SetSqlQuery("SET GLOBAL local_infile = 1")
			_, _ = backStmt.ExecuteUpdate(s.ctx)
			_ = backStmt.Close()
		}
	}()

	tableName := "fallback_ingest_test"
	schema := arrow.NewSchema([]arrow.Field{{Name: "col1", Type: arrow.PrimitiveTypes.Int32}}, nil)
	bldr := array.NewRecordBuilder(s.mem, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int32Builder).Append(123)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	stmt, err := cnxn.NewStatement()
	s.NoError(err)
	defer stmt.Close()

	s.NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, tableName))
	s.NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate))
	s.NoError(stmt.Bind(s.ctx, rec))

	// This should now use INSERT statements because LOAD DATA is disabled
	affected, err := stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)
	s.EqualValues(1, affected)

	s.NoError(stmt.SetSqlQuery("SELECT * FROM " + tableName))
	rdr, _, err := stmt.ExecuteQuery(s.ctx)
	s.NoError(err)
	defer rdr.Release()
	s.True(rdr.Next())
	s.Equal(int32(123), rdr.RecordBatch().Column(0).(*array.Int32).Value(0))
}

func (s *MySQLIngestTestSuite) TestComplexTypes() {
	tableName := "complex_types_ingest"

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_us},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)

	bldr := array.NewRecordBuilder(s.mem, schema)
	defer bldr.Release()

	// 2026-03-13 12:00:00 UTC
	tsValue := int64(1773403200000000)

	bldr.Field(0).(*array.Int32Builder).Append(1)
	bldr.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(tsValue))
	bldr.Field(2).(*array.BooleanBuilder).Append(true)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	stmt, err := s.cnxn.NewStatement()
	s.NoError(err)
	defer stmt.Close()

	s.NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, tableName))
	s.NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate))
	s.NoError(stmt.Bind(s.ctx, rec))

	_, err = stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	s.NoError(stmt.SetSqlQuery("SELECT * FROM " + tableName))
	rdr, _, err := stmt.ExecuteQuery(s.ctx)
	s.NoError(err)
	defer rdr.Release()
	s.True(rdr.Next())

	recOut := rdr.RecordBatch()
	s.Equal(int32(1), recOut.Column(0).(*array.Int32).Value(0))
	s.NotNil(recOut.Column(1).(*array.Timestamp))
	// MySQL TINYINT(1) used for boolean is returned as Int8 by the ADBC driver
	s.Equal(int8(1), recOut.Column(2).(*array.Int8).Value(0))
}

func TestMySQLIngestSuite(t *testing.T) {
	suite.Run(t, new(MySQLIngestTestSuite))
}
