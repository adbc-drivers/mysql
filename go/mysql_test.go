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
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/adbc-drivers/driverbase-go/validation"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	mysql "github.com/adbc-drivers/mysql"
)

// MySQLQuirks implements validation.DriverQuirks for MySQL ADBC driver
type MySQLQuirks struct {
	dsn string
	mem *memory.CheckedAllocator
}

func (q *MySQLQuirks) SetupDriver(t *testing.T) adbc.Driver {
	q.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	return mysql.NewDriver(q.mem)
}

func (q *MySQLQuirks) TearDownDriver(t *testing.T, _ adbc.Driver) {
	q.mem.AssertSize(t, 0)
}

func (q *MySQLQuirks) DatabaseOptions() map[string]string {
	return map[string]string{
		adbc.OptionKeyURI: q.dsn,
	}
}

func (q *MySQLQuirks) CreateSampleTable(tableName string, r arrow.RecordBatch) error {
	// Use standard database/sql to create table directly
	db, err := sql.Open("mysql", q.dsn)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	// Drop table if it exists first to ensure clean state
	_, err = db.Exec("DROP TABLE IF EXISTS " + tableName)
	if err != nil {
		return fmt.Errorf("failed to drop existing table: %w", err)
	}

	// Build CREATE TABLE statement based on Arrow schema
	var createQuery strings.Builder
	createQuery.WriteString("CREATE TABLE ")
	createQuery.WriteString(tableName)
	createQuery.WriteString(" (")

	schema := r.Schema()
	for i, field := range schema.Fields() {
		if i > 0 {
			createQuery.WriteString(", ")
		}
		createQuery.WriteString(field.Name)
		createQuery.WriteString(" ")

		// Map Arrow types to MySQL types
		switch field.Type.ID() {
		case arrow.INT32:
			createQuery.WriteString("INT")
		case arrow.INT64:
			createQuery.WriteString("BIGINT")
		case arrow.STRING:
			createQuery.WriteString("VARCHAR(255)")
		case arrow.FLOAT32:
			createQuery.WriteString("FLOAT")
		case arrow.FLOAT64:
			createQuery.WriteString("DOUBLE")
		case arrow.BOOL:
			createQuery.WriteString("BOOLEAN")
		default:
			createQuery.WriteString("TEXT") // Default fallback
		}

		if !field.Nullable {
			createQuery.WriteString(" NOT NULL")
		}
	}
	createQuery.WriteString(")")

	_, err = db.Exec(createQuery.String())
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Insert data from Arrow record
	if r.NumRows() > 0 {
		// Insert each row separately to handle NULL values correctly
		for row := range r.NumRows() {
			var insertQuery strings.Builder
			insertQuery.WriteString("INSERT INTO ")
			insertQuery.WriteString(tableName)
			insertQuery.WriteString(" VALUES (")

			values := make([]interface{}, r.NumCols())
			for col := range r.NumCols() {
				column := r.Column(int(col))
				if column.IsNull(int(row)) {
					values[col] = nil
				} else {
					// Extract value based on column type
					switch arr := column.(type) {
					case *array.Int32:
						values[col] = arr.Value(int(row))
					case *array.Int64:
						values[col] = arr.Value(int(row))
					case *array.String:
						values[col] = arr.Value(int(row))
					case *array.Float32:
						values[col] = arr.Value(int(row))
					case *array.Float64:
						values[col] = arr.Value(int(row))
					case *array.Boolean:
						values[col] = arr.Value(int(row))
					default:
						values[col] = fmt.Sprintf("%v", column)
					}
				}
			}

			// Build placeholders and collect non-null values for prepared statement
			var queryParams []interface{}
			for i, val := range values {
				if i > 0 {
					insertQuery.WriteString(", ")
				}
				if val == nil {
					insertQuery.WriteString("NULL")
				} else {
					insertQuery.WriteString("?")
					queryParams = append(queryParams, val)
				}
			}
			insertQuery.WriteString(")")

			_, err = db.Exec(insertQuery.String(), queryParams...)
			if err != nil {
				return fmt.Errorf("failed to insert row %d: %w", row, err)
			}
		}
	}

	return nil
}

func (q *MySQLQuirks) DropTable(cnxn adbc.Connection, tblName string) error {
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()

	if err = stmt.SetSqlQuery("DROP TABLE IF EXISTS " + tblName); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(context.Background())
	return err
}

func (q *MySQLQuirks) SampleTableSchemaMetadata(tblName string, dt arrow.DataType) arrow.Metadata {
	// Return metadata that matches what our MySQL type converter actually returns
	metadata := map[string]string{}

	switch dt.ID() {
	case arrow.INT32:
		metadata["sql.column_name"] = "ints"
		metadata["sql.database_type_name"] = "int"
		metadata["sql.precision"] = "10"
		metadata["sql.scale"] = "0"
	case arrow.INT64:
		metadata["sql.column_name"] = "ints"
		metadata["sql.database_type_name"] = "bigint"
		metadata["sql.precision"] = "19"
		metadata["sql.scale"] = "0"
	case arrow.STRING:
		metadata["sql.column_name"] = "strings"
		metadata["sql.database_type_name"] = "varchar"
		metadata["sql.length"] = "255"
	case arrow.FLOAT32:
		metadata["sql.column_name"] = "floats"
		metadata["sql.database_type_name"] = "float"
	case arrow.FLOAT64:
		metadata["sql.column_name"] = "doubles"
		metadata["sql.database_type_name"] = "double"
	case arrow.BOOL:
		metadata["sql.column_name"] = "bools"
		metadata["sql.database_type_name"] = "tinyint"
	}

	return arrow.MetadataFrom(metadata)
}

func (q *MySQLQuirks) Alloc() memory.Allocator      { return q.mem }
func (q *MySQLQuirks) BindParameter(idx int) string { return "?" }

// SupportsBulkIngest returns false because MySQL doesn't support "NULLS LAST" syntax
// used in the ADBC validation bulk ingest tests.
// TODO: enable this once the validation framework is fixed.
// Filed issue: https://github.com/adbc-drivers/driverbase-go/issues/69
func (q *MySQLQuirks) SupportsBulkIngest(string) bool              { return false }
func (q *MySQLQuirks) SupportsConcurrentStatements() bool          { return false }
func (q *MySQLQuirks) SupportsCurrentCatalogSchema() bool          { return true }
func (q *MySQLQuirks) SupportsExecuteSchema() bool                 { return true }
func (q *MySQLQuirks) SupportsGetSetOptions() bool                 { return true }
func (q *MySQLQuirks) SupportsPartitionedData() bool               { return false }
func (q *MySQLQuirks) SupportsStatistics() bool                    { return false }
func (q *MySQLQuirks) SupportsTransactions() bool                  { return false }
func (q *MySQLQuirks) SupportsGetParameterSchema() bool            { return false }
func (q *MySQLQuirks) SupportsDynamicParameterBinding() bool       { return true }
func (q *MySQLQuirks) SupportsErrorIngestIncompatibleSchema() bool { return true }
func (q *MySQLQuirks) Catalog() string                             { return "db" }
func (q *MySQLQuirks) DBSchema() string                            { return "" }

func (q *MySQLQuirks) GetMetadata(code adbc.InfoCode) interface{} {
	switch code {
	case adbc.InfoDriverName:
		return "ADBC Driver Foundry Driver for MySQL"
	case adbc.InfoDriverVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoVendorVersion:
		return "9.4.0 (MySQL Community Server - GPL)"
	case adbc.InfoVendorArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverADBCVersion:
		return adbc.AdbcVersion1_1_0
	case adbc.InfoVendorName:
		return "MySQL"
	case adbc.InfoVendorSql:
		return true
	case adbc.InfoVendorSubstrait:
		return false
	}
	return nil
}

func withQuirks(t *testing.T, fn func(*MySQLQuirks)) {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		t.Skip("Set MYSQL_DSN environment variable for validation tests")
	}

	q := &MySQLQuirks{dsn: dsn}
	fn(q)
}

type MySQLStatementTests struct {
	validation.StatementTests
}

func (s *MySQLStatementTests) TestSqlIngestErrors() {
	s.T().Skip()
}

// TestValidation runs the comprehensive ADBC validation test suite
// This is the primary test that validates ADBC specification compliance
func TestValidation(t *testing.T) {
	withQuirks(t, func(q *MySQLQuirks) {
		suite.Run(t, &validation.DatabaseTests{Quirks: q})
		suite.Run(t, &validation.ConnectionTests{Quirks: q})
		suite.Run(t, &validation.StatementTests{Quirks: q})
	})
}

// -------------------- Additional Tests --------------------

type MySQLTests struct {
	suite.Suite

	Quirks *MySQLQuirks

	ctx    context.Context
	driver adbc.Driver
	db     adbc.Database
	cnxn   adbc.Connection
	stmt   adbc.Statement
}

func (s *MySQLTests) SetupTest() {
	var err error
	s.ctx = context.Background()
	s.driver = s.Quirks.SetupDriver(s.T())
	s.db, err = s.driver.NewDatabase(s.Quirks.DatabaseOptions())
	s.NoError(err)
	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)
	s.stmt, err = s.cnxn.NewStatement()
	s.NoError(err)
}

func (s *MySQLTests) TearDownTest() {
	s.NoError(s.stmt.Close())
	s.NoError(s.cnxn.Close())
	s.Quirks.TearDownDriver(s.T(), s.driver)
	s.cnxn = nil
	s.NoError(s.db.Close())
	s.db = nil
	s.driver = nil
}

type selectCase struct {
	name     string
	query    string
	schema   *arrow.Schema
	expected string
}

func (s *MySQLTests) TestSelect() {
	// Create test table with various MySQL types including spatial
	s.NoError(s.stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE test_types (
			bool_col TINYINT(1),
			tinyint_col TINYINT,
			int_col INT,
			bigint_col BIGINT,
			float_col FLOAT,
			double_col DOUBLE,
			varchar_col VARCHAR(100),
			json_col JSON,
			enum_col ENUM('active', 'inactive'),
			point_col POINT,
			polygon_col POLYGON,
			geometry_col GEOMETRY,
			bit_col BIT(8)
		)
	`))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	// Insert test data including spatial data
	s.NoError(s.stmt.SetSqlQuery(`
		INSERT INTO test_types VALUES (
			1, 42, 12345, 9876543210, 3.25, 6.75, 'hello world',
			'{"key": "value", "number": 42}', 'active',
			ST_GeomFromText('POINT(1 2)'),
			ST_GeomFromText('POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))'),
			ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'),
			b'10101010'
		)
	`))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	for _, testCase := range []selectCase{
		{
			name:  "boolean",
			query: "SELECT bool_col AS istrue FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "istrue",
					Type:     arrow.PrimitiveTypes.Int8,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "istrue",
						"sql.database_type_name": "TINYINT",
					}),
				},
			}, nil),
			expected: `[{"istrue": 1}]`,
		},
		{
			name:  "tinyint",
			query: "SELECT tinyint_col AS value FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Int8,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "TINYINT",
					}),
				},
			}, nil),
			expected: `[{"value": 42}]`,
		},
		{
			name:  "int32",
			query: "SELECT int_col AS theanswer FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "theanswer",
					Type:     arrow.PrimitiveTypes.Int32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "theanswer",
						"sql.database_type_name": "INT",
					}),
				},
			}, nil),
			expected: `[{"theanswer": 12345}]`,
		},
		{
			name:  "int64",
			query: "SELECT bigint_col AS theanswer FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "theanswer",
					Type:     arrow.PrimitiveTypes.Int64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "theanswer",
						"sql.database_type_name": "BIGINT",
					}),
				},
			}, nil),
			expected: `[{"theanswer": 9876543210}]`,
		},
		{
			name:  "float32",
			query: "SELECT float_col AS value FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Float32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "FLOAT",
						"sql.precision":          "9223372036854775807",
						"sql.scale":              "9223372036854775807",
					}),
				},
			}, nil),
			expected: `[{"value": 3.25}]`,
		},
		{
			name:  "float64",
			query: "SELECT double_col AS value FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Float64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "DOUBLE",
						"sql.precision":          "9223372036854775807",
						"sql.scale":              "9223372036854775807",
					}),
				},
			}, nil),
			expected: `[{"value": 6.75}]`,
		},
		{
			name:  "string",
			query: "SELECT varchar_col AS greeting FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "greeting",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "greeting",
						"sql.database_type_name": "VARCHAR",
					}),
				},
			}, nil),
			expected: `[{"greeting": "hello world"}]`,
		},
		{
			name:  "json",
			query: "SELECT json_col AS data FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "data",
					Type:     func() arrow.DataType { t, _ := extensions.NewJSONType(arrow.BinaryTypes.String); return t }(),
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "data",
						"sql.database_type_name": "JSON",
					}),
				},
			}, nil),
			expected: `[{"data": "{\"key\": \"value\", \"number\": 42}"}]`,
		},
		{
			name:  "enum",
			query: "SELECT enum_col AS status FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "status",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "status",
						"sql.database_type_name": "ENUM",
						"mysql.is_enum_set":      "true",
					}),
				},
			}, nil),
			expected: `[{"status": "active"}]`,
		},
		{
			name:  "point",
			query: "SELECT point_col AS location FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "location",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "location",
						"sql.database_type_name": "GEOMETRY",
						"mysql.is_spatial":       "true",
					}),
				},
			}, nil),
			expected: `[{"location": "AAAAAAEBAAAAAAAAAAAA8D8AAAAAAAAAQA=="}]`,
		},
		{
			name:  "polygon",
			query: "SELECT polygon_col AS area FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "area",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "area",
						"sql.database_type_name": "GEOMETRY",
						"mysql.is_spatial":       "true",
					}),
				},
			}, nil),
			expected: `[{"area": "AAAAAAEDAAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIQAAAAAAAAAhAAAAAAAAACEAAAAAAAAAIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="}]`,
		},
		{
			name:  "geometry",
			query: "SELECT geometry_col AS shape FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "shape",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "shape",
						"sql.database_type_name": "GEOMETRY",
						"mysql.is_spatial":       "true",
					}),
				},
			}, nil),
			expected: `[{"shape": "AAAAAAECAAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADwPwAAAAAAAPA/AAAAAAAAAEAAAAAAAAAAQA=="}]`,
		},
		{
			name:  "bit8",
			query: "SELECT bit_col AS bitvalue FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "bitvalue",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "bitvalue",
						"sql.database_type_name": "BIT",
					}),
				},
			}, nil),
			expected: `[{"bitvalue": "qg=="}]`,
		},
	} {
		s.Run(testCase.name, func() {
			s.NoError(s.stmt.SetSqlQuery(testCase.query))

			rdr, rows, err := s.stmt.ExecuteQuery(s.ctx)
			s.NoError(err)
			defer rdr.Release()

			s.Truef(testCase.schema.Equal(rdr.Schema()), "expected: %s\ngot: %s", testCase.schema, rdr.Schema())
			s.Equal(int64(-1), rows)
			s.Truef(rdr.Next(), "no record, error? %s", rdr.Err())

			expectedRecord, _, err := array.RecordFromJSON(s.Quirks.Alloc(), testCase.schema, bytes.NewReader([]byte(testCase.expected)))
			s.NoError(err)
			defer expectedRecord.Release()

			rec := rdr.RecordBatch()
			s.NotNil(rec)

			s.Truef(array.RecordEqual(expectedRecord, rec), "expected: %s\ngot: %s", expectedRecord, rec)

			s.False(rdr.Next())
			s.NoError(rdr.Err())
		})
	}
}

type MySQLTestSuite struct {
	suite.Suite
	dsn    string
	mem    *memory.CheckedAllocator
	ctx    context.Context
	driver adbc.Driver
	db     adbc.Database
	cnxn   adbc.Connection
	stmt   adbc.Statement
}

func (s *MySQLTestSuite) SetupSuite() {
	var err error
	s.dsn = os.Getenv("MYSQL_DSN")
	if s.dsn == "" {
		s.T().Skip("Set MYSQL_DSN environment variable")
	}

	s.ctx = context.Background()
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)

	s.driver = mysql.NewDriver(s.mem)
	s.db, err = s.driver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: s.dsn,
	})
	s.NoError(err)

	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)

	s.stmt, err = s.cnxn.NewStatement()
	s.NoError(err)
}

func (s *MySQLTestSuite) TearDownSuite() {
	if s.stmt != nil {
		s.NoError(s.stmt.Close())
	}
	if s.cnxn != nil {
		s.NoError(s.cnxn.Close())
	}
	if s.db != nil {
		s.NoError(s.db.Close())
	}
	s.mem.AssertSize(s.T(), 0)
}

func TestMySQLTypeTests(t *testing.T) {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		t.Skip("Set MYSQL_DSN environment variable for type tests")
	}

	quirks := &MySQLQuirks{dsn: dsn}
	suite.Run(t, &MySQLTests{Quirks: quirks})
}

func TestMySQLIntegrationSuite(t *testing.T) {
	suite.Run(t, new(MySQLTestSuite))
}

// TestURIParsing tests the parseToMySQLDSN function with various URI formats
func TestURIParsing(t *testing.T) {
	factory := mysql.NewMySQLDBFactory()

	tests := []struct {
		name          string
		mysqlURI      string
		username      string
		password      string
		expectedDSN   string
		shouldError   bool
		errorContains string
	}{
		// TCP connection variations
		{
			name:        "basic tcp with port",
			mysqlURI:    "mysql://user:pass@localhost:3306/testdb",
			expectedDSN: "user:pass@tcp(localhost:3306)/testdb",
		},
		{
			name:        "tcp without port - should default to 3306",
			mysqlURI:    "mysql://user:pass@localhost/testdb",
			expectedDSN: "user:pass@tcp(localhost:3306)/testdb",
		},
		{
			name:          "tcp without host - should be invalid",
			mysqlURI:      "mysql://user:pass@/testdb",
			shouldError:   true,
			errorContains: "missing hostname in URI",
		},
		{
			name:        "tcp without database",
			mysqlURI:    "mysql://user:pass@localhost:3306",
			expectedDSN: "user:pass@tcp(localhost:3306)/",
		},
		{
			name:        "tcp without database but with slash",
			mysqlURI:    "mysql://user:pass@localhost:3306/",
			expectedDSN: "user:pass@tcp(localhost:3306)/",
		},
		{
			name:        "tcp with custom port",
			mysqlURI:    "mysql://user:pass@example.com:3307/myapp",
			expectedDSN: "user:pass@tcp(example.com:3307)/myapp",
		},
		{
			name:        "tcp with ip address",
			mysqlURI:    "mysql://user:pass@127.0.0.1:3306/testdb",
			expectedDSN: "user:pass@tcp(127.0.0.1:3306)/testdb",
		},
		{
			name:        "tcp with ipv6 host",
			mysqlURI:    "mysql://user:pass@[::1]:3306/testdb",
			expectedDSN: "user:pass@tcp([::1]:3306)/testdb",
		},
		{
			name:        "tcp with ipv6 host, default port",
			mysqlURI:    "mysql://user:pass@[::1]/testdb",
			expectedDSN: "user:pass@tcp([::1]:3306)/testdb",
		},

		// Credential handling variations
		{
			name:        "no credentials in uri",
			mysqlURI:    "mysql://localhost:3306/testdb",
			expectedDSN: "tcp(localhost:3306)/testdb",
		},
		{
			name:        "only username in uri",
			mysqlURI:    "mysql://user@localhost:3306/testdb",
			expectedDSN: "user@tcp(localhost:3306)/testdb",
		},
		{
			name:        "override credentials with options",
			mysqlURI:    "mysql://olduser:oldpass@localhost:3306/testdb",
			username:    "newuser",
			password:    "newpass",
			expectedDSN: "newuser:newpass@tcp(localhost:3306)/testdb",
		},
		{
			name:        "add credentials via options",
			mysqlURI:    "mysql://localhost:3306/testdb",
			username:    "admin",
			password:    "secret",
			expectedDSN: "admin:secret@tcp(localhost:3306)/testdb",
		},
		{
			name:        "override only username",
			mysqlURI:    "mysql://user:pass@localhost:3306/testdb",
			username:    "newuser",
			expectedDSN: "newuser:pass@tcp(localhost:3306)/testdb",
		},
		{
			name:        "override only password",
			mysqlURI:    "mysql://user:pass@localhost:3306/testdb",
			password:    "newpass",
			expectedDSN: "user:newpass@tcp(localhost:3306)/testdb",
		},

		// Query parameter variations
		{
			name:        "single query parameter",
			mysqlURI:    "mysql://user:pass@localhost:3306/testdb?charset=utf8mb4",
			expectedDSN: "user:pass@tcp(localhost:3306)/testdb?charset=utf8mb4",
		},
		{
			name:        "multiple query parameters",
			mysqlURI:    "mysql://user:pass@localhost:3306/testdb?charset=utf8mb4&timeout=30s&tls=false",
			expectedDSN: "user:pass@tcp(localhost:3306)/testdb?charset=utf8mb4&timeout=30s&tls=false",
		},
		{
			name:        "ssl parameters",
			mysqlURI:    "mysql://user:pass@localhost:3306/testdb?tls=skip-verify&timeout=10s",
			expectedDSN: "user:pass@tcp(localhost:3306)/testdb?tls=skip-verify&timeout=10s",
		},
		{
			name:        "url encoded database name",
			mysqlURI:    "mysql://user:pass@localhost:3306/test%20db?charset=utf8",
			expectedDSN: "user:pass@tcp(localhost:3306)/test%20db?charset=utf8",
		},
		{
			name:        "query parameters with encoding",
			mysqlURI:    "mysql://user:pass@localhost/testdb?time_zone=%27%2B00%3A00%27",
			expectedDSN: "user:pass@tcp(localhost:3306)/testdb?time_zone=%27%2B00%3A00%27",
		},

		// Unix socket variations
		{
			name:        "unix socket with parentheses",
			mysqlURI:    "mysql://user:pass@(/tmp/mysql.sock)/testdb",
			expectedDSN: "user:pass@unix(/tmp/mysql.sock)/testdb",
		},
		{
			name:          "unix socket with percent encoding - should be invalid. Must use parenthesis",
			mysqlURI:      "mysql://user:pass@/tmp%2Fmysql.sock/testdb",
			shouldError:   true,
			errorContains: "missing hostname in URI",
		},
		{
			name:        "unix socket with complex path",
			mysqlURI:    "mysql://user:pass@(/var/run/mysqld/mysqld.sock)/myapp",
			expectedDSN: "user:pass@unix(/var/run/mysqld/mysqld.sock)/myapp",
		},
		{
			name:        "unix socket without database",
			mysqlURI:    "mysql://user:pass@(/tmp/mysql.sock)",
			expectedDSN: "user:pass@unix(/tmp/mysql.sock)/",
		},
		{
			name:        "unix socket with query params",
			mysqlURI:    "mysql://user:pass@(/tmp/mysql.sock)/testdb?charset=utf8mb4",
			expectedDSN: "user:pass@unix(/tmp/mysql.sock)/testdb?charset=utf8mb4",
		},
		{
			name:          "unix socket with empty host (ambiguous) - should be invalid",
			mysqlURI:      "mysql://user:pass@/tmp/mysql.sock/testdb",
			shouldError:   true,
			errorContains: "missing hostname in URI",
		},
		{
			name:          "invalid unix socket (missing parenthesis)",
			mysqlURI:      "mysql://user@(/tmp/mysql.sock/testdb",
			shouldError:   true,
			errorContains: "missing closing ')'",
		},
		{
			name:        "unix socket (paren) with encoded db name",
			mysqlURI:    "mysql://user:pass@(/tmp/mysql.sock)/my%20db?foo=bar",
			expectedDSN: "user:pass@unix(/tmp/mysql.sock)/my%20db?foo=bar",
		},
		// Special characters and edge cases
		{
			name:        "credentials with special characters",
			mysqlURI:    "mysql://my%40user:p%40ss%24word@localhost:3306/testdb",
			expectedDSN: "my@user:p@ss$word@tcp(localhost:3306)/testdb",
		},

		// Error cases
		{
			name:          "invalid mysql uri format",
			mysqlURI:      "mysql://[invalid-uri",
			shouldError:   true,
			errorContains: "invalid MySQL URI format",
		},
		{
			name:          "invalid socket path encoding",
			mysqlURI:      "mysql://user:pass@%ZZ%invalid/testdb",
			shouldError:   true,
			errorContains: "invalid MySQL URI format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := map[string]string{
				adbc.OptionKeyURI: tt.mysqlURI,
			}
			if tt.username != "" {
				opts[adbc.OptionKeyUsername] = tt.username
			}
			if tt.password != "" {
				opts[adbc.OptionKeyPassword] = tt.password
			}

			result, err := factory.BuildMySQLDSN(opts)

			if tt.shouldError {
				require.ErrorContains(t, err, tt.errorContains)
				return
			}

			require.NoError(t, err, "unexpected error")
			assert.Equal(t, tt.expectedDSN, result, "DSN should match expected value")
		})
	}
}
