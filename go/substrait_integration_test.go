// Copyright (c) 2026 ADBC Drivers Contributors
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
	"fmt"
	"io/fs"
	"math"
	"os"
	"slices"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/goccy/go-yaml"
	substrait "github.com/substrait-io/substrait"
	"github.com/substrait-io/substrait-go/v8/expr"
	substraitext "github.com/substrait-io/substrait-go/v8/extensions"
	"github.com/substrait-io/substrait-go/v8/functions"
	"github.com/substrait-io/substrait-go/v8/plan"
	"github.com/substrait-io/substrait-go/v8/testcases/parser"
	"github.com/substrait-io/substrait-go/v8/types"
	"github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
)

func (s *MySQLTestSuite) TestSubstraitNamedTable() {
	for _, query := range []string{
		"DROP TABLE IF EXISTS substrait_read",
		"CREATE TABLE substrait_read (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(255), value DOUBLE)",
		"INSERT INTO substrait_read VALUES (1, 'a', 1.5), (2, NULL, NULL), (3, 'c', -2.0)",
		"DROP TABLE IF EXISTS secondary.substrait_read",
		"CREATE TABLE secondary.substrait_read (id BIGINT NOT NULL)",
		"INSERT INTO secondary.substrait_read VALUES (42)",
	} {
		s.Require().NoError(s.stmt.SetSqlQuery(s.ctx, query))
		_, err := s.stmt.ExecuteUpdate(s.ctx)
		s.Require().NoError(err)
	}
	defer func() {
		for _, query := range []string{"DROP TABLE substrait_read", "DROP TABLE secondary.substrait_read"} {
			s.NoError(s.stmt.SetSqlQuery(s.ctx, query))
			_, err := s.stmt.ExecuteUpdate(s.ctx)
			s.NoError(err)
		}
	}()

	baseSchema := types.NamedStruct{
		Names: []string{"id", "name", "value"},
		Struct: types.StructType{
			Nullability: types.NullabilityRequired,
			Types: []types.Type{
				&types.Int64Type{Nullability: types.NullabilityRequired},
				&types.StringType{Nullability: types.NullabilityNullable},
				&types.Float64Type{Nullability: types.NullabilityNullable},
			},
		},
	}
	secondarySchema := types.NamedStruct{
		Names: []string{"id"},
		Struct: types.StructType{
			Nullability: types.NullabilityRequired,
			Types:       []types.Type{&types.Int64Type{Nullability: types.NullabilityRequired}},
		},
	}

	for _, tc := range []struct {
		name     string
		build    func(b plan.Builder) (*plan.Plan, error)
		expected string
	}{
		{
			name: "table",
			build: func(b plan.Builder) (*plan.Plan, error) {
				return b.Plan(b.NamedScan([]string{"substrait_read"}, baseSchema), baseSchema.Names)
			},
			expected: "SELECT id, name, value FROM substrait_read",
		},
		{
			name: "database_table",
			build: func(b plan.Builder) (*plan.Plan, error) {
				return b.Plan(b.NamedScan([]string{"secondary", "substrait_read"}, secondarySchema), []string{"x"})
			},
			expected: "SELECT id AS x FROM secondary.substrait_read",
		},
		{
			name: "emit_and_rename",
			build: func(b plan.Builder) (*plan.Plan, error) {
				scan, err := b.NamedScan([]string{"substrait_read"}, baseSchema).Remap(2, 0)
				if err != nil {
					return nil, err
				}
				return b.Plan(scan, []string{"v", "i"})
			},
			expected: "SELECT value AS v, id AS i FROM substrait_read",
		},
	} {
		s.Run(tc.name, func() {
			p, err := tc.build(plan.NewBuilderDefault())
			s.Require().NoError(err)
			pb, err := p.ToProto()
			s.Require().NoError(err)
			serialized, err := proto.Marshal(pb)
			s.Require().NoError(err)

			stmt, err := s.cnxn.NewStatement(s.ctx)
			s.Require().NoError(err)
			defer func() { s.NoError(stmt.Close(s.ctx)) }()

			s.Require().NoError(stmt.SetSqlQuery(s.ctx, tc.expected))
			expectedRdr, _, err := stmt.ExecuteQuery(s.ctx)
			s.Require().NoError(err)
			s.Require().True(expectedRdr.Next())
			expected := expectedRdr.RecordBatch()
			expected.Retain()
			defer expected.Release()
			expectedRdr.Release()

			s.Require().NoError(stmt.SetSubstraitPlan(s.ctx, serialized))
			schema, err := stmt.(adbc.StatementExecuteSchema).ExecuteSchema(s.ctx)
			s.Require().NoError(err)
			s.Truef(expected.Schema().Equal(schema), "expected: %s\ngot: %s", expected.Schema(), schema)

			rdr, _, err := stmt.ExecuteQuery(s.ctx)
			s.Require().NoError(err)
			defer rdr.Release()
			s.Require().True(rdr.Next())
			got := rdr.RecordBatch()
			// Both queries scan the same table in primary key order
			s.Truef(array.RecordEqual(expected, got), "expected: %s\ngot: %s", expected, got)
			s.False(rdr.Next())
			s.NoError(rdr.Err())
		})
	}

	s.Run("unsupported", func() {
		p, err := plan.NewBuilderDefault().Plan(
			plan.NewBuilderDefault().NamedScan([]string{"a", "b", "c"}, secondarySchema), []string{"id"})
		s.Require().NoError(err)
		pb, err := p.ToProto()
		s.Require().NoError(err)
		serialized, err := proto.Marshal(pb)
		s.Require().NoError(err)

		// A rejected plan must not leave the previous query in place
		s.Require().NoError(s.stmt.SetSqlQuery(s.ctx, "SELECT 1"))
		var adbcErr adbc.Error
		s.Require().ErrorAs(s.stmt.SetSubstraitPlan(s.ctx, serialized), &adbcErr)
		s.Equal(adbc.StatusNotImplemented, adbcErr.Code)
		_, _, err = s.stmt.ExecuteQuery(s.ctx)
		s.Error(err)
	})

	s.Run("malformed", func() {
		var adbcErr adbc.Error
		s.Require().ErrorAs(s.stmt.SetSubstraitPlan(s.ctx, []byte{0xff, 0xff, 0xff}), &adbcErr)
		s.Equal(adbc.StatusInvalidArgument, adbcErr.Code)
	})
}

// substraitPlan serializes a plan, optionally adjusting its protobuf form
// first.
func (s *MySQLTestSuite) substraitPlan(p *plan.Plan, adjust func(*substraitpb.Plan)) []byte {
	pb, err := p.ToProto()
	s.Require().NoError(err)
	if adjust != nil {
		adjust(pb)
	}
	serialized, err := proto.Marshal(pb)
	s.Require().NoError(err)
	return serialized
}

// substraitIDs runs a plan whose single output column is a BIGINT id.
func (s *MySQLTestSuite) substraitIDs(serialized []byte) ([]int64, error) {
	stmt, err := s.cnxn.NewStatement(s.ctx)
	s.Require().NoError(err)
	defer func() { s.NoError(stmt.Close(s.ctx)) }()
	if err := stmt.SetSubstraitPlan(s.ctx, serialized); err != nil {
		return nil, err
	}
	rdr, _, err := stmt.ExecuteQuery(s.ctx)
	s.Require().NoError(err)
	defer rdr.Release()
	ids := []int64{}
	for rdr.Next() {
		ids = append(ids, rdr.RecordBatch().Column(0).(*array.Int64).Int64Values()...)
	}
	s.Require().NoError(rdr.Err())
	return ids, nil
}

func (s *MySQLTestSuite) TestSubstraitFilter() {
	for _, query := range []string{
		"DROP TABLE IF EXISTS substrait_filter",
		"CREATE TABLE substrait_filter (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(20), f FLOAT, d DATE)",
		"INSERT INTO substrait_filter VALUES (1, 'abc', 0.1, '2024-01-01'), (2, 'ABC', 1.5, '2024-06-01'), (3, 'abc ', NULL, NULL), (4, NULL, 2.5, '2023-12-31'), (5, 'ábc', -1, '2025-01-01')",
	} {
		s.Require().NoError(s.stmt.SetSqlQuery(s.ctx, query))
		_, err := s.stmt.ExecuteUpdate(s.ctx)
		s.Require().NoError(err)
	}
	defer func() {
		s.NoError(s.stmt.SetSqlQuery(s.ctx, "DROP TABLE substrait_filter"))
		_, err := s.stmt.ExecuteUpdate(s.ctx)
		s.NoError(err)
	}()

	baseSchema := types.NamedStruct{
		Names: []string{"id", "name", "f", "d"},
		Struct: types.StructType{
			Nullability: types.NullabilityRequired,
			Types: []types.Type{
				&types.Int64Type{Nullability: types.NullabilityRequired},
				&types.StringType{Nullability: types.NullabilityNullable},
				&types.Float32Type{Nullability: types.NullabilityNullable},
				&types.DateType{Nullability: types.NullabilityNullable},
			},
		},
	}
	const (
		cmp     = substraitext.SubstraitDefaultURNPrefix + "functions_comparison"
		boolean = substraitext.SubstraitDefaultURNPrefix + "functions_boolean"
	)
	date := func(v string) expr.Expression {
		d, err := time.Parse(time.DateOnly, v)
		s.Require().NoError(err)
		return expr.NewPrimitiveLiteral(types.Date(d.Unix()/86400), false)
	}
	type builders struct {
		col func(int32) expr.Expression
		fn  func(string, string, ...types.FuncArg) expr.Expression
	}

	for _, tc := range []struct {
		name string
		cond func(b builders) expr.Expression
		// inRead puts the condition in the ReadRel's filter instead of a
		// FilterRel, and inBestEffort in its best_effort_filter
		inRead       bool
		inBestEffort bool
		expected     []int64
		unsupported  bool
	}{
		{
			name:     "is_null",
			cond:     func(b builders) expr.Expression { return b.fn(cmp, "is_null", b.col(1)) },
			expected: []int64{4},
		},
		{
			name: "float_equal",
			cond: func(b builders) expr.Expression {
				return b.fn(cmp, "equal", b.col(2), expr.NewPrimitiveLiteral(float32(0.1), false))
			},
			expected: []int64{1},
		},
		{
			name: "date_between",
			cond: func(b builders) expr.Expression {
				return b.fn(cmp, "between", b.col(3), date("2024-01-01"), date("2024-12-31"))
			},
			expected: []int64{1, 2},
		},
		{
			name: "is_distinct_from_null",
			cond: func(b builders) expr.Expression {
				return b.fn(cmp, "is_distinct_from", b.col(2), expr.NewNullLiteral(&types.Float32Type{Nullability: types.NullabilityNullable}))
			},
			expected: []int64{1, 2, 4, 5},
		},
		{
			// Row 3 is NULL on both sides of the OR, so it is dropped
			name: "boolean_logic",
			cond: func(b builders) expr.Expression {
				return b.fn(boolean, "or",
					b.fn(boolean, "not", b.fn(cmp, "gte", b.col(2), expr.NewPrimitiveLiteral(float32(0), false))),
					b.fn(boolean, "and", b.fn(cmp, "gt", b.col(0), expr.NewPrimitiveLiteral(int64(1), false)), b.fn(cmp, "lt", b.col(3), date("2024-12-31"))))
			},
			expected: []int64{2, 4, 5},
		},
		{
			name: "read_filter",
			cond: func(b builders) expr.Expression {
				return b.fn(cmp, "lte", b.col(0), expr.NewPrimitiveLiteral(int64(2), false))
			},
			inRead:   true,
			expected: []int64{1, 2},
		},
		{
			// MySQL's BETWEEN would be FALSE, not NULL, for rows with f > 0
			name: "between_null_literal_bound",
			cond: func(b builders) expr.Expression {
				return b.fn(boolean, "not", b.fn(cmp, "between", b.col(2), expr.NewNullLiteral(&types.Float32Type{Nullability: types.NullabilityNullable}), expr.NewPrimitiveLiteral(float32(0), false)))
			},
			expected: []int64{},
		},
		{
			// Row 3's f is NULL, so between is NULL and the row is dropped
			name: "between_null_column_bound",
			cond: func(b builders) expr.Expression {
				return b.fn(boolean, "not", b.fn(cmp, "between", expr.NewPrimitiveLiteral(float32(1), false), b.col(2), expr.NewPrimitiveLiteral(float32(0), false)))
			},
			expected: []int64{1, 2, 4, 5},
		},
		{
			name: "between_null_upper_bound",
			cond: func(b builders) expr.Expression {
				return b.fn(boolean, "not", b.fn(cmp, "between", b.col(0), expr.NewPrimitiveLiteral(int64(5), false), expr.NewNullLiteral(&types.Int64Type{Nullability: types.NullabilityNullable})))
			},
			expected: []int64{},
		},
		{
			// Can't be translated, so it's ignored and all rows are returned
			name: "unsupported_best_effort_filter",
			cond: func(b builders) expr.Expression {
				return b.fn(cmp, "equal", b.col(1), expr.NewPrimitiveLiteral("abc", false))
			},
			inBestEffort: true,
			expected:     []int64{1, 2, 3, 4, 5},
		},
		{
			name: "string_equal",
			cond: func(b builders) expr.Expression {
				return b.fn(cmp, "equal", b.col(1), expr.NewPrimitiveLiteral("abc", false))
			},
			unsupported: true,
		},
	} {
		s.Run(tc.name, func() {
			b := plan.NewBuilderDefault()
			scan := b.NamedScan([]string{"substrait_filter"}, baseSchema)
			cond := tc.cond(builders{
				col: func(idx int32) expr.Expression {
					ref, err := b.RootFieldRef(scan, idx)
					s.Require().NoError(err)
					return ref
				},
				fn: func(urn, name string, args ...types.FuncArg) expr.Expression {
					f, err := b.ScalarFn(urn, name, nil, args...)
					s.Require().NoError(err)
					return f
				},
			})
			filter, err := b.Filter(scan, cond)
			s.Require().NoError(err)
			ids, err := filter.Remap(0)
			s.Require().NoError(err)
			p, err := b.Plan(ids, []string{"id"})
			s.Require().NoError(err)
			serialized := s.substraitPlan(p, func(pb *substraitpb.Plan) {
				if tc.inRead || tc.inBestEffort {
					// Move the condition into the ReadRel, keeping the emit
					filterRel := pb.Relations[0].GetRoot().Input.GetFilter()
					read := filterRel.Input.GetRead()
					if tc.inRead {
						read.Filter = filterRel.Condition
					} else {
						read.BestEffortFilter = filterRel.Condition
					}
					read.Common = filterRel.Common
					pb.Relations[0].GetRoot().Input = filterRel.Input
				}
			})

			got, err := s.substraitIDs(serialized)
			if tc.unsupported {
				var adbcErr adbc.Error
				s.Require().ErrorAs(err, &adbcErr)
				s.Equal(adbc.StatusNotImplemented, adbcErr.Code)
				return
			}
			s.Require().NoError(err)
			slices.Sort(got)
			s.Equal(tc.expected, got)
		})
	}
}

// TestSubstraitZeroDate checks null tests on a NOT NULL DATE column holding
// a zero date, which MySQL's IS NULL treats as NULL.
func (s *MySQLTestSuite) TestSubstraitZeroDate() {
	for _, query := range []string{
		"DROP TABLE IF EXISTS substrait_zero_date",
		"CREATE TABLE substrait_zero_date (id BIGINT NOT NULL PRIMARY KEY, d DATE NOT NULL)",
		// IGNORE allows zero dates despite NO_ZERO_DATE and strict mode
		"INSERT IGNORE INTO substrait_zero_date VALUES (1, '0000-00-00'), (2, '2024-01-01')",
	} {
		s.Require().NoError(s.stmt.SetSqlQuery(s.ctx, query))
		_, err := s.stmt.ExecuteUpdate(s.ctx)
		s.Require().NoError(err)
	}
	defer func() {
		s.NoError(s.stmt.SetSqlQuery(s.ctx, "DROP TABLE substrait_zero_date"))
		_, err := s.stmt.ExecuteUpdate(s.ctx)
		s.NoError(err)
	}()

	schema := types.NamedStruct{
		Names: []string{"id", "d"},
		Struct: types.StructType{
			Nullability: types.NullabilityRequired,
			Types: []types.Type{
				&types.Int64Type{Nullability: types.NullabilityRequired},
				&types.DateType{Nullability: types.NullabilityRequired},
			},
		},
	}
	for _, tc := range []struct {
		fn       string
		expected []int64
	}{
		{"is_null", []int64{}},
		{"is_not_null", []int64{1, 2}},
	} {
		s.Run(tc.fn, func() {
			b := plan.NewBuilderDefault()
			scan := b.NamedScan([]string{"substrait_zero_date"}, schema)
			ref, err := b.RootFieldRef(scan, 1)
			s.Require().NoError(err)
			cond, err := b.ScalarFn(substraitext.SubstraitDefaultURNPrefix+"functions_comparison", tc.fn, nil, ref)
			s.Require().NoError(err)
			filter, err := b.Filter(scan, cond)
			s.Require().NoError(err)
			ids, err := filter.Remap(0)
			s.Require().NoError(err)
			p, err := b.Plan(ids, []string{"id"})
			s.Require().NoError(err)

			got, err := s.substraitIDs(s.substraitPlan(p, nil))
			s.Require().NoError(err)
			slices.Sort(got)
			s.Equal(tc.expected, got)
		})
	}
}

// substraitDialectFunctions reads the scalar function implementations listed
// in substrait_dialect.yaml, keyed the way substrait-go identifies function
// variants (e.g. "equal:any_any").
//
// TODO: use substrait-go's dialect support (functions.LoadDialect) instead
// once we can upgrade to a version that reads the dialect format defined in
// the Substrait specification. v8 only reads an older, incompatible format.
func substraitDialectFunctions() (map[substraitext.ID]bool, error) {
	data, err := os.ReadFile("substrait_dialect.yaml")
	if err != nil {
		return nil, err
	}
	var dialect struct {
		Dependencies             map[string]string `yaml:"dependencies"`
		SupportedScalarFunctions []struct {
			Source         string   `yaml:"source"`
			Name           string   `yaml:"name"`
			SupportedImpls []string `yaml:"supported_impls"`
		} `yaml:"supported_scalar_functions"`
	}
	if err := yaml.Unmarshal(data, &dialect); err != nil {
		return nil, err
	}
	functions := map[substraitext.ID]bool{}
	for _, fn := range dialect.SupportedScalarFunctions {
		urn, ok := dialect.Dependencies[fn.Source]
		if !ok {
			return nil, fmt.Errorf("function %s uses undeclared dependency %q", fn.Name, fn.Source)
		}
		for _, impl := range fn.SupportedImpls {
			functions[substraitext.ID{URN: urn, Name: fn.Name + ":" + impl}] = true
		}
	}
	return functions, nil
}

// substraitCaseException marks Substrait test cases whose outcome differs
// from what substrait_dialect.yaml implies.
type substraitCaseException struct {
	reason string
	match  func(tc *parser.TestCase) bool
	// skip means the case isn't run at all; otherwise the translator must
	// refuse it
	skip bool
}

var substraitCaseExceptions = []substraitCaseException{
	{
		// The dialect can't express this: listing FP32/FP64 implies all
		// their literals are supported
		reason: "MySQL has no infinity or NaN literals",
		match: func(tc *parser.TestCase) bool {
			// The expected result is a literal in the generated query too
			return slices.ContainsFunc(append([]*parser.CaseLiteral{tc.Result}, tc.Args...), func(arg *parser.CaseLiteral) bool {
				switch v := arg.Value.(type) {
				case *expr.PrimitiveLiteral[float32]:
					return math.IsInf(float64(v.Value), 0) || math.IsNaN(float64(v.Value))
				case *expr.PrimitiveLiteral[float64]:
					return math.IsInf(v.Value, 0) || math.IsNaN(v.Value)
				}
				return false
			})
		},
	},
	{
		// Can't be compared with the expected result, since the types differ
		reason: "the test case expects a boolean result from a function returning decimal",
		match: func(tc *parser.TestCase) bool {
			return tc.String() == "nullif(null::decimal?<38,0>, null::decimal?<38,0>) = null::boolean?"
		},
		skip: true,
	},
}

// TestSubstraitFunctionCases runs the comparison and boolean function test
// cases from the Substrait specification as filters over a one-row table:
// each case f(args) = result becomes the condition
// is_not_distinct_from(f(args), result), so the row is returned exactly when
// the function gives the expected result, including NULL. Cases whose function implementation is listed in substrait_dialect.yaml
// must give the expected result, and all others must be rejected as
// unsupported, unless an entry in substraitCaseExceptions says otherwise.
// This also keeps the dialect in sync with the translator.
func (s *MySQLTestSuite) TestSubstraitFunctionCases() {
	for _, query := range []string{
		"DROP TABLE IF EXISTS substrait_one",
		"CREATE TABLE substrait_one (id BIGINT NOT NULL PRIMARY KEY)",
		"INSERT INTO substrait_one VALUES (1)",
	} {
		s.Require().NoError(s.stmt.SetSqlQuery(s.ctx, query))
		_, err := s.stmt.ExecuteUpdate(s.ctx)
		s.Require().NoError(err)
	}
	defer func() {
		s.NoError(s.stmt.SetSqlQuery(s.ctx, "DROP TABLE substrait_one"))
		_, err := s.stmt.ExecuteUpdate(s.ctx)
		s.NoError(err)
	}()
	schema := types.NamedStruct{
		Names: []string{"id"},
		Struct: types.StructType{
			Nullability: types.NullabilityRequired,
			Types:       []types.Type{&types.Int64Type{Nullability: types.NullabilityRequired}},
		},
	}
	dialect, err := substraitDialectFunctions()
	s.Require().NoError(err)
	collection := substraitext.GetDefaultCollectionWithNoError()
	funcRegistry := functions.NewFunctionRegistry(collection)
	testsFS := substrait.GetSubstraitTestsFS()
	// Outcomes per function variant, and the number of cases each exception
	// matched
	passed := map[substraitext.ID]int{}
	refused := map[substraitext.ID]int{}
	exceptionMatches := make([]int, len(substraitCaseExceptions))
	for _, dir := range []string{"tests/cases/comparison", "tests/cases/boolean"} {
		entries, err := fs.ReadDir(testsFS, dir)
		s.Require().NoError(err)
		for _, entry := range entries {
			path := dir + "/" + entry.Name()
			file, err := parser.ParseTestCaseFileFromFS(testsFS, path)
			s.Require().NoError(err, path)
			for _, tc := range file.TestCases {
				if tc.FuncType != parser.ScalarFuncType {
					s.Run(tc.String(), func() { s.T().Skip("aggregate functions are not supported yet") })
					continue
				}
				var exception *substraitCaseException
				for i := range substraitCaseExceptions {
					if substraitCaseExceptions[i].match(tc) {
						exceptionMatches[i]++
						exception = &substraitCaseExceptions[i]
						break
					}
				}

				s.Run(tc.String(), func() {
					if exception != nil && exception.skip {
						s.T().Skip(exception.reason)
					}
					b := plan.NewBuilderDefault()
					scan := b.NamedScan([]string{"substrait_one"}, schema)
					reg := b.GetExprBuilder().Reg
					fn, err := tc.GetScalarFunctionInvocation(&reg, funcRegistry)
					s.Require().NoError(err)
					// Function options aren't supported, and a dialect lists
					// the ones it requires
					inDialect := dialect[fn.ID()] && len(tc.Options) == 0

					cond, err := b.ScalarFn(substraitext.SubstraitDefaultURNPrefix+"functions_comparison", "is_not_distinct_from", nil, fn, tc.Result.Value)
					s.Require().NoError(err)
					filter, err := b.Filter(scan, cond)
					s.Require().NoError(err)
					p, err := b.Plan(filter, []string{"id"})
					s.Require().NoError(err)

					ids, err := s.substraitIDs(s.substraitPlan(p, nil))
					if !inDialect || exception != nil {
						reason := "not in substrait_dialect.yaml"
						if exception != nil {
							reason = exception.reason
						}
						var adbcErr adbc.Error
						s.Require().ErrorAs(err, &adbcErr, "expected the case to be refused: %s", reason)
						s.Require().Equal(adbc.StatusNotImplemented, adbcErr.Code, "%s", err)
						refused[fn.ID()]++
						return
					}
					s.Require().NoError(err, "%s is in substrait_dialect.yaml", fn.ID())
					s.Equal([]int64{1}, ids, "result differs from %s", tc.Result)
					passed[fn.ID()]++
				})
			}
		}
	}

	// The harness checks results with is_not_distinct_from, which is
	// translated by the code under test, so make sure a wrong expected value
	// is caught
	s.Run("negative_control", func() {
		b := plan.NewBuilderDefault()
		scan := b.NamedScan([]string{"substrait_one"}, schema)
		const cmp = substraitext.SubstraitDefaultURNPrefix + "functions_comparison"
		eq, err := b.ScalarFn(cmp, "equal", nil, expr.NewPrimitiveLiteral(int64(1), false), expr.NewPrimitiveLiteral(int64(1), false))
		s.Require().NoError(err)
		for _, tc := range []struct {
			expected expr.Expression
			ids      []int64
		}{
			{expr.NewPrimitiveLiteral(true, false), []int64{1}},
			{expr.NewPrimitiveLiteral(false, false), []int64{}},
			{expr.NewNullLiteral(&types.BooleanType{Nullability: types.NullabilityNullable}), []int64{}},
		} {
			cond, err := b.ScalarFn(cmp, "is_not_distinct_from", nil, eq, tc.expected)
			s.Require().NoError(err)
			filter, err := b.Filter(scan, cond)
			s.Require().NoError(err)
			p, err := b.Plan(filter, []string{"id"})
			s.Require().NoError(err)
			ids, err := s.substraitIDs(s.substraitPlan(p, nil))
			s.Require().NoError(err)
			s.Equal(tc.ids, ids, "equal(1, 1) compared with %s", tc.expected)
		}
	})

	for i, exception := range substraitCaseExceptions {
		s.Positivef(exceptionMatches[i], "exception %q matches no test cases", exception.reason)
	}
	// Variants without any test cases upstream, such as is_distinct_from,
	// are only covered by TestSubstraitFilter
	for id := range refused {
		if dialect[id] {
			s.Positivef(passed[id], "every test case for %s was refused", id)
		}
	}
	s.T().Logf("Substrait function cases passed: %v", passed)
	s.T().Logf("Substrait function cases refused: %v", refused)
}
