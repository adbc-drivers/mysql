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

package mysql

import (
	"math"
	"os"
	"strings"
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/v8/expr"
	"github.com/substrait-io/substrait-go/v8/literal"
	"github.com/substrait-io/substrait-go/v8/plan"
	"github.com/substrait-io/substrait-go/v8/types"
	"github.com/substrait-io/substrait-protobuf/go/substraitpb"
	extensionspb "github.com/substrait-io/substrait-protobuf/go/substraitpb/extensions"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

var testBaseSchema = types.NamedStruct{
	Names: []string{"id", "name", "weird`col"},
	Struct: types.StructType{
		Nullability: types.NullabilityRequired,
		Types: []types.Type{
			&types.Int64Type{Nullability: types.NullabilityRequired},
			&types.StringType{Nullability: types.NullabilityNullable},
			&types.Float64Type{Nullability: types.NullabilityNullable},
		},
	},
}

// buildPlan serializes a plan built with the substrait-go builder, the way a
// real producer would hand it to the driver.
func buildPlan(t *testing.T, build func(b plan.Builder) (*plan.Plan, error)) []byte {
	t.Helper()
	p, err := build(plan.NewBuilderDefault())
	require.NoError(t, err)
	pb, err := p.ToProto()
	require.NoError(t, err)
	serialized, err := proto.Marshal(pb)
	require.NoError(t, err)
	return serialized
}

func TestSubstraitNamedTable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		build    func(b plan.Builder) (*plan.Plan, error)
		expected string
	}{
		{
			name: "table",
			build: func(b plan.Builder) (*plan.Plan, error) {
				return b.Plan(b.NamedScan([]string{"t"}, testBaseSchema), testBaseSchema.Names)
			},
			expected: "SELECT `id` AS `id`, `name` AS `name`, `weird``col` AS `weird``col` FROM `t`",
		},
		{
			name: "database_table",
			build: func(b plan.Builder) (*plan.Plan, error) {
				return b.Plan(b.NamedScan([]string{"db", "t`x"}, testBaseSchema), testBaseSchema.Names)
			},
			expected: "SELECT `id` AS `id`, `name` AS `name`, `weird``col` AS `weird``col` FROM `db`.`t``x`",
		},
		{
			name: "root_names",
			build: func(b plan.Builder) (*plan.Plan, error) {
				return b.Plan(b.NamedScan([]string{"t"}, testBaseSchema), []string{"a", "b", "c"})
			},
			expected: "SELECT `id` AS `a`, `name` AS `b`, `weird``col` AS `c` FROM `t`",
		},
		{
			name: "emit",
			build: func(b plan.Builder) (*plan.Plan, error) {
				scan, err := b.NamedScan([]string{"t"}, testBaseSchema).Remap(1, 0, 1)
				if err != nil {
					return nil, err
				}
				return b.Plan(scan, []string{"x", "y", "z"})
			},
			expected: "SELECT `name` AS `x`, `id` AS `y`, `name` AS `z` FROM `t`",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query, err := substraitToSQL(buildPlan(t, tc.build))
			require.NoError(t, err)
			assert.Equal(t, tc.expected, query)
		})
	}
}

func namedTableRead(names ...string) *substraitpb.ReadRel {
	return &substraitpb.ReadRel{
		BaseSchema: &substraitpb.NamedStruct{
			Names: []string{"id"},
			Struct: &substraitpb.Type_Struct{Types: []*substraitpb.Type{
				{Kind: &substraitpb.Type_I64_{I64: &substraitpb.Type_I64{}}},
			}},
		},
		ReadType: &substraitpb.ReadRel_NamedTable_{NamedTable: &substraitpb.ReadRel_NamedTable{Names: names}},
	}
}

// columnOfType returns a read of a table with a single column of type t.
func columnOfType(t *substraitpb.Type) *substraitpb.ReadRel {
	read := namedTableRead("t")
	read.BaseSchema.Struct.Types = []*substraitpb.Type{t}
	return read
}

func rootPlan(rel *substraitpb.Rel, names ...string) *substraitpb.Plan {
	return &substraitpb.Plan{Relations: []*substraitpb.PlanRel{{
		RelType: &substraitpb.PlanRel_Root{Root: &substraitpb.RelRoot{Input: rel, Names: names}},
	}}}
}

func readPlan(read *substraitpb.ReadRel, names ...string) *substraitpb.Plan {
	return rootPlan(&substraitpb.Rel{RelType: &substraitpb.Rel_Read{Read: read}}, names...)
}

func TestSubstraitErrors(t *testing.T) {
	for _, tc := range []struct {
		name        string
		plan        func() *substraitpb.Plan
		unsupported bool
	}{
		{
			name: "bare_rel",
			plan: func() *substraitpb.Plan {
				return &substraitpb.Plan{Relations: []*substraitpb.PlanRel{{
					RelType: &substraitpb.PlanRel_Rel{Rel: &substraitpb.Rel{RelType: &substraitpb.Rel_Read{Read: namedTableRead("t")}}},
				}}}
			},
			unsupported: true,
		},
		{
			name: "root_without_names",
			plan: func() *substraitpb.Plan { return readPlan(namedTableRead("t")) },
		},
		{
			name: "no_relations",
			plan: func() *substraitpb.Plan { return &substraitpb.Plan{} },
		},
		{
			name: "multiple_relations",
			plan: func() *substraitpb.Plan {
				p := readPlan(namedTableRead("t"), "id")
				p.Relations = append(p.Relations, p.Relations[0])
				return p
			},
			unsupported: true,
		},
		{
			name: "parameter_bindings",
			plan: func() *substraitpb.Plan {
				p := readPlan(namedTableRead("t"), "id")
				p.ParameterBindings = []*substraitpb.DynamicParameterBinding{{}}
				return p
			},
			unsupported: true,
		},
		{
			name: "project_rel",
			plan: func() *substraitpb.Plan {
				return rootPlan(&substraitpb.Rel{RelType: &substraitpb.Rel_Project{Project: &substraitpb.ProjectRel{}}}, "id")
			},
			unsupported: true,
		},
		{
			name: "virtual_table",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.ReadType = &substraitpb.ReadRel_VirtualTable_{VirtualTable: &substraitpb.ReadRel_VirtualTable{}}
				return readPlan(read, "id")
			},
			unsupported: true,
		},
		{
			name: "projection",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.Projection = &substraitpb.Expression_MaskExpression{}
				return readPlan(read, "id")
			},
			unsupported: true,
		},
		{
			name:        "three_part_name",
			plan:        func() *substraitpb.Plan { return readPlan(namedTableRead("c", "s", "t"), "id") },
			unsupported: true,
		},
		{
			name: "struct_column",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.BaseSchema.Names = []string{"s", "f"}
				read.BaseSchema.Struct.Types = []*substraitpb.Type{{Kind: &substraitpb.Type_Struct_{Struct: &substraitpb.Type_Struct{
					Types: []*substraitpb.Type{{Kind: &substraitpb.Type_I64_{I64: &substraitpb.Type_I64{}}}},
				}}}}
				return readPlan(read, "s", "f")
			},
			unsupported: true,
		},
		{
			// The struct's field adds a name, so this must not be reported as
			// a names/types mismatch
			name: "list_of_struct_column",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.BaseSchema.Names = []string{"l", "f"}
				read.BaseSchema.Struct.Types = []*substraitpb.Type{{Kind: &substraitpb.Type_List_{List: &substraitpb.Type_List{
					Type: &substraitpb.Type{Kind: &substraitpb.Type_Struct_{Struct: &substraitpb.Type_Struct{
						Types: []*substraitpb.Type{{Kind: &substraitpb.Type_I64_{I64: &substraitpb.Type_I64{}}}},
					}}},
				}}}}
				return readPlan(read, "l")
			},
			unsupported: true,
		},
		{
			name: "map_column",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.BaseSchema.Names = []string{"m"}
				read.BaseSchema.Struct.Types = []*substraitpb.Type{{Kind: &substraitpb.Type_Map_{Map: &substraitpb.Type_Map{
					Key:   &substraitpb.Type{Kind: &substraitpb.Type_I64_{I64: &substraitpb.Type_I64{}}},
					Value: &substraitpb.Type{Kind: &substraitpb.Type_I64_{I64: &substraitpb.Type_I64{}}},
				}}}}
				return readPlan(read, "m")
			},
			unsupported: true,
		},
		{
			name: "empty_table_name",
			plan: func() *substraitpb.Plan { return readPlan(namedTableRead("db", ""), "id") },
		},
		{
			name: "no_base_schema",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.BaseSchema = nil
				return readPlan(read, "id")
			},
		},
		{
			name: "base_schema_mismatch",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.BaseSchema.Names = []string{"a", "b"}
				return readPlan(read, "a", "b")
			},
		},
		{
			name: "root_names_mismatch",
			plan: func() *substraitpb.Plan { return readPlan(namedTableRead("t"), "a", "b") },
		},
		{
			name: "duplicate_root_names",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.Common = &substraitpb.RelCommon{EmitKind: &substraitpb.RelCommon_Emit_{
					Emit: &substraitpb.RelCommon_Emit{OutputMapping: []int32{0, 0}},
				}}
				return readPlan(read, "Ж", "ж")
			},
			unsupported: true,
		},
		{
			name: "emit_out_of_range",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.Common = &substraitpb.RelCommon{EmitKind: &substraitpb.RelCommon_Emit_{
					Emit: &substraitpb.RelCommon_Emit{OutputMapping: []int32{1}},
				}}
				return readPlan(read, "id")
			},
		},
		{
			name: "no_input",
			plan: func() *substraitpb.Plan { return rootPlan(nil, "id") },
		},
		{
			name:        "rel_without_type",
			plan:        func() *substraitpb.Plan { return rootPlan(&substraitpb.Rel{}, "id") },
			unsupported: true,
		},
		{
			name: "read_without_type",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.ReadType = nil
				return readPlan(read, "id")
			},
			unsupported: true,
		},
		{
			name: "no_table_name",
			plan: func() *substraitpb.Plan { return readPlan(namedTableRead(), "id") },
		},
		{
			name: "empty_column_name",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.BaseSchema.Names = []string{""}
				return readPlan(read, "id")
			},
		},
		{
			name: "column_without_type",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.BaseSchema.Struct.Types = []*substraitpb.Type{{}}
				return readPlan(read, "id")
			},
			unsupported: true,
		},
		{
			// The alias may stand for a struct, whose field names would
			// otherwise trip the names/types count check
			name: "type_alias_column",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.BaseSchema.Names = []string{"s", "f"}
				read.BaseSchema.Struct.Types = []*substraitpb.Type{{Kind: &substraitpb.Type_Alias{Alias: &substraitpb.Type_TypeAliasReference{}}}}
				return readPlan(read, "s")
			},
			unsupported: true,
		},
		{
			name: "uuid_column",
			plan: func() *substraitpb.Plan {
				return readPlan(columnOfType(&substraitpb.Type{Kind: &substraitpb.Type_Uuid{Uuid: &substraitpb.Type_UUID{}}}), "id")
			},
			unsupported: true,
		},
		{
			name: "deprecated_timestamp_column",
			plan: func() *substraitpb.Plan {
				return readPlan(columnOfType(&substraitpb.Type{Kind: &substraitpb.Type_Timestamp_{Timestamp: &substraitpb.Type_Timestamp{}}}), "id") //nolint:staticcheck // deprecated types must be refused
			},
			unsupported: true,
		},
		{
			name: "interval_column",
			plan: func() *substraitpb.Plan {
				return readPlan(columnOfType(&substraitpb.Type{Kind: &substraitpb.Type_IntervalDay_{IntervalDay: &substraitpb.Type_IntervalDay{}}}), "id")
			},
			unsupported: true,
		},
		{
			name: "user_defined_column",
			plan: func() *substraitpb.Plan {
				return readPlan(columnOfType(&substraitpb.Type{Kind: &substraitpb.Type_UserDefined_{UserDefined: &substraitpb.Type_UserDefined{}}}), "id")
			},
			unsupported: true,
		},
		{
			name: "nanosecond_timestamp_column",
			plan: func() *substraitpb.Plan {
				return readPlan(columnOfType(&substraitpb.Type{Kind: &substraitpb.Type_PrecisionTimestamp_{
					PrecisionTimestamp: &substraitpb.Type_PrecisionTimestamp{Precision: 9},
				}}), "id")
			},
			unsupported: true,
		},
		{
			name: "column_type_variation",
			plan: func() *substraitpb.Plan {
				return readPlan(columnOfType(&substraitpb.Type{Kind: &substraitpb.Type_I64_{I64: &substraitpb.Type_I64{TypeVariationReference: 1}}}), "id")
			},
			unsupported: true,
		},
		{
			name: "filter_without_expression_type",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.Filter = &substraitpb.Expression{}
				return readPlan(read, "id")
			},
			unsupported: true,
		},
		{
			name: "malformed_best_effort_filter",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.BestEffortFilter = &substraitpb.Expression{RexType: &substraitpb.Expression_Selection{
					Selection: &substraitpb.Expression_FieldReference{
						ReferenceType: &substraitpb.Expression_FieldReference_DirectReference{DirectReference: &substraitpb.Expression_ReferenceSegment{
							ReferenceType: &substraitpb.Expression_ReferenceSegment_StructField_{StructField: &substraitpb.Expression_ReferenceSegment_StructField{Field: 5}},
						}},
						RootType: &substraitpb.Expression_FieldReference_RootReference_{RootReference: &substraitpb.Expression_FieldReference_RootReference{}},
					},
				}}
				return readPlan(read, "id")
			},
		},
		{
			name: "filter_rel_without_condition",
			plan: func() *substraitpb.Plan {
				return rootPlan(&substraitpb.Rel{RelType: &substraitpb.Rel_Filter{Filter: &substraitpb.FilterRel{
					Input: &substraitpb.Rel{RelType: &substraitpb.Rel_Read{Read: namedTableRead("t")}},
				}}}, "id")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := planToSQL(tc.plan())
			require.Error(t, err)
			if tc.unsupported {
				assert.ErrorIs(t, err, errSubstraitUnsupported)
			} else {
				assert.ErrorIs(t, err, errSubstraitInvalid)
			}
		})
	}
}

func TestSubstraitMalformedBytes(t *testing.T) {
	for _, serialized := range [][]byte{{0xff, 0xff, 0xff}, nil} {
		_, err := substraitToSQL(serialized)
		assert.ErrorIs(t, err, errSubstraitInvalid)
	}
}

const (
	urnComparison = "extension:io.substrait:functions_comparison"
	urnBoolean    = "extension:io.substrait:functions_boolean"
)

// filterBuilders builds expressions over a scan of testBaseSchema.
type filterBuilders struct {
	t    *testing.T
	b    plan.Builder
	scan plan.Rel
}

func (f filterBuilders) col(idx int32) expr.Expression {
	ref, err := f.b.RootFieldRef(f.scan, idx)
	require.NoError(f.t, err)
	return ref
}

func (f filterBuilders) fn(urn, name string, args ...types.FuncArg) expr.Expression {
	fn, err := f.b.ScalarFn(urn, name, nil, args...)
	require.NoError(f.t, err)
	return fn
}

func lit[T int8 | int16 | int32 | int64 | float32 | float64 | bool | types.Date](v T) expr.Expression {
	return expr.NewPrimitiveLiteral(v, false)
}

func decimalLit(t *testing.T, v string) expr.Expression {
	l, err := literal.NewDecimalFromString(v, false)
	require.NoError(t, err)
	return l
}

// filterPlan builds SELECT id FROM t WHERE <cond> as a FilterRel over a
// NamedTable read of testBaseSchema.
func filterPlan(t *testing.T, cond func(f filterBuilders) expr.Expression) *substraitpb.Plan {
	t.Helper()
	b := plan.NewBuilderDefault()
	f := filterBuilders{t: t, b: b, scan: b.NamedScan([]string{"t"}, testBaseSchema)}
	filter, err := b.Filter(f.scan, cond(f))
	require.NoError(t, err)
	ids, err := filter.Remap(0)
	require.NoError(t, err)
	p, err := b.Plan(ids, []string{"id"})
	require.NoError(t, err)
	pb, err := p.ToProto()
	require.NoError(t, err)
	return pb
}

// filterInput returns the FilterRel of a plan built by filterPlan.
func filterInput(p *substraitpb.Plan) *substraitpb.FilterRel {
	return p.GetRelations()[0].GetRoot().GetInput().GetFilter()
}

func TestSubstraitFilterSQL(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cond     func(f filterBuilders) expr.Expression
		adjust   func(p *substraitpb.Plan)
		expected string
	}{
		{
			name:     "comparison",
			cond:     func(f filterBuilders) expr.Expression { return f.fn(urnComparison, "gt", f.col(0), lit(int64(5))) },
			expected: "SELECT `id` AS `id` FROM `t` WHERE (`id` > 5)",
		},
		{
			name: "negative_literals",
			cond: func(f filterBuilders) expr.Expression {
				return f.fn(urnBoolean, "and",
					f.fn(urnComparison, "lt", f.col(0), lit(int64(-5))),
					f.fn(urnComparison, "equal", f.col(2), lit(-1.5)),
					f.fn(urnComparison, "not_equal", f.col(2), lit(math.Copysign(0, -1))))
			},
			expected: "SELECT `id` AS `id` FROM `t` WHERE ((`id` < (-5)) AND (`weird``col` = (-1.5e+00)) AND (`weird``col` <> (-0e+00)))",
		},
		{
			name: "float32_literal",
			cond: func(f filterBuilders) expr.Expression {
				return f.fn(urnComparison, "equal", lit(float32(0.1)), lit(float32(0.1)))
			},
			expected: "SELECT `id` AS `id` FROM `t` WHERE (1.0000000149011612e-01 = 1.0000000149011612e-01)",
		},
		{
			name: "decimal_literals",
			cond: func(f filterBuilders) expr.Expression {
				return f.fn(urnBoolean, "or",
					f.fn(urnComparison, "equal", decimalLit(f.t, "1.75"), decimalLit(f.t, "9.05")),
					f.fn(urnComparison, "equal", decimalLit(f.t, "-0.05"), decimalLit(f.t, "0.01")),
					f.fn(urnComparison, "equal", decimalLit(f.t, "-12"), decimalLit(f.t, "34")),
					f.fn(urnComparison, "equal", decimalLit(f.t, "99999999999999999999999999999999999999"), decimalLit(f.t, "-99999999999999999999999999999999999999")))
			},
			expected: "SELECT `id` AS `id` FROM `t` WHERE ((1.75 = 9.05) OR ((-0.05) = 0.01) OR ((-12) = 34) OR " +
				"(99999999999999999999999999999999999999 = (-99999999999999999999999999999999999999)))",
		},
		{
			name: "date_literal",
			cond: func(f filterBuilders) expr.Expression {
				return f.fn(urnComparison, "between", lit(types.Date(0)), lit(types.Date(-1)), lit(types.Date(19723)))
			},
			expected: "SELECT `id` AS `id` FROM `t` WHERE (DATE '1970-01-01' BETWEEN DATE '1969-12-31' AND DATE '2024-01-01')",
		},
		{
			name: "null_tests",
			cond: func(f filterBuilders) expr.Expression {
				return f.fn(urnBoolean, "or",
					f.fn(urnComparison, "is_null", f.col(1)),
					f.fn(urnComparison, "is_not_null", f.col(2)),
					f.fn(urnComparison, "is_distinct_from", f.col(2), expr.NewNullLiteral(&types.Float64Type{Nullability: types.NullabilityNullable})),
					f.fn(urnComparison, "is_not_distinct_from", f.col(0), lit(int64(1))))
			},
			expected: "SELECT `id` AS `id` FROM `t` WHERE ((`name` <=> NULL) OR (NOT (`weird``col` <=> NULL)) OR " +
				"(NOT (`weird``col` <=> NULL)) OR (`id` <=> 1))",
		},
		{
			name: "boolean_logic",
			cond: func(f filterBuilders) expr.Expression {
				eq := f.fn(urnComparison, "equal", f.col(0), lit(int64(1)))
				return f.fn(urnBoolean, "and",
					f.fn(urnBoolean, "not", eq),
					f.fn(urnBoolean, "xor", eq, lit(true)),
					f.fn(urnBoolean, "and_not", lit(false), eq),
					f.fn(urnComparison, "is_not_true", eq),
					f.fn(urnComparison, "is_false", eq))
			},
			expected: "SELECT `id` AS `id` FROM `t` WHERE ((NOT (`id` = 1)) AND ((`id` = 1) XOR TRUE) AND " +
				"(FALSE AND (NOT (`id` = 1))) AND ((`id` = 1) IS NOT TRUE) AND ((`id` = 1) IS FALSE))",
		},
		{
			// MySQL's BETWEEN isn't NULL for a NULL bound when the other
			// comparison is false
			name: "between_nullable_bounds",
			cond: func(f filterBuilders) expr.Expression {
				return f.fn(urnComparison, "between", f.col(2), expr.NewNullLiteral(&types.Float64Type{Nullability: types.NullabilityNullable}), lit(0.0))
			},
			expected: "SELECT `id` AS `id` FROM `t` WHERE " +
				"(CASE WHEN (NULL <=> NULL) OR (0e+00 <=> NULL) THEN NULL ELSE (`weird``col` BETWEEN NULL AND 0e+00) END)",
		},
		{
			name: "between_column_bound",
			cond: func(f filterBuilders) expr.Expression {
				return f.fn(urnComparison, "between", lit(1.5), f.col(2), lit(2.5))
			},
			expected: "SELECT `id` AS `id` FROM `t` WHERE " +
				"(CASE WHEN (`weird``col` <=> NULL) OR (2.5e+00 <=> NULL) THEN NULL ELSE (1.5e+00 BETWEEN `weird``col` AND 2.5e+00) END)",
		},
		{
			name:     "empty_and",
			cond:     func(f filterBuilders) expr.Expression { return f.fn(urnBoolean, "and") },
			expected: "SELECT `id` AS `id` FROM `t` WHERE TRUE",
		},
		{
			name:     "empty_or",
			cond:     func(f filterBuilders) expr.Expression { return f.fn(urnBoolean, "or") },
			expected: "SELECT `id` AS `id` FROM `t` WHERE FALSE",
		},
		{
			// The second filter's field reference points into the first
			// filter's emitted columns
			name: "stacked_filters_with_emit",
			cond: func(f filterBuilders) expr.Expression { return f.fn(urnComparison, "is_null", f.col(1)) },
			adjust: func(p *substraitpb.Plan) {
				inner := filterInput(p)
				inner.Common = &substraitpb.RelCommon{EmitKind: &substraitpb.RelCommon_Emit_{
					Emit: &substraitpb.RelCommon_Emit{OutputMapping: []int32{2, 0}},
				}}
				outerCond := proto.Clone(inner.Condition).(*substraitpb.Expression)
				outerCond.GetScalarFunction().Arguments[0].GetValue().GetSelection().GetDirectReference().GetStructField().Field = 0
				p.Relations[0].GetRoot().Input = &substraitpb.Rel{RelType: &substraitpb.Rel_Filter{Filter: &substraitpb.FilterRel{
					Common: &substraitpb.RelCommon{EmitKind: &substraitpb.RelCommon_Emit_{
						Emit: &substraitpb.RelCommon_Emit{OutputMapping: []int32{1}},
					}},
					Input:     p.Relations[0].GetRoot().Input,
					Condition: outerCond,
				}}}
			},
			expected: "SELECT `id` AS `id` FROM `t` WHERE (`name` <=> NULL) AND (`weird``col` <=> NULL)",
		},
		{
			// A best-effort filter that can't be translated is ignored, since
			// the producer filters the results again
			name: "unsupported_best_effort_filter",
			cond: func(f filterBuilders) expr.Expression {
				return f.fn(urnComparison, "equal", f.col(1), expr.NewNullLiteral(&types.StringType{Nullability: types.NullabilityNullable}))
			},
			adjust: func(p *substraitpb.Plan) {
				filter := filterInput(p)
				read := filter.Input.GetRead()
				read.BestEffortFilter = filter.Condition
				read.Filter = &substraitpb.Expression{RexType: &substraitpb.Expression_Literal_{
					Literal: &substraitpb.Expression_Literal{LiteralType: &substraitpb.Expression_Literal_Boolean{Boolean: true}},
				}}
				read.Common = filter.Common
				p.Relations[0].GetRoot().Input = filter.Input
			},
			expected: "SELECT `id` AS `id` FROM `t` WHERE TRUE",
		},
		{
			// A ReadRel's filters reference the base schema, before its emit
			name: "read_filters",
			cond: func(f filterBuilders) expr.Expression { return f.fn(urnComparison, "gt", f.col(0), lit(int64(1))) },
			adjust: func(p *substraitpb.Plan) {
				filter := filterInput(p)
				read := filter.Input.GetRead()
				read.Filter = filter.Condition
				read.BestEffortFilter = proto.Clone(filter.Condition).(*substraitpb.Expression)
				read.Common = &substraitpb.RelCommon{EmitKind: &substraitpb.RelCommon_Emit_{
					Emit: &substraitpb.RelCommon_Emit{OutputMapping: []int32{2}},
				}}
				p.Relations[0].GetRoot().Input = filter.Input
			},
			expected: "SELECT `weird``col` AS `id` FROM `t` WHERE (`id` > 1) AND (`id` > 1)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filterPlan(t, tc.cond)
			if tc.adjust != nil {
				tc.adjust(p)
			}
			query, err := planToSQL(p)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, query)
		})
	}
}

func TestSubstraitFilterErrors(t *testing.T) {
	// eq builds id = 1, which the adjust functions below then break
	eq := func(f filterBuilders) expr.Expression { return f.fn(urnComparison, "equal", f.col(0), lit(int64(1))) }
	condition := func(p *substraitpb.Plan) *substraitpb.Expression { return filterInput(p).Condition }
	function := func(p *substraitpb.Plan) *substraitpb.Expression_ScalarFunction {
		return condition(p).GetScalarFunction()
	}
	arg := func(p *substraitpb.Plan, i int) *substraitpb.Expression {
		return function(p).Arguments[i].GetValue()
	}

	for _, tc := range []struct {
		name        string
		cond        func(f filterBuilders) expr.Expression
		adjust      func(p *substraitpb.Plan)
		unsupported bool
	}{
		{
			name: "string_comparison",
			cond: func(f filterBuilders) expr.Expression {
				return f.fn(urnComparison, "equal", f.col(1), expr.NewNullLiteral(&types.StringType{Nullability: types.NullabilityNullable}))
			},
			unsupported: true,
		},
		{
			name: "string_between",
			cond: func(f filterBuilders) expr.Expression {
				return f.fn(urnComparison, "between", f.col(1), f.col(1), f.col(1))
			},
			unsupported: true,
		},
		{
			name: "mismatched_types",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				arg(p, 1).GetLiteral().LiteralType = &substraitpb.Expression_Literal_I32{I32: 1}
			},
		},
		{
			// A string column compared with an integer is malformed, not
			// just an unsupported string comparison
			name: "mismatched_types_with_string",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				ref := proto.Clone(arg(p, 0)).(*substraitpb.Expression)
				ref.GetSelection().GetDirectReference().GetStructField().Field = 1
				function(p).Arguments[1] = &substraitpb.FunctionArgument{ArgType: &substraitpb.FunctionArgument_Value{Value: ref}}
			},
		},
		{
			// MariaDB would round 0.205 to the other side's scale when using
			// an index
			name: "decimal_scale_mismatch",
			cond: func(f filterBuilders) expr.Expression {
				return f.fn(urnComparison, "equal", decimalLit(f.t, "1.05"), decimalLit(f.t, "2.05"))
			},
			adjust: func(p *substraitpb.Plan) {
				d := arg(p, 1).GetLiteral().GetDecimal()
				d.Precision, d.Scale = 4, 3
			},
		},
		{
			name: "timestamp_precision_mismatch",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				types := filterInput(p).Input.GetRead().BaseSchema.Struct.Types
				types[0] = &substraitpb.Type{Kind: &substraitpb.Type_PrecisionTimestamp_{PrecisionTimestamp: &substraitpb.Type_PrecisionTimestamp{Precision: 6}}}
				types[2] = &substraitpb.Type{Kind: &substraitpb.Type_PrecisionTimestamp_{PrecisionTimestamp: &substraitpb.Type_PrecisionTimestamp{Precision: 3}}}
				ref := proto.Clone(arg(p, 0)).(*substraitpb.Expression)
				ref.GetSelection().GetDirectReference().GetStructField().Field = 2
				function(p).Arguments[1] = &substraitpb.FunctionArgument{ArgType: &substraitpb.FunctionArgument_Value{Value: ref}}
			},
		},
		{
			name: "null_literal_of_unsupported_type",
			cond: func(f filterBuilders) expr.Expression { return f.fn(urnComparison, "is_null", f.col(0)) },
			adjust: func(p *substraitpb.Plan) {
				function(p).Arguments[0] = &substraitpb.FunctionArgument{ArgType: &substraitpb.FunctionArgument_Value{
					Value: &substraitpb.Expression{RexType: &substraitpb.Expression_Literal_{Literal: &substraitpb.Expression_Literal{
						LiteralType: &substraitpb.Expression_Literal_Null{Null: &substraitpb.Type{Kind: &substraitpb.Type_Uuid{Uuid: &substraitpb.Type_UUID{}}}},
					}}},
				}}
			},
			unsupported: true,
		},
		{
			name: "null_literal_type_variation",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				arg(p, 1).GetLiteral().LiteralType = &substraitpb.Expression_Literal_Null{
					Null: &substraitpb.Type{Kind: &substraitpb.Type_I64_{I64: &substraitpb.Type_I64{TypeVariationReference: 1}}},
				}
			},
			unsupported: true,
		},
		{
			// The deprecated field would otherwise make and(false) look like
			// and(), which is TRUE
			name: "deprecated_args",
			cond: func(f filterBuilders) expr.Expression { return f.fn(urnBoolean, "and") },
			adjust: func(p *substraitpb.Plan) {
				function(p).Args = []*substraitpb.Expression{{RexType: &substraitpb.Expression_Literal_{ //nolint:staticcheck // testing that it isn't ignored
					Literal: &substraitpb.Expression_Literal{LiteralType: &substraitpb.Expression_Literal_Boolean{Boolean: false}},
				}}}
			},
			unsupported: true,
		},
		{
			name: "i8_out_of_range",
			cond: func(f filterBuilders) expr.Expression {
				return f.fn(urnComparison, "equal", lit(int8(1)), lit(int8(1)))
			},
			adjust: func(p *substraitpb.Plan) {
				arg(p, 0).GetLiteral().LiteralType = &substraitpb.Expression_Literal_I8{I8: 1000}
			},
		},
		{
			name: "i16_out_of_range",
			cond: func(f filterBuilders) expr.Expression {
				return f.fn(urnComparison, "equal", lit(int16(1)), lit(int16(1)))
			},
			adjust: func(p *substraitpb.Plan) {
				arg(p, 1).GetLiteral().LiteralType = &substraitpb.Expression_Literal_I16{I16: -40000}
			},
		},
		{
			name: "non_boolean_condition",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				filterInput(p).Condition = arg(p, 0)
			},
		},
		{
			name: "non_boolean_output_type",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				function(p).OutputType = &substraitpb.Type{Kind: &substraitpb.Type_I64_{I64: &substraitpb.Type_I64{}}}
			},
		},
		{
			name:   "no_output_type",
			cond:   eq,
			adjust: func(p *substraitpb.Plan) { function(p).OutputType = nil },
		},
		{
			name: "non_boolean_argument",
			cond: func(f filterBuilders) expr.Expression { return f.fn(urnBoolean, "not", lit(true)) },
			adjust: func(p *substraitpb.Plan) {
				function(p).Arguments[0] = &substraitpb.FunctionArgument{ArgType: &substraitpb.FunctionArgument_Value{
					Value: &substraitpb.Expression{RexType: &substraitpb.Expression_Literal_{Literal: &substraitpb.Expression_Literal{
						LiteralType: &substraitpb.Expression_Literal_I64{I64: 1},
					}}},
				}}
			},
		},
		{
			name:   "wrong_arity",
			cond:   eq,
			adjust: func(p *substraitpb.Plan) { function(p).Arguments = function(p).Arguments[:1] },
		},
		{
			name:   "undeclared_function",
			cond:   eq,
			adjust: func(p *substraitpb.Plan) { function(p).FunctionReference = 42 },
		},
		{
			name: "undeclared_urn",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				p.Extensions[0].GetExtensionFunction().ExtensionUrnReference = 42
			},
		},
		{
			name: "duplicate_function_anchor",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				p.Extensions = append(p.Extensions, p.Extensions[0])
			},
		},
		{
			name: "duplicate_urn_anchor",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				p.ExtensionUrns = append(p.ExtensionUrns, p.ExtensionUrns[0])
			},
		},
		{
			name: "unknown_function",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				p.Extensions[0].GetExtensionFunction().Name = "like:str_str"
			},
			unsupported: true,
		},
		{
			name: "unknown_urn",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				p.ExtensionUrns[0].Urn = "extension:example:functions_comparison"
			},
			unsupported: true,
		},
		{
			name: "function_options",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				function(p).Options = []*substraitpb.FunctionOption{{Name: "nan_equality"}}
			},
			unsupported: true,
		},
		{
			name: "enum_argument",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				function(p).Arguments[1] = &substraitpb.FunctionArgument{ArgType: &substraitpb.FunctionArgument_Enum{Enum: "x"}}
			},
			unsupported: true,
		},
		{
			name:   "field_reference_out_of_range",
			cond:   eq,
			adjust: func(p *substraitpb.Plan) { arg(p, 0).GetSelection().GetDirectReference().GetStructField().Field = 3 },
		},
		{
			name: "outer_reference",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				arg(p, 0).GetSelection().RootType = &substraitpb.Expression_FieldReference_OuterReference_{
					OuterReference: &substraitpb.Expression_FieldReference_OuterReference{},
				}
			},
			unsupported: true,
		},
		{
			name: "in_list",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				filterInput(p).Condition = &substraitpb.Expression{RexType: &substraitpb.Expression_SingularOrList_{
					SingularOrList: &substraitpb.Expression_SingularOrList{Value: arg(p, 0), Options: []*substraitpb.Expression{arg(p, 1)}},
				}}
			},
			unsupported: true,
		},
		{
			name:        "infinite_literal",
			cond:        func(f filterBuilders) expr.Expression { return f.fn(urnComparison, "lt", f.col(2), lit(math.Inf(1))) },
			unsupported: true,
		},
		{
			name:        "nan_literal",
			cond:        func(f filterBuilders) expr.Expression { return f.fn(urnComparison, "lt", f.col(2), lit(math.NaN())) },
			unsupported: true,
		},
		{
			name: "date_literal_out_of_range",
			cond: func(f filterBuilders) expr.Expression {
				// 0999-12-31
				return f.fn(urnComparison, "equal", lit(types.Date(-354286)), lit(types.Date(0)))
			},
			unsupported: true,
		},
		{
			name: "decimal_scale_over_30",
			cond: func(f filterBuilders) expr.Expression {
				v := "0." + strings.Repeat("1", 31)
				return f.fn(urnComparison, "equal", decimalLit(f.t, v), decimalLit(f.t, v))
			},
			unsupported: true,
		},
		{
			name: "decimal_wrong_length",
			cond: func(f filterBuilders) expr.Expression {
				return f.fn(urnComparison, "equal", decimalLit(f.t, "1"), decimalLit(f.t, "1"))
			},
			adjust: func(p *substraitpb.Plan) {
				d := arg(p, 0).GetLiteral().GetDecimal()
				d.Value = d.Value[:8]
			},
		},
		{
			name: "decimal_exceeds_precision",
			cond: func(f filterBuilders) expr.Expression {
				return f.fn(urnComparison, "equal", decimalLit(f.t, "100"), decimalLit(f.t, "100"))
			},
			adjust: func(p *substraitpb.Plan) {
				d := arg(p, 0).GetLiteral().GetDecimal()
				d.Precision = 2
			},
		},
		{
			name: "literal_type_variation",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				arg(p, 1).GetLiteral().TypeVariationReference = 1
			},
			unsupported: true,
		},
		{
			name: "untyped_null",
			cond: eq,
			adjust: func(p *substraitpb.Plan) {
				arg(p, 1).GetLiteral().LiteralType = &substraitpb.Expression_Literal_Null{Null: &substraitpb.Type{}}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filterPlan(t, tc.cond)
			if tc.adjust != nil {
				tc.adjust(p)
			}
			_, err := planToSQL(p)
			require.Error(t, err)
			if tc.unsupported {
				assert.ErrorIs(t, err, errSubstraitUnsupported)
			} else {
				assert.ErrorIs(t, err, errSubstraitInvalid)
			}
		})
	}
}

// TestSubstraitDialectTypes checks that checkSupportedType accepts exactly the
// supported_types listed in substrait_dialect.yaml.
func TestSubstraitDialectTypes(t *testing.T) {
	data, err := os.ReadFile("substrait_dialect.yaml")
	require.NoError(t, err)
	var dialect struct {
		SupportedTypes []any `yaml:"supported_types"`
	}
	require.NoError(t, yaml.Unmarshal(data, &dialect))
	// Dialect type names are the upper-cased names of Type's kind fields.
	// Each is mapped to its limits, keyed by the type parameter they limit.
	limitParams := map[string]string{"max_precision": "precision", "max_scale": "scale"}
	listed := map[string]map[string]int32{}
	for _, entry := range dialect.SupportedTypes {
		switch e := entry.(type) {
		case string:
			listed[e] = nil
		case map[string]any:
			name, ok := e["type"].(string)
			require.True(t, ok, "%v", e)
			listed[name] = map[string]int32{}
			for key, value := range e {
				if key == "type" {
					continue
				}
				param, ok := limitParams[key]
				require.True(t, ok, "unexpected %s for %s", key, name)
				listed[name][param] = int32(value.(uint64))
			}
		default:
			t.Fatalf("unexpected supported_types entry %v", entry)
		}
	}

	kinds := (&substraitpb.Type{}).ProtoReflect().Descriptor().Oneofs().ByName("kind").Fields()
	for i := range kinds.Len() {
		field := kinds.Get(i)
		name := strings.ToUpper(string(field.Name()))
		t.Run(name, func(t *testing.T) {
			// typeWith returns the type with one parameter set, if any
			typeWith := func(param string, value int32) *substraitpb.Type {
				typ := &substraitpb.Type{}
				kind := typ.ProtoReflect().NewField(field)
				if param != "" {
					p := kind.Message().Descriptor().Fields().ByName(protoreflect.Name(param))
					require.NotNil(t, p, "%s has no %s", name, param)
					kind.Message().Set(p, protoreflect.ValueOfInt32(value))
				}
				typ.ProtoReflect().Set(field, kind)
				return typ
			}
			limits, ok := listed[name]
			delete(listed, name)
			if !ok {
				assert.ErrorIs(t, checkSupportedType(typeWith("", 0)), errSubstraitUnsupported)
				return
			}
			assert.NoError(t, checkSupportedType(typeWith("", 0)))
			for param, limit := range limits {
				assert.NoError(t, checkSupportedType(typeWith(param, limit)), "%s %d", param, limit)
				assert.ErrorIs(t, checkSupportedType(typeWith(param, limit+1)), errSubstraitUnsupported, "%s %d", param, limit+1)
			}
		})
	}
	assert.Empty(t, listed, "dialect types that aren't Type kinds")
}

// fetchPlan builds a plan over a NamedTable read of testBaseSchema, with the
// relations added by build, and root names.
func fetchPlan(t *testing.T, build func(f filterBuilders) (plan.Rel, error), names ...string) *substraitpb.Plan {
	t.Helper()
	b := plan.NewBuilderDefault()
	rel, err := build(filterBuilders{t: t, b: b, scan: b.NamedScan([]string{"t"}, testBaseSchema)})
	require.NoError(t, err)
	p, err := b.Plan(rel, names)
	require.NoError(t, err)
	pb, err := p.ToProto()
	require.NoError(t, err)
	return pb
}

// fetchRemap adds a FetchRel with an emit over input.
func fetchRemap(b plan.Builder, input plan.Rel, offset, count int64, remap ...int32) (plan.Rel, error) {
	fetch, err := b.Fetch(input, offset, count)
	if err != nil {
		return nil, err
	}
	return fetch.Remap(remap...)
}

// literalExpr wraps a literal in an expression.
func literalExpr(lit *substraitpb.Expression_Literal) *substraitpb.Expression {
	return &substraitpb.Expression{RexType: &substraitpb.Expression_Literal_{Literal: lit}}
}

func TestSubstraitFetchSQL(t *testing.T) {
	allColumns := []string{"a", "b", "c"}
	fetch := func(offset, count int64) func(f filterBuilders) (plan.Rel, error) {
		return func(f filterBuilders) (plan.Rel, error) { return f.b.Fetch(f.scan, offset, count) }
	}
	for _, tc := range []struct {
		name     string
		build    func(f filterBuilders) (plan.Rel, error)
		names    []string
		adjust   func(p *substraitpb.Plan)
		expected string
	}{
		{
			name:     "limit",
			build:    fetch(0, 3),
			names:    allColumns,
			expected: "SELECT `id` AS `a`, `name` AS `b`, `weird``col` AS `c` FROM `t` LIMIT 3",
		},
		{
			name:     "zero_limit",
			build:    fetch(0, 0),
			names:    allColumns,
			expected: "SELECT `id` AS `a`, `name` AS `b`, `weird``col` AS `c` FROM `t` LIMIT 0",
		},
		{
			// MySQL has no OFFSET without LIMIT
			name:     "offset",
			build:    fetch(2, -1),
			names:    allColumns,
			expected: "SELECT `id` AS `a`, `name` AS `b`, `weird``col` AS `c` FROM `t` LIMIT 18446744073709551615 OFFSET 2",
		},
		{
			name:     "limit_and_offset",
			build:    fetch(2, 3),
			names:    allColumns,
			expected: "SELECT `id` AS `a`, `name` AS `b`, `weird``col` AS `c` FROM `t` LIMIT 3 OFFSET 2",
		},
		{
			name:     "all_rows",
			build:    fetch(0, -1),
			names:    allColumns,
			expected: "SELECT `id` AS `a`, `name` AS `b`, `weird``col` AS `c` FROM `t`",
		},
		{
			name:  "expression_bounds",
			build: fetch(0, -1),
			names: allColumns,
			adjust: func(p *substraitpb.Plan) {
				fetch := p.GetRelations()[0].GetRoot().GetInput().GetFetch()
				fetch.OffsetMode = &substraitpb.FetchRel_OffsetExpr{OffsetExpr: literalExpr(&substraitpb.Expression_Literal{
					LiteralType: &substraitpb.Expression_Literal_I32{I32: 4},
				})}
				fetch.CountMode = &substraitpb.FetchRel_CountExpr{CountExpr: literalExpr(&substraitpb.Expression_Literal{
					LiteralType: &substraitpb.Expression_Literal_I64{I64: 5},
				})}
			},
			expected: "SELECT `id` AS `a`, `name` AS `b`, `weird``col` AS `c` FROM `t` LIMIT 5 OFFSET 4",
		},
		{
			// A NULL offset is 0, and a NULL count means all rows
			name:  "null_expression_bounds",
			build: fetch(1, 1),
			names: allColumns,
			adjust: func(p *substraitpb.Plan) {
				fetch := p.GetRelations()[0].GetRoot().GetInput().GetFetch()
				null := literalExpr(&substraitpb.Expression_Literal{LiteralType: &substraitpb.Expression_Literal_Null{
					Null: &substraitpb.Type{Kind: &substraitpb.Type_I64_{I64: &substraitpb.Type_I64{Nullability: substraitpb.Type_NULLABILITY_NULLABLE}}},
				}})
				fetch.OffsetMode = &substraitpb.FetchRel_OffsetExpr{OffsetExpr: null}
				fetch.CountMode = &substraitpb.FetchRel_CountExpr{CountExpr: null}
			},
			expected: "SELECT `id` AS `a`, `name` AS `b`, `weird``col` AS `c` FROM `t`",
		},
		{
			name: "fetch_over_filter",
			build: func(f filterBuilders) (plan.Rel, error) {
				filter, err := f.b.Filter(f.scan, f.fn(urnComparison, "gt", f.col(0), lit(int64(5))))
				if err != nil {
					return nil, err
				}
				return fetchRemap(f.b, filter, 0, 3, 0)
			},
			names:    []string{"id"},
			expected: "SELECT `id` AS `id` FROM `t` WHERE (`id` > 5) LIMIT 3",
		},
		{
			// The filter has to be applied after the LIMIT
			name: "filter_over_fetch",
			build: func(f filterBuilders) (plan.Rel, error) {
				fetch, err := f.b.Fetch(f.scan, 0, 3)
				if err != nil {
					return nil, err
				}
				ref, err := f.b.RootFieldRef(fetch, 0)
				if err != nil {
					return nil, err
				}
				filter, err := f.b.Filter(fetch, f.fn(urnComparison, "gt", ref, lit(int64(5))))
				if err != nil {
					return nil, err
				}
				return filter.Remap(0)
			},
			names: []string{"id"},
			expected: "SELECT `c0` AS `id` FROM (SELECT `id` AS `c0`, `name` AS `c1`, `weird``col` AS `c2` FROM `t` LIMIT 3) AS `sub` " +
				"WHERE (`c0` > 5)",
		},
		{
			name: "fetch_over_fetch",
			build: func(f filterBuilders) (plan.Rel, error) {
				inner, err := f.b.Fetch(f.scan, 1, 5)
				if err != nil {
					return nil, err
				}
				return f.b.Fetch(inner, 2, 10)
			},
			names: allColumns,
			expected: "SELECT `c0` AS `a`, `c1` AS `b`, `c2` AS `c` FROM (SELECT `id` AS `c0`, `name` AS `c1`, `weird``col` AS `c2` FROM `t` " +
				"LIMIT 5 OFFSET 1) AS `sub` LIMIT 10 OFFSET 2",
		},
		{
			// emit can repeat a column, which a subquery can't have twice
			// under the same name
			name: "repeated_column_in_subquery",
			build: func(f filterBuilders) (plan.Rel, error) {
				fetch, err := fetchRemap(f.b, f.scan, 0, 3, 0, 0)
				if err != nil {
					return nil, err
				}
				ref, err := f.b.RootFieldRef(fetch, 1)
				if err != nil {
					return nil, err
				}
				return f.b.Filter(fetch, f.fn(urnComparison, "is_not_null", ref))
			},
			names:    []string{"x", "y"},
			expected: "SELECT `c0` AS `x`, `c1` AS `y` FROM (SELECT `id` AS `c0`, `id` AS `c1` FROM `t` LIMIT 3) AS `sub` WHERE (NOT (`c1` <=> NULL))",
		},
		{
			name: "nested_subqueries",
			build: func(f filterBuilders) (plan.Rel, error) {
				first, err := fetchRemap(f.b, f.scan, 0, 10, 0)
				if err != nil {
					return nil, err
				}
				ref, err := f.b.RootFieldRef(first, 0)
				if err != nil {
					return nil, err
				}
				filter, err := f.b.Filter(first, f.fn(urnComparison, "gt", ref, lit(int64(5))))
				if err != nil {
					return nil, err
				}
				second, err := f.b.Fetch(filter, 0, 2)
				if err != nil {
					return nil, err
				}
				ref, err = f.b.RootFieldRef(second, 0)
				if err != nil {
					return nil, err
				}
				return f.b.Filter(second, f.fn(urnComparison, "lt", ref, lit(int64(8))))
			},
			names: []string{"id"},
			expected: "SELECT `c0` AS `id` FROM (SELECT `c0` AS `c0` FROM (SELECT `id` AS `c0` FROM `t` LIMIT 10) AS `sub` " +
				"WHERE (`c0` > 5) LIMIT 2) AS `sub` WHERE (`c0` < 8)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := fetchPlan(t, tc.build, tc.names...)
			if tc.adjust != nil {
				tc.adjust(p)
			}
			query, err := planToSQL(p)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, query)
		})
	}
}

func TestSubstraitFetchErrors(t *testing.T) {
	i64 := func(v int64) *substraitpb.Expression {
		return literalExpr(&substraitpb.Expression_Literal{LiteralType: &substraitpb.Expression_Literal_I64{I64: v}})
	}
	for _, tc := range []struct {
		name        string
		adjust      func(fetch *substraitpb.FetchRel)
		unsupported bool
	}{
		{
			name:   "negative_offset",
			adjust: func(fetch *substraitpb.FetchRel) { fetch.OffsetMode = &substraitpb.FetchRel_Offset{Offset: -1} }, //nolint:staticcheck // deprecated, but still produced
		},
		{
			// -1 means all rows, but other negative counts are invalid
			name:   "negative_count",
			adjust: func(fetch *substraitpb.FetchRel) { fetch.CountMode = &substraitpb.FetchRel_Count{Count: -2} }, //nolint:staticcheck // deprecated, but still produced
		},
		{
			name: "negative_offset_expression",
			adjust: func(fetch *substraitpb.FetchRel) {
				fetch.OffsetMode = &substraitpb.FetchRel_OffsetExpr{OffsetExpr: i64(-1)}
			},
		},
		{
			name: "negative_count_expression",
			adjust: func(fetch *substraitpb.FetchRel) {
				fetch.CountMode = &substraitpb.FetchRel_CountExpr{CountExpr: i64(-1)}
			},
		},
		{
			name: "non_integer_count",
			adjust: func(fetch *substraitpb.FetchRel) {
				fetch.CountMode = &substraitpb.FetchRel_CountExpr{CountExpr: literalExpr(&substraitpb.Expression_Literal{
					LiteralType: &substraitpb.Expression_Literal_Fp64{Fp64: 3},
				})}
			},
		},
		{
			name: "non_integer_null_count",
			adjust: func(fetch *substraitpb.FetchRel) {
				fetch.CountMode = &substraitpb.FetchRel_CountExpr{CountExpr: literalExpr(&substraitpb.Expression_Literal{
					LiteralType: &substraitpb.Expression_Literal_Null{Null: &substraitpb.Type{Kind: &substraitpb.Type_String_{String_: &substraitpb.Type_String{}}}},
				})}
			},
		},
		{
			name: "null_count_type_variation",
			adjust: func(fetch *substraitpb.FetchRel) {
				fetch.CountMode = &substraitpb.FetchRel_CountExpr{CountExpr: literalExpr(&substraitpb.Expression_Literal{
					LiteralType: &substraitpb.Expression_Literal_Null{Null: &substraitpb.Type{Kind: &substraitpb.Type_I64_{I64: &substraitpb.Type_I64{TypeVariationReference: 7}}}},
				})}
			},
			unsupported: true,
		},
		{
			name: "untyped_null_count",
			adjust: func(fetch *substraitpb.FetchRel) {
				fetch.CountMode = &substraitpb.FetchRel_CountExpr{CountExpr: literalExpr(&substraitpb.Expression_Literal{
					LiteralType: &substraitpb.Expression_Literal_Null{Null: &substraitpb.Type{}},
				})}
			},
		},
		{
			// Possibly from a newer Substrait version
			name: "count_literal_without_type",
			adjust: func(fetch *substraitpb.FetchRel) {
				fetch.CountMode = &substraitpb.FetchRel_CountExpr{CountExpr: literalExpr(&substraitpb.Expression_Literal{})}
			},
			unsupported: true,
		},
		{
			// e.g. an unsigned integer from an extension
			name: "user_defined_count",
			adjust: func(fetch *substraitpb.FetchRel) {
				fetch.CountMode = &substraitpb.FetchRel_CountExpr{CountExpr: literalExpr(&substraitpb.Expression_Literal{
					LiteralType: &substraitpb.Expression_Literal_UserDefined_{UserDefined: &substraitpb.Expression_Literal_UserDefined{}},
				})}
			},
			unsupported: true,
		},
		{
			name: "i8_count_out_of_range",
			adjust: func(fetch *substraitpb.FetchRel) {
				fetch.CountMode = &substraitpb.FetchRel_CountExpr{CountExpr: literalExpr(&substraitpb.Expression_Literal{
					LiteralType: &substraitpb.Expression_Literal_I8{I8: 1000},
				})}
			},
		},
		{
			// MySQL's LIMIT only takes constants
			name: "column_count",
			adjust: func(fetch *substraitpb.FetchRel) {
				fetch.CountMode = &substraitpb.FetchRel_CountExpr{CountExpr: &substraitpb.Expression{RexType: &substraitpb.Expression_Selection{
					Selection: &substraitpb.Expression_FieldReference{
						ReferenceType: &substraitpb.Expression_FieldReference_DirectReference{DirectReference: &substraitpb.Expression_ReferenceSegment{
							ReferenceType: &substraitpb.Expression_ReferenceSegment_StructField_{StructField: &substraitpb.Expression_ReferenceSegment_StructField{}},
						}},
						RootType: &substraitpb.Expression_FieldReference_RootReference_{RootReference: &substraitpb.Expression_FieldReference_RootReference{}},
					},
				}}}
			},
			unsupported: true,
		},
		{
			name: "count_type_variation",
			adjust: func(fetch *substraitpb.FetchRel) {
				fetch.CountMode = &substraitpb.FetchRel_CountExpr{CountExpr: literalExpr(&substraitpb.Expression_Literal{
					LiteralType: &substraitpb.Expression_Literal_I64{I64: 1}, TypeVariationReference: 1,
				})}
			},
			unsupported: true,
		},
		{
			name: "enhancement",
			adjust: func(fetch *substraitpb.FetchRel) {
				fetch.AdvancedExtension = &extensionspb.AdvancedExtension{Enhancement: &anypb.Any{}}
			},
			unsupported: true,
		},
		{
			name:   "no_input",
			adjust: func(fetch *substraitpb.FetchRel) { fetch.Input = nil },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := fetchPlan(t, func(f filterBuilders) (plan.Rel, error) { return f.b.Fetch(f.scan, 0, 3) }, "a", "b", "c")
			tc.adjust(p.GetRelations()[0].GetRoot().GetInput().GetFetch())
			_, err := planToSQL(p)
			require.Error(t, err)
			if tc.unsupported {
				assert.ErrorIs(t, err, errSubstraitUnsupported)
			} else {
				assert.ErrorIs(t, err, errSubstraitInvalid)
			}
		})
	}
}
