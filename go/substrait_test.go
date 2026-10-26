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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/v8/plan"
	"github.com/substrait-io/substrait-go/v8/types"
	"github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
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
			name: "non_read_rel",
			plan: func() *substraitpb.Plan {
				return rootPlan(&substraitpb.Rel{RelType: &substraitpb.Rel_Filter{Filter: &substraitpb.FilterRel{}}}, "id")
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
			name: "filter",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.Filter = &substraitpb.Expression{}
				return readPlan(read, "id")
			},
			unsupported: true,
		},
		{
			name: "best_effort_filter",
			plan: func() *substraitpb.Plan {
				read := namedTableRead("t")
				read.BestEffortFilter = &substraitpb.Expression{}
				return readPlan(read, "id")
			},
			unsupported: true,
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
