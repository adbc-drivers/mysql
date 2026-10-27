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
	"errors"
	"fmt"
	"slices"
	"strings"

	substraitpb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
)

// Errors returned by the Substrait translator. errSubstraitUnsupported marks
// valid plans that use features the translator does not handle yet;
// errSubstraitInvalid marks malformed plans.
var (
	errSubstraitUnsupported = errors.New("unsupported Substrait feature")
	errSubstraitInvalid     = errors.New("invalid Substrait plan")
)

func substraitUnsupported(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errSubstraitUnsupported, fmt.Sprintf(format, args...))
}

func substraitInvalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errSubstraitInvalid, fmt.Sprintf(format, args...))
}

// substraitToSQL decodes a serialized Substrait plan and translates it into a
// MySQL query.
func substraitToSQL(serialized []byte) (string, error) {
	var plan substraitpb.Plan
	if err := proto.Unmarshal(serialized, &plan); err != nil {
		return "", substraitInvalid("failed to decode plan: %s", err)
	}
	return planToSQL(&plan)
}

// selectQuery is the SQL form of a relation: a single-table SELECT whose
// output columns reference the table's columns. Identifiers are stored
// unquoted; toSQL quotes them.
type selectQuery struct {
	table   []string
	columns []string
}

// toSQL renders q, aliasing each output column to the corresponding name.
func (q *selectQuery) toSQL(names []string) string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	for i, col := range q.columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteIdentifier(col))
		sb.WriteString(" AS ")
		sb.WriteString(quoteIdentifier(names[i]))
	}
	sb.WriteString(" FROM ")
	for i, part := range q.table {
		if i > 0 {
			sb.WriteString(".")
		}
		sb.WriteString(quoteIdentifier(part))
	}
	return sb.String()
}

// planToSQL translates a Substrait plan into a MySQL query. Currently only a
// ReadRel over a NamedTable is supported.
func planToSQL(plan *substraitpb.Plan) (string, error) {
	if len(plan.GetParameterBindings()) > 0 {
		return "", substraitUnsupported("parameter bindings")
	}
	if plan.GetAdvancedExtensions().GetEnhancement() != nil {
		return "", substraitUnsupported("plan advanced extension enhancements")
	}
	switch len(plan.GetRelations()) {
	case 0:
		return "", substraitInvalid("plan has no relations")
	case 1:
	default:
		return "", substraitUnsupported("plans with %d relations (expected exactly 1)", len(plan.GetRelations()))
	}

	var root *substraitpb.RelRoot
	switch r := plan.GetRelations()[0].GetRelType().(type) {
	case *substraitpb.PlanRel_Root:
		root = r.Root
	case *substraitpb.PlanRel_Rel:
		// A bare Rel is meant for references and CTEs, not as the query root
		return "", substraitUnsupported("plans without a RelRoot")
	default:
		// Also covers relation types added in newer Substrait versions, which
		// decode as unknown fields with no relation type set
		return "", substraitUnsupported("plan relation type %T", r)
	}

	q, err := relToQuery(root.GetInput())
	if err != nil {
		return "", err
	}
	// Nested columns aren't supported, so every output field has exactly one name
	if len(root.GetNames()) != len(q.columns) {
		return "", substraitInvalid("root has %d names but relation produces %d columns", len(root.GetNames()), len(q.columns))
	}
	// MySQL compares column names case-insensitively, and duplicate names
	// break queries that wrap this one, such as ExecuteSchema's
	seen := make(map[string]bool, len(root.GetNames()))
	for _, name := range root.GetNames() {
		key := strings.ToLower(name)
		if seen[key] {
			return "", substraitUnsupported("duplicate output column name %q (names are case-insensitive in MySQL)", name)
		}
		seen[key] = true
	}
	return q.toSQL(root.GetNames()), nil
}

func relToQuery(rel *substraitpb.Rel) (*selectQuery, error) {
	if rel == nil {
		return nil, substraitInvalid("missing relation")
	}
	switch r := rel.GetRelType().(type) {
	case *substraitpb.Rel_Read:
		return readRelToQuery(r.Read)
	default:
		return nil, substraitUnsupported("relation type %T", rel.GetRelType())
	}
}

func readRelToQuery(read *substraitpb.ReadRel) (*selectQuery, error) {
	if read.GetFilter() != nil {
		return nil, substraitUnsupported("ReadRel filter")
	}
	if read.GetBestEffortFilter() != nil {
		return nil, substraitUnsupported("ReadRel best_effort_filter")
	}
	if read.GetProjection() != nil {
		return nil, substraitUnsupported("ReadRel projection")
	}
	if read.GetAdvancedExtension().GetEnhancement() != nil {
		return nil, substraitUnsupported("ReadRel advanced extension enhancements")
	}

	namedTable := read.GetNamedTable()
	if namedTable == nil {
		return nil, substraitUnsupported("ReadRel read type %T", read.GetReadType())
	}
	if namedTable.GetAdvancedExtension().GetEnhancement() != nil {
		return nil, substraitUnsupported("NamedTable advanced extension enhancements")
	}
	if err := checkNamedTable(namedTable.GetNames()); err != nil {
		return nil, err
	}

	columns, err := baseSchemaColumns(read.GetBaseSchema())
	if err != nil {
		return nil, err
	}
	q := &selectQuery{table: namedTable.GetNames(), columns: columns}
	return q, applyEmit(q, read.GetCommon())
}

// checkNamedTable validates a NamedTable name. MySQL databases are exposed as
// ADBC catalogs, so a name is either [table] or [database, table].
func checkNamedTable(names []string) error {
	switch len(names) {
	case 0:
		return substraitInvalid("NamedTable has no name")
	case 1, 2:
	default:
		return substraitUnsupported("NamedTable with %d name parts (expected [table] or [database, table])", len(names))
	}
	if slices.Contains(names, "") {
		return substraitInvalid("NamedTable has an empty name part")
	}
	return nil
}

// baseSchemaColumns returns the top-level column names of a ReadRel's base
// schema. Nested columns are not supported, so names and types must line up
// one-to-one: names include the fields of structs nested anywhere, including
// inside lists and maps.
func baseSchemaColumns(schema *substraitpb.NamedStruct) ([]string, error) {
	if schema == nil {
		return nil, substraitInvalid("ReadRel has no base_schema")
	}
	names := schema.GetNames()
	types := schema.GetStruct().GetTypes()
	for _, t := range types {
		switch t.GetKind().(type) {
		case *substraitpb.Type_Struct_, *substraitpb.Type_List_, *substraitpb.Type_Map_:
			return nil, substraitUnsupported("nested column types in base_schema")
		case *substraitpb.Type_Alias:
			// An alias may stand for a nested type
			return nil, substraitUnsupported("type alias columns in base_schema")
		case *substraitpb.Type_Func_:
			return nil, substraitUnsupported("func columns in base_schema")
		case nil:
			// Possibly a type added in a newer Substrait version
			return nil, substraitUnsupported("base_schema column without a known type")
		}
	}
	if len(names) != len(types) {
		return nil, substraitInvalid("base_schema has %d names but %d types", len(names), len(types))
	}
	if len(names) == 0 {
		return nil, substraitInvalid("base_schema has no columns")
	}
	if slices.Contains(names, "") {
		return nil, substraitInvalid("base_schema has an empty column name")
	}
	return names, nil
}

// applyEmit narrows q's output columns according to RelCommon's emit
// mapping, if any.
func applyEmit(q *selectQuery, common *substraitpb.RelCommon) error {
	if common.GetAdvancedExtension().GetEnhancement() != nil {
		return substraitUnsupported("RelCommon advanced extension enhancements")
	}
	emit := common.GetEmit()
	if emit == nil {
		return nil
	}
	mapping := emit.GetOutputMapping()
	if len(mapping) == 0 {
		return substraitInvalid("emit has an empty output_mapping")
	}
	columns := make([]string, len(mapping))
	for i, idx := range mapping {
		if idx < 0 || int(idx) >= len(q.columns) {
			return substraitInvalid("emit output_mapping index %d out of range [0, %d)", idx, len(q.columns))
		}
		columns[i] = q.columns[idx]
	}
	q.columns = columns
	return nil
}
