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
	"math"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"

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

// sqlExpr is a translated expression along with its Substrait type.
type sqlExpr struct {
	sql string
	typ *substraitpb.Type
	// notNull is set for expressions that are never NULL. Currently only
	// non-null literals set it, since base_schema nullability isn't checked
	// against the table.
	notNull bool
}

// selectQuery is the SQL form of a relation: a single-table SELECT whose
// output columns and filter predicates are expressions over the table's
// columns. The table name is stored unquoted; toSQL quotes it.
type selectQuery struct {
	table   []string
	columns []sqlExpr
	where   []string
}

// toSQL renders q, aliasing each output column to the corresponding name.
func (q *selectQuery) toSQL(names []string) string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	for i, col := range q.columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col.sql)
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
	if len(q.where) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(q.where, " AND "))
	}
	return sb.String()
}

// substraitTranslator holds plan-level state needed while translating
// relations, such as the declared extension functions.
type substraitTranslator struct {
	functions map[uint32]extensionFunction
}

type extensionFunction struct {
	urn  string
	name string
}

// planToSQL translates a Substrait plan into a MySQL query. Currently only
// FilterRels over a ReadRel over a NamedTable are supported.
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

	t, err := newSubstraitTranslator(plan)
	if err != nil {
		return "", err
	}
	q, err := t.relToQuery(root.GetInput())
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

func newSubstraitTranslator(plan *substraitpb.Plan) (*substraitTranslator, error) {
	urns := make(map[uint32]string, len(plan.GetExtensionUrns()))
	for _, urn := range plan.GetExtensionUrns() {
		if _, ok := urns[urn.GetExtensionUrnAnchor()]; ok {
			return nil, substraitInvalid("duplicate extension URN anchor %d", urn.GetExtensionUrnAnchor())
		}
		urns[urn.GetExtensionUrnAnchor()] = urn.GetUrn()
	}

	t := &substraitTranslator{functions: map[uint32]extensionFunction{}}
	for _, decl := range plan.GetExtensions() {
		fn := decl.GetExtensionFunction()
		if fn == nil {
			// Extension types and type variations only matter where they are
			// used, and are rejected there
			continue
		}
		urn, ok := urns[fn.GetExtensionUrnReference()]
		if !ok {
			return nil, substraitInvalid("function %q references undeclared extension URN anchor %d", fn.GetName(), fn.GetExtensionUrnReference())
		}
		if _, ok := t.functions[fn.GetFunctionAnchor()]; ok {
			return nil, substraitInvalid("duplicate function anchor %d", fn.GetFunctionAnchor())
		}
		// Names may be compound (e.g. "equal:any_any"). Argument types are
		// checked against the arguments themselves, so the signature isn't
		// needed.
		name, _, _ := strings.Cut(fn.GetName(), ":")
		t.functions[fn.GetFunctionAnchor()] = extensionFunction{urn: urn, name: name}
	}
	return t, nil
}

func (t *substraitTranslator) relToQuery(rel *substraitpb.Rel) (*selectQuery, error) {
	if rel == nil {
		return nil, substraitInvalid("missing relation")
	}
	switch r := rel.GetRelType().(type) {
	case *substraitpb.Rel_Read:
		return t.readRelToQuery(r.Read)
	case *substraitpb.Rel_Filter:
		return t.filterRelToQuery(r.Filter)
	default:
		return nil, substraitUnsupported("relation type %T", rel.GetRelType())
	}
}

func (t *substraitTranslator) readRelToQuery(read *substraitpb.ReadRel) (*selectQuery, error) {
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

	// Both filters reference the base schema, so they are added before emit
	// is applied
	if filter := read.GetFilter(); filter != nil {
		if err := t.addFilter(q, filter); err != nil {
			return nil, err
		}
	}
	// A best-effort filter may be applied in full or not at all, since the
	// producer filters the results again. It is only applied if it can be
	// translated exactly, and malformed ones are still rejected.
	if filter := read.GetBestEffortFilter(); filter != nil {
		if err := t.addFilter(q, filter); err != nil && !errors.Is(err, errSubstraitUnsupported) {
			return nil, err
		}
	}
	return q, applyEmit(q, read.GetCommon())
}

func (t *substraitTranslator) filterRelToQuery(filter *substraitpb.FilterRel) (*selectQuery, error) {
	if filter.GetAdvancedExtension().GetEnhancement() != nil {
		return nil, substraitUnsupported("FilterRel advanced extension enhancements")
	}
	if filter.GetCondition() == nil {
		return nil, substraitInvalid("FilterRel has no condition")
	}
	// A filter only drops rows, so it can be merged into its input's WHERE
	// clause as long as the input is a plain SELECT, which is all relToQuery
	// produces for now
	q, err := t.relToQuery(filter.GetInput())
	if err != nil {
		return nil, err
	}
	if err := t.addFilter(q, filter.GetCondition()); err != nil {
		return nil, err
	}
	return q, applyEmit(q, filter.GetCommon())
}

// addFilter adds a predicate over q's current output columns to q's WHERE
// clause.
func (t *substraitTranslator) addFilter(q *selectQuery, condition *substraitpb.Expression) error {
	cond, err := t.exprToSQL(condition, q.columns)
	if err != nil {
		return err
	}
	if cond.typ.GetBool() == nil {
		return substraitInvalid("filter condition must be boolean")
	}
	q.where = append(q.where, cond.sql)
	return nil
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

// baseSchemaColumns returns the top-level columns of a ReadRel's base
// schema. Nested columns are not supported, so names and types must line up
// one-to-one: names include the fields of structs nested anywhere, including
// inside lists and maps.
func baseSchemaColumns(schema *substraitpb.NamedStruct) ([]sqlExpr, error) {
	if schema == nil {
		return nil, substraitInvalid("ReadRel has no base_schema")
	}
	names := schema.GetNames()
	types := schema.GetStruct().GetTypes()
	// Checked before the names, which also include the fields of nested
	// types
	for _, t := range types {
		if err := checkSupportedType(t); err != nil {
			return nil, err
		}
	}
	if len(names) != len(types) {
		return nil, substraitInvalid("base_schema has %d names but %d types", len(names), len(types))
	}
	if len(names) == 0 {
		return nil, substraitInvalid("base_schema has no columns")
	}
	columns := make([]sqlExpr, len(names))
	for i, name := range names {
		if name == "" {
			return nil, substraitInvalid("base_schema has an empty column name")
		}
		columns[i] = sqlExpr{sql: quoteIdentifier(name), typ: types[i]}
	}
	return columns, nil
}

// maxTimePrecision is the highest fractional seconds precision MySQL
// supports (microseconds).
const maxTimePrecision = 6

// MySQL's maximum DECIMAL scale
const maxDecimalScale = 30

// checkSupportedType checks that t is one of the supported_types in
// substrait_dialect.yaml.
func checkSupportedType(t *substraitpb.Type) error {
	var variation interface{ GetTypeVariationReference() uint32 }
	precision := int32(-1)
	switch k := t.GetKind().(type) {
	case *substraitpb.Type_Bool:
		variation = k.Bool
	case *substraitpb.Type_I8_:
		variation = k.I8
	case *substraitpb.Type_I16_:
		variation = k.I16
	case *substraitpb.Type_I32_:
		variation = k.I32
	case *substraitpb.Type_I64_:
		variation = k.I64
	case *substraitpb.Type_Fp32:
		variation = k.Fp32
	case *substraitpb.Type_Fp64:
		variation = k.Fp64
	case *substraitpb.Type_Binary_:
		variation = k.Binary
	case *substraitpb.Type_FixedBinary_:
		variation = k.FixedBinary
	case *substraitpb.Type_String_:
		variation = k.String_
	case *substraitpb.Type_Varchar:
		variation = k.Varchar
	case *substraitpb.Type_FixedChar_:
		variation = k.FixedChar
	case *substraitpb.Type_Decimal_:
		variation = k.Decimal
		if k.Decimal.GetScale() > maxDecimalScale {
			return substraitUnsupported("decimals with scale %d (MySQL supports up to %d)", k.Decimal.GetScale(), maxDecimalScale)
		}
	case *substraitpb.Type_Date_:
		variation = k.Date
	case *substraitpb.Type_PrecisionTime_:
		variation, precision = k.PrecisionTime, k.PrecisionTime.GetPrecision()
	case *substraitpb.Type_PrecisionTimestamp_:
		variation, precision = k.PrecisionTimestamp, k.PrecisionTimestamp.GetPrecision()
	case *substraitpb.Type_PrecisionTimestampTz:
		variation, precision = k.PrecisionTimestampTz, k.PrecisionTimestampTz.GetPrecision()
	case nil:
		// Possibly a type added in a newer Substrait version
		return substraitUnsupported("type without a known kind")
	default:
		// Includes nested types, the deprecated timestamp and time types,
		// intervals, UUIDs and user-defined types
		return substraitUnsupported("type %T", k)
	}
	if variation.GetTypeVariationReference() != 0 {
		return substraitUnsupported("type variations")
	}
	if precision > maxTimePrecision {
		return substraitUnsupported("%T with precision %d (MySQL supports up to %d)", t.GetKind(), precision, maxTimePrecision)
	}
	return nil
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
	columns := make([]sqlExpr, len(mapping))
	for i, idx := range mapping {
		if idx < 0 || int(idx) >= len(q.columns) {
			return substraitInvalid("emit output_mapping index %d out of range [0, %d)", idx, len(q.columns))
		}
		columns[i] = q.columns[idx]
	}
	q.columns = columns
	return nil
}

const (
	urnFunctionsComparison = "extension:io.substrait:functions_comparison"
	urnFunctionsBoolean    = "extension:io.substrait:functions_boolean"
)

// Types of translated literals and functions. Nullability doesn't matter for
// translation.
var (
	boolType = &substraitpb.Type{Kind: &substraitpb.Type_Bool{Bool: &substraitpb.Type_Boolean{}}}
	i8Type   = &substraitpb.Type{Kind: &substraitpb.Type_I8_{I8: &substraitpb.Type_I8{}}}
	i16Type  = &substraitpb.Type{Kind: &substraitpb.Type_I16_{I16: &substraitpb.Type_I16{}}}
	i32Type  = &substraitpb.Type{Kind: &substraitpb.Type_I32_{I32: &substraitpb.Type_I32{}}}
	i64Type  = &substraitpb.Type{Kind: &substraitpb.Type_I64_{I64: &substraitpb.Type_I64{}}}
	fp32Type = &substraitpb.Type{Kind: &substraitpb.Type_Fp32{Fp32: &substraitpb.Type_FP32{}}}
	fp64Type = &substraitpb.Type{Kind: &substraitpb.Type_Fp64{Fp64: &substraitpb.Type_FP64{}}}
	dateType = &substraitpb.Type{Kind: &substraitpb.Type_Date_{Date: &substraitpb.Type_Date{}}}
)

// exprToSQL translates an expression whose field references point into
// input. The generated SQL is either atomic or parenthesized, so it can be
// embedded in larger expressions as-is.
func (t *substraitTranslator) exprToSQL(e *substraitpb.Expression, input []sqlExpr) (sqlExpr, error) {
	switch e := e.GetRexType().(type) {
	case *substraitpb.Expression_Selection:
		return fieldReferenceToSQL(e.Selection, input)
	case *substraitpb.Expression_Literal_:
		return literalToSQL(e.Literal)
	case *substraitpb.Expression_ScalarFunction_:
		return t.scalarFunctionToSQL(e.ScalarFunction, input)
	case nil:
		// Also covers expression types added in newer Substrait versions
		return sqlExpr{}, substraitUnsupported("expression without a known type")
	default:
		return sqlExpr{}, substraitUnsupported("expression type %T", e)
	}
}

func fieldReferenceToSQL(ref *substraitpb.Expression_FieldReference, input []sqlExpr) (sqlExpr, error) {
	if ref.GetRootReference() == nil {
		return sqlExpr{}, substraitUnsupported("field references other than root references")
	}
	field := ref.GetDirectReference().GetStructField()
	if field == nil {
		return sqlExpr{}, substraitUnsupported("field references other than direct struct field references")
	}
	if field.GetChild() != nil {
		return sqlExpr{}, substraitUnsupported("nested field references")
	}
	idx := field.GetField()
	if idx < 0 || int(idx) >= len(input) {
		return sqlExpr{}, substraitInvalid("field reference %d out of range [0, %d)", idx, len(input))
	}
	return input[idx], nil
}

// MySQL's supported DATE range, as days since the Unix epoch
var (
	minDateLiteral = int32(time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400)
	maxDateLiteral = int32(time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC).Unix() / 86400)
)

func literalToSQL(lit *substraitpb.Expression_Literal) (sqlExpr, error) {
	if lit.GetTypeVariationReference() != 0 {
		return sqlExpr{}, substraitUnsupported("literals with type variations")
	}
	switch l := lit.GetLiteralType().(type) {
	case *substraitpb.Expression_Literal_Boolean:
		if l.Boolean {
			return sqlExpr{"TRUE", boolType, true}, nil
		}
		return sqlExpr{"FALSE", boolType, true}, nil
	case *substraitpb.Expression_Literal_I8:
		// I8 and I16 literals are stored as int32
		if l.I8 < math.MinInt8 || l.I8 > math.MaxInt8 {
			return sqlExpr{}, substraitInvalid("i8 literal %d out of range", l.I8)
		}
		return sqlExpr{intLiteralToSQL(int64(l.I8)), i8Type, true}, nil
	case *substraitpb.Expression_Literal_I16:
		if l.I16 < math.MinInt16 || l.I16 > math.MaxInt16 {
			return sqlExpr{}, substraitInvalid("i16 literal %d out of range", l.I16)
		}
		return sqlExpr{intLiteralToSQL(int64(l.I16)), i16Type, true}, nil
	case *substraitpb.Expression_Literal_I32:
		return sqlExpr{intLiteralToSQL(int64(l.I32)), i32Type, true}, nil
	case *substraitpb.Expression_Literal_I64:
		return sqlExpr{intLiteralToSQL(l.I64), i64Type, true}, nil
	case *substraitpb.Expression_Literal_Fp32:
		s, err := floatLiteralToSQL(float64(l.Fp32))
		return sqlExpr{s, fp32Type, true}, err
	case *substraitpb.Expression_Literal_Fp64:
		s, err := floatLiteralToSQL(l.Fp64)
		return sqlExpr{s, fp64Type, true}, err
	case *substraitpb.Expression_Literal_Decimal_:
		sql, err := decimalLiteralToSQL(l.Decimal)
		if err != nil {
			return sqlExpr{}, err
		}
		typ := &substraitpb.Type{Kind: &substraitpb.Type_Decimal_{Decimal: &substraitpb.Type_Decimal{
			Precision: l.Decimal.GetPrecision(),
			Scale:     l.Decimal.GetScale(),
		}}}
		return sqlExpr{sql, typ, true}, nil
	case *substraitpb.Expression_Literal_Date:
		if l.Date < minDateLiteral || l.Date > maxDateLiteral {
			return sqlExpr{}, substraitUnsupported("date literals outside MySQL's range 1000-01-01 to 9999-12-31")
		}
		date := time.Unix(int64(l.Date)*86400, 0).UTC().Format(time.DateOnly)
		return sqlExpr{"DATE '" + date + "'", dateType, true}, nil
	case *substraitpb.Expression_Literal_Null:
		if l.Null.GetKind() == nil {
			return sqlExpr{}, substraitInvalid("null literal has no type")
		}
		if err := checkSupportedType(l.Null); err != nil {
			return sqlExpr{}, err
		}
		return sqlExpr{sql: "NULL", typ: l.Null}, nil
	case nil:
		// Also covers literal types added in newer Substrait versions
		return sqlExpr{}, substraitUnsupported("literal without a known type")
	default:
		return sqlExpr{}, substraitUnsupported("literal type %T", l)
	}
}

// intLiteralToSQL parenthesizes negative values, so that a minus sign can't
// combine with a preceding operator.
func intLiteralToSQL(v int64) string {
	s := strconv.FormatInt(v, 10)
	if v < 0 {
		return "(" + s + ")"
	}
	return s
}

// floatLiteralToSQL renders a floating point literal in exponent notation,
// so MySQL treats it as a DOUBLE rather than an exact DECIMAL. FP32 values are
// widened first, so they compare equal to the same value in a FLOAT column.
func floatLiteralToSQL(v float64) (string, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return "", substraitUnsupported("non-finite floating point literals")
	}
	s := strconv.FormatFloat(v, 'e', -1, 64)
	if math.Signbit(v) {
		return "(" + s + ")", nil
	}
	return s, nil
}

// decimalLiteralToSQL renders a decimal literal as an exact value, which
// MySQL parses as a DECIMAL.
func decimalLiteralToSQL(lit *substraitpb.Expression_Literal_Decimal) (string, error) {
	value := lit.GetValue()
	if len(value) != 16 {
		return "", substraitInvalid("decimal literal must have 16 bytes, got %d", len(value))
	}
	precision, scale := lit.GetPrecision(), lit.GetScale()
	if precision < 1 || precision > 38 || scale < 0 || scale > precision {
		return "", substraitInvalid("invalid decimal literal precision %d and scale %d", precision, scale)
	}
	if scale > maxDecimalScale {
		return "", substraitUnsupported("decimal literals with scale over %d", maxDecimalScale)
	}

	// The value is a little-endian two's complement integer
	bigEndian := make([]byte, len(value))
	for i, b := range value {
		bigEndian[len(value)-1-i] = b
	}
	unscaled := new(big.Int).SetBytes(bigEndian)
	if bigEndian[0]&0x80 != 0 {
		unscaled.Sub(unscaled, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	digits := new(big.Int).Abs(unscaled).String()
	if len(digits) > int(precision) {
		return "", substraitInvalid("decimal literal does not fit precision %d", precision)
	}

	if scale > 0 {
		if len(digits) <= int(scale) {
			digits = strings.Repeat("0", int(scale)-len(digits)+1) + digits
		}
		digits = digits[:len(digits)-int(scale)] + "." + digits[len(digits)-int(scale):]
	}
	if unscaled.Sign() < 0 {
		return "(-" + digits + ")", nil
	}
	return digits, nil
}

// checkComparable checks that comparison operands have the same type, and
// that it is one MySQL compares the way Substrait does.
func checkComparable(name string, args []sqlExpr) error {
	// Mismatched types make the plan invalid, whether or not the types are
	// supported
	for _, arg := range args[1:] {
		if !sameType(arg.typ, args[0].typ) {
			return substraitInvalid("function %s has arguments of different types", name)
		}
	}
	switch args[0].typ.GetKind().(type) {
	case *substraitpb.Type_Bool,
		*substraitpb.Type_I8_, *substraitpb.Type_I16_, *substraitpb.Type_I32_, *substraitpb.Type_I64_,
		*substraitpb.Type_Fp32, *substraitpb.Type_Fp64,
		*substraitpb.Type_Decimal_, *substraitpb.Type_Date_,
		*substraitpb.Type_PrecisionTimestamp_, *substraitpb.Type_PrecisionTimestampTz:
		return nil
	default:
		return substraitUnsupported("comparisons of type %T (%s)", args[0].typ.GetKind(), name)
	}
}

// sameType reports whether a and b are the same type, ignoring nullability.
// Parameters matter: MariaDB rounds a DECIMAL constant to the column's scale
// when it uses an index, so 1.005 matches 1.01 in a DECIMAL(5,2) column.
func sameType(a, b *substraitpb.Type) bool {
	if fmt.Sprintf("%T", a.GetKind()) != fmt.Sprintf("%T", b.GetKind()) {
		return false
	}
	switch {
	case a.GetDecimal() != nil:
		return a.GetDecimal().GetPrecision() == b.GetDecimal().GetPrecision() &&
			a.GetDecimal().GetScale() == b.GetDecimal().GetScale()
	case a.GetPrecisionTimestamp() != nil:
		return a.GetPrecisionTimestamp().GetPrecision() == b.GetPrecisionTimestamp().GetPrecision()
	case a.GetPrecisionTimestampTz() != nil:
		return a.GetPrecisionTimestampTz().GetPrecision() == b.GetPrecisionTimestampTz().GetPrecision()
	}
	return true
}

func checkBoolean(name string, args []sqlExpr) error {
	for _, arg := range args {
		if arg.typ.GetBool() == nil {
			return substraitInvalid("function %s expects boolean arguments", name)
		}
	}
	return nil
}

var binaryComparisonOperators = map[string]string{
	"equal":                "=",
	"not_equal":            "<>",
	"lt":                   "<",
	"lte":                  "<=",
	"gt":                   ">",
	"gte":                  ">=",
	"is_not_distinct_from": "<=>",
}

var booleanTests = map[string]string{
	"is_true":      "IS TRUE",
	"is_not_true":  "IS NOT TRUE",
	"is_false":     "IS FALSE",
	"is_not_false": "IS NOT FALSE",
}

func (t *substraitTranslator) scalarFunctionToSQL(fn *substraitpb.Expression_ScalarFunction, input []sqlExpr) (sqlExpr, error) {
	decl, ok := t.functions[fn.GetFunctionReference()]
	if !ok {
		return sqlExpr{}, substraitInvalid("undeclared function anchor %d", fn.GetFunctionReference())
	}
	if len(fn.GetOptions()) > 0 {
		return sqlExpr{}, substraitUnsupported("options for function %s", decl.name)
	}
	if fn.GetOutputType().GetKind() == nil {
		return sqlExpr{}, substraitInvalid("function %s has no output type", decl.name)
	}
	if len(fn.GetArgs()) > 0 { //nolint:staticcheck // must not be ignored
		// Superseded by arguments, which would otherwise look empty
		return sqlExpr{}, substraitUnsupported("deprecated args field for function %s", decl.name)
	}

	args := make([]sqlExpr, len(fn.GetArguments()))
	for i, arg := range fn.GetArguments() {
		value := arg.GetValue()
		if value == nil {
			return sqlExpr{}, substraitUnsupported("non-value arguments for function %s", decl.name)
		}
		var err error
		if args[i], err = t.exprToSQL(value, input); err != nil {
			return sqlExpr{}, err
		}
	}

	sql, err := functionToSQL(decl, args)
	if err != nil {
		return sqlExpr{}, err
	}
	// Every supported function returns a boolean
	if fn.GetOutputType().GetBool() == nil {
		return sqlExpr{}, substraitInvalid("function %s must return a boolean", decl.name)
	}
	if err := checkSupportedType(fn.GetOutputType()); err != nil {
		return sqlExpr{}, err
	}
	return sqlExpr{sql: sql, typ: fn.GetOutputType()}, nil
}

// isNullToSQL tests whether expr is NULL. IS NULL isn't used, since it also
// matches zero dates in NOT NULL DATE and DATETIME columns, and, with
// sql_auto_is_null, the last inserted AUTO_INCREMENT row.
func isNullToSQL(expr string) string {
	return "(" + expr + " <=> NULL)"
}

func functionToSQL(decl extensionFunction, args []sqlExpr) (string, error) {
	arity := func(n int) error {
		if len(args) != n {
			return substraitInvalid("function %s expects %d arguments, got %d", decl.name, n, len(args))
		}
		return nil
	}

	switch decl.urn {
	case urnFunctionsComparison:
		if op, ok := binaryComparisonOperators[decl.name]; ok {
			if err := arity(2); err != nil {
				return "", err
			}
			if err := checkComparable(decl.name, args); err != nil {
				return "", err
			}
			return "(" + args[0].sql + " " + op + " " + args[1].sql + ")", nil
		}
		if test, ok := booleanTests[decl.name]; ok {
			if err := arity(1); err != nil {
				return "", err
			}
			if err := checkBoolean(decl.name, args); err != nil {
				return "", err
			}
			return "(" + args[0].sql + " " + test + ")", nil
		}
		switch decl.name {
		case "is_null":
			if err := arity(1); err != nil {
				return "", err
			}
			return isNullToSQL(args[0].sql), nil
		case "is_not_null":
			if err := arity(1); err != nil {
				return "", err
			}
			return "(NOT " + isNullToSQL(args[0].sql) + ")", nil
		case "is_distinct_from":
			if err := arity(2); err != nil {
				return "", err
			}
			if err := checkComparable(decl.name, args); err != nil {
				return "", err
			}
			return "(NOT (" + args[0].sql + " <=> " + args[1].sql + "))", nil
		case "between":
			if err := arity(3); err != nil {
				return "", err
			}
			if err := checkComparable(decl.name, args); err != nil {
				return "", err
			}
			sql := "(" + args[0].sql + " BETWEEN " + args[1].sql + " AND " + args[2].sql + ")"
			if args[1].notNull && args[2].notNull {
				return sql, nil
			}
			// Substrait's between is NULL if a bound is NULL, while MySQL's
			// BETWEEN is FALSE if the comparison with the other bound is,
			// e.g. 1 BETWEEN NULL AND 0. Plain BETWEEN is kept for non-null
			// bounds, since MySQL can use it for index range scans.
			return "(CASE WHEN " + isNullToSQL(args[1].sql) + " OR " + isNullToSQL(args[2].sql) + " THEN NULL ELSE " + sql + " END)", nil
		}
	case urnFunctionsBoolean:
		switch decl.name {
		case "and", "or":
			if err := checkBoolean(decl.name, args); err != nil {
				return "", err
			}
			if len(args) == 0 {
				// The identity of each operation
				if decl.name == "and" {
					return "TRUE", nil
				}
				return "FALSE", nil
			}
			parts := make([]string, len(args))
			for i, arg := range args {
				parts[i] = arg.sql
			}
			return "(" + strings.Join(parts, " "+strings.ToUpper(decl.name)+" ") + ")", nil
		case "and_not", "xor":
			if err := arity(2); err != nil {
				return "", err
			}
			if err := checkBoolean(decl.name, args); err != nil {
				return "", err
			}
			if decl.name == "and_not" {
				return "(" + args[0].sql + " AND (NOT " + args[1].sql + "))", nil
			}
			return "(" + args[0].sql + " XOR " + args[1].sql + ")", nil
		case "not":
			if err := arity(1); err != nil {
				return "", err
			}
			if err := checkBoolean(decl.name, args); err != nil {
				return "", err
			}
			return "(NOT " + args[0].sql + ")", nil
		}
	}
	return "", substraitUnsupported("function %s from %s", decl.name, decl.urn)
}
