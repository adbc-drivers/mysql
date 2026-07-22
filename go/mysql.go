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
	"errors"
	"fmt"
	"strings"
	"time"

	// register the "mysql" driver with database/sql
	_ "github.com/go-sql-driver/mysql"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/go-ext/variant"
)

const (
	OptionKeyZeroDatetimeBehavior = "mysql.query.zero_datetime_behavior"

	OptionValueZeroDatetimeBehaviorError         = "error"
	OptionValueZeroDatetimeBehaviorConvertToNull = "convert_to_null"
)

type zeroDatetimeBehavior int

const (
	zeroDatetimeBehaviorError zeroDatetimeBehavior = iota
	zeroDatetimeBehaviorConvertToNull
)

func (b zeroDatetimeBehavior) String() string {
	switch b {
	case zeroDatetimeBehaviorError:
		return OptionValueZeroDatetimeBehaviorError
	case zeroDatetimeBehaviorConvertToNull:
		return OptionValueZeroDatetimeBehaviorConvertToNull
	default:
		return OptionValueZeroDatetimeBehaviorError
	}
}

func parseZeroDatetimeBehavior(value string, errorHelper *driverbase.ErrorHelper) (zeroDatetimeBehavior, error) {
	switch value {
	case OptionValueZeroDatetimeBehaviorError:
		return zeroDatetimeBehaviorError, nil
	case OptionValueZeroDatetimeBehaviorConvertToNull:
		return zeroDatetimeBehaviorConvertToNull, nil
	default:
		if errorHelper == nil {
			return zeroDatetimeBehaviorError, fmt.Errorf(
				"invalid %s value %q, expected %q or %q",
				OptionKeyZeroDatetimeBehavior,
				value,
				OptionValueZeroDatetimeBehaviorError,
				OptionValueZeroDatetimeBehaviorConvertToNull)
		}
		return zeroDatetimeBehaviorError, errorHelper.InvalidArgument(
			"invalid %s value %q, expected %q or %q",
			OptionKeyZeroDatetimeBehavior,
			value,
			OptionValueZeroDatetimeBehaviorError,
			OptionValueZeroDatetimeBehaviorConvertToNull)
	}
}

// MySQLTypeConverter provides MySQL-specific type conversion enhancements
type mySQLTypeConverter struct {
	sqlwrapper.DefaultTypeConverter
	zeroDatetimeBehavior zeroDatetimeBehavior
}

func makeTypeConverter(zeroDatetimeBehavior zeroDatetimeBehavior) sqlwrapper.TypeConverter {
	return &mySQLTypeConverter{
		DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{VendorName: "MySQL"},
		zeroDatetimeBehavior: zeroDatetimeBehavior,
	}
}

// normalizeUnsignedTypeName converts "UNSIGNED INT" -> "INT UNSIGNED" format
// The go-sql-driver/mysql returns "UNSIGNED X" but the default type converter expects "X UNSIGNED"
func normalizeUnsignedTypeName(typeName string) string {
	if after, ok := strings.CutPrefix(typeName, "UNSIGNED "); ok {
		return after + " UNSIGNED"
	}
	return typeName
}

// ConvertRawColumnType implements TypeConverter with MySQL-specific enhancements
func (m *mySQLTypeConverter) ConvertRawColumnType(colType sqlwrapper.ColumnType) (arrow.DataType, bool, arrow.Metadata, error) {
	typeName := strings.ToUpper(colType.DatabaseTypeName)
	nullable := colType.Nullable

	// Normalize "UNSIGNED X" to "X UNSIGNED" for the default type converter
	// Only update DatabaseTypeName when reordering is needed, to preserve original casing in metadata
	typeName = normalizeUnsignedTypeName(typeName)
	if typeName != strings.ToUpper(colType.DatabaseTypeName) {
		colType.DatabaseTypeName = typeName
	}

	switch typeName {
	case "BIT":
		// Handle BIT type as binary data
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName,
			"sql.column_name":        colType.Name,
		}

		if colType.Length != nil {
			metadataMap["sql.length"] = fmt.Sprintf("%d", *colType.Length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.Binary, nullable, metadata, nil

	case "GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON":
		// Convert MySQL spatial types to binary with spatial metadata
		// TODO: we should use geoarrow extension types if applicable
		metadata := arrow.MetadataFrom(map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName,
			"sql.column_name":        colType.Name,
			"mysql.is_spatial":       "true",
		})
		return arrow.BinaryTypes.Binary, nullable, metadata, nil

	case "ENUM", "SET":
		// Handle ENUM/SET as string with special metadata
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName,
			"sql.column_name":        colType.Name,
			"mysql.is_enum_set":      "true",
		}

		if colType.Length != nil {
			metadataMap["sql.length"] = fmt.Sprintf("%d", *colType.Length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, nullable, metadata, nil

	case "TIMESTAMP":
		var timestampType arrow.DataType
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		if colType.Precision != nil {
			precision := *colType.Precision
			metadataMap[sqlwrapper.MetaKeyFractionalSecondsPrecision] = fmt.Sprintf("%d", precision)
			if precision > 6 {
				precision = 6
			}
			timeUnit := arrow.TimeUnit(precision / 3)
			timestampType = &arrow.TimestampType{Unit: timeUnit, TimeZone: "UTC"}
		} else {
			// No precision info available, default to microseconds (most common)
			timestampType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return timestampType, colType.Nullable, metadata, nil

	default:
		// Fall back to default conversion for standard types
		return m.DefaultTypeConverter.ConvertRawColumnType(colType)
	}
}

// CreateInserter creates MySQL-specific inserters bound to builders for enhanced performance
func (m *mySQLTypeConverter) CreateInserter(field *arrow.Field, builder array.Builder) (sqlwrapper.Inserter, error) {
	// Check for MySQL-specific types first
	switch field.Type.(type) {
	case *extensions.JSONType:
		return &mysqlJSONInserter{builder: builder}, nil
	case *arrow.BinaryType:
		if dbTypeName, ok := field.Metadata.GetValue("sql.database_type_name"); ok && dbTypeName == "BIT" {
			return &mysqlBitInserter{builder: builder.(array.BinaryLikeBuilder)}, nil
		}
		// Handle MySQL spatial types
		if isSpatial, ok := field.Metadata.GetValue("mysql.is_spatial"); ok && isSpatial == "true" {
			return &mysqlSpatialInserter{builder: builder.(array.BinaryLikeBuilder)}, nil
		}
		// Fall through to default for non-spatial binary
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	case *arrow.Date32Type:
		defaultInserter, err := m.DefaultTypeConverter.CreateInserter(field, builder)
		if err != nil {
			return nil, err
		}
		return &mysqlZeroDatetimeInserter{
			builder:              builder,
			defaultInserter:      defaultInserter,
			zeroDatetimeBehavior: m.zeroDatetimeBehavior,
		}, nil
	case *arrow.Date64Type:
		defaultInserter, err := m.DefaultTypeConverter.CreateInserter(field, builder)
		if err != nil {
			return nil, err
		}
		return &mysqlZeroDatetimeInserter{
			builder:              builder,
			defaultInserter:      defaultInserter,
			zeroDatetimeBehavior: m.zeroDatetimeBehavior,
		}, nil
	case *arrow.TimestampType:
		defaultInserter, err := m.DefaultTypeConverter.CreateInserter(field, builder)
		if err != nil {
			return nil, err
		}
		return &mysqlZeroDatetimeInserter{
			builder:              builder,
			defaultInserter:      defaultInserter,
			zeroDatetimeBehavior: m.zeroDatetimeBehavior,
		}, nil
	default:
		// For all other types, use default inserter
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	}
}

// MySQL-specific inserters
type mysqlJSONInserter struct {
	builder array.Builder
}

func (ins *mysqlJSONInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := sqlValue.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte for mysql json inserter, got %T", sqlValue)
	}

	// For extension types, we need to use AppendValueFromString
	// since the ExtensionBuilder doesn't implement StringLikeBuilder.Append
	return ins.builder.AppendValueFromString(string(t))
}

type mysqlBitInserter struct {
	builder array.BinaryLikeBuilder
}

func (ins *mysqlBitInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := sqlValue.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte for mysql bit inserter, got %T", sqlValue)
	}

	ins.builder.Append(t)
	return nil
}

type mysqlSpatialInserter struct {
	builder array.BinaryLikeBuilder
}

func (ins *mysqlSpatialInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := sqlValue.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte for mysql spatial inserter, got %T", sqlValue)
	}

	ins.builder.Append(t)
	return nil
}

type mysqlZeroDatetimeInserter struct {
	builder              array.Builder
	defaultInserter      sqlwrapper.Inserter
	zeroDatetimeBehavior zeroDatetimeBehavior
}

func (ins *mysqlZeroDatetimeInserter) AppendValue(sqlValue any) error {
	isZeroDatetime, err := isZeroDatetimeValue(sqlValue)
	if err != nil {
		return err
	}
	if !isZeroDatetime {
		return ins.defaultInserter.AppendValue(sqlValue)
	}

	switch ins.zeroDatetimeBehavior {
	case zeroDatetimeBehaviorError:
		return adbc.Error{
			Code: adbc.StatusInvalidData,
			Msg:  "zero datetime value cannot be converted to Arrow date or timestamp",
		}
	case zeroDatetimeBehaviorConvertToNull:
		ins.builder.AppendNull()
		return nil
	default:
		return adbc.Error{
			Code: adbc.StatusInvalidData,
			Msg:  "zero datetime value cannot be converted to Arrow date or timestamp",
		}
	}
}

func isZeroDatetimeValue(sqlValue any) (bool, error) {
	switch v := sqlValue.(type) {
	case nil:
		return false, nil
	case []byte:
		return hasZeroDatePrefix(string(v)), nil
	case string:
		return hasZeroDatePrefix(v), nil
	default:
		return false, nil
	}
}

func hasZeroDatePrefix(value string) bool {
	if len(value) < len("0000-00-00") {
		return false
	}
	if value[4] != '-' || value[7] != '-' {
		return false
	}

	year := value[:4]
	month := value[5:7]
	day := value[8:10]
	return year == "0000" || month == "00" || day == "00"
}

// ConvertArrowToGo implements MySQL-specific Arrow value to Go value conversion
func (m *mySQLTypeConverter) ConvertArrowToGo(arrowArray arrow.Array, index int, field *arrow.Field) (any, error) {
	if arrowArray.IsNull(index) {
		return nil, nil
	}

	// Handle MySQL-specific Arrow to Go conversions
	switch a := arrowArray.(type) {
	case *extensions.JSONArray:
		// Handle JSON extension type arrays
		jsonStr := a.ValueStr(index)
		v := variant.New(jsonStr)
		return v, nil

	case *array.Time32:
		// For MySQL driver, always convert Time32 arrays to time-only format strings
		// This handles both explicit TIME column metadata and parameter binding scenarios
		timeType := a.DataType().(*arrow.Time32Type)
		t := a.Value(index).ToTime(timeType.Unit)
		return t.Format("15:04:05.000000"), nil

	case *array.Time64:
		// For MySQL driver, always convert Time64 arrays to time-only format strings
		// This handles both explicit TIME column metadata and parameter binding scenarios
		timeType := a.DataType().(*arrow.Time64Type)
		t := a.Value(index).ToTime(timeType.Unit)
		return t.Format("15:04:05.000000"), nil

	case *array.Timestamp:
		timestampType := a.DataType().(*arrow.TimestampType)
		rawValue := a.Value(index)
		t := rawValue.ToTime(timestampType.Unit)

		// For nanosecond precision, truncate to microseconds
		if timestampType.Unit == arrow.Nanosecond {
			microseconds := t.UnixMicro()
			converted := time.UnixMicro(microseconds).UTC()
			return converted, nil
		}

		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)

	case *array.Float16:
		return a.Value(index).Float32(), nil

	default:
		// For all other types, use default conversion
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)
	}
}

// mysqlConnectionImpl extends sqlwrapper connection with MySQL-specific functionality
type mysqlConnectionImpl struct {
	*sqlwrapper.ConnectionImplBase // Embed sqlwrapper connection for all standard functionality

	version              string
	zeroDatetimeBehavior zeroDatetimeBehavior
	// activeTransaction tracks whether a BEGIN has been issued on this session
	// (i.e. autocommit is disabled and a transaction is currently open).
	activeTransaction bool
	// currentIsolationLevel caches the last isolation level applied via
	// adbc.OptionKeyIsolationLevel so GetOption can return it. Empty string
	// means no level has been set (the session default is in effect).
	currentIsolationLevel adbc.OptionIsolationLevel
}

// implements BulkIngester interface
var _ sqlwrapper.BulkIngester = (*mysqlConnectionImpl)(nil)

// implements CurrentNameSpacer interface
var _ driverbase.CurrentNamespacer = (*mysqlConnectionImpl)(nil)

// implements TableTypeLister interface
var _ driverbase.TableTypeLister = (*mysqlConnectionImpl)(nil)

// implements AutocommitSetter interface (auto-registered by sqlwrapper)
var _ driverbase.AutocommitSetter = (*mysqlConnectionImpl)(nil)

// mysqlConnectionFactory creates MySQL connections
type mysqlConnectionFactory struct {
}

func (f *mysqlConnectionFactory) CreateDatabase(database *sqlwrapper.DatabaseImplBase) (sqlwrapper.DatabaseImpl, error) {
	return &mysqlDatabase{
		DatabaseImplBase:     database,
		zeroDatetimeBehavior: zeroDatetimeBehaviorError,
	}, nil
}

func (f *mysqlConnectionFactory) CreateConnection(
	ctx context.Context,
	conn *sqlwrapper.ConnectionImplBase,
) (sqlwrapper.ConnectionImpl, error) {
	// Wrap the pre-built sqlwrapper connection with MySQL-specific functionality
	return &mysqlConnectionImpl{
		ConnectionImplBase:   conn,
		zeroDatetimeBehavior: conn.Database.Derived.(*mysqlDatabase).zeroDatetimeBehavior,
	}, nil
}

func (f *mysqlConnectionFactory) CreateStatement(stmt *sqlwrapper.StatementImplBase) (sqlwrapper.StatementImpl, error) {
	return &mysqlStatement{
		StatementImplBase:    stmt,
		zeroDatetimeBehavior: stmt.Conn.Derived.(*mysqlConnectionImpl).zeroDatetimeBehavior,
	}, nil
}

// mysqlIsolationLevelStatement maps an ADBC OptionIsolationLevel to a MySQL
// `SET SESSION TRANSACTION ISOLATION LEVEL ...` statement. Unsupported levels
// (SNAPSHOT, LINEARIZABLE) return StatusNotImplemented per the ADBC spec.
func mysqlIsolationLevelStatement(l adbc.OptionIsolationLevel) (string, error) {
	switch l {
	case adbc.LevelDefault:
		// MySQL's documented default isolation level is REPEATABLE READ.
		// This mirrors the C++ PostgreSQL driver's handling of LevelDefault
		// (which sends the backend's documented default, READ COMMITTED).
		return "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ", nil
	case adbc.LevelReadUncommitted:
		return "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", nil
	case adbc.LevelReadCommitted:
		return "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED", nil
	case adbc.LevelRepeatableRead:
		return "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ", nil
	case adbc.LevelSerializable:
		return "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE", nil
	case adbc.LevelSnapshot, adbc.LevelLinearizable:
		return "", adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("[MySQL] isolation level %q not supported", string(l)),
		}
	default:
		return "", adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[MySQL] unknown isolation level %q", string(l)),
		}
	}
}

// SetAutocommit implements driverbase.AutocommitSetter. Disabling autocommit
// begins a transaction on the dedicated session connection; enabling it
// commits any in-flight transaction and lets subsequent statements
// auto-commit (the MySQL default). State is mirrored by the driverbase
// wrapper into Base().Autocommit after a successful return.
//
// The previously-set isolation level, if any, is already persisted at the
// SESSION scope via `SET SESSION TRANSACTION ISOLATION LEVEL ...` in
// SetOption, so it automatically applies to subsequent transactions without
// any re-application here.
func (c *mysqlConnectionImpl) SetAutocommit(ctx context.Context, enabled bool) error {
	if enabled {
		if c.activeTransaction {
			if _, err := c.Conn.ExecContext(ctx, "COMMIT"); err != nil {
				return c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "COMMIT failed: %v", err)
			}
			c.activeTransaction = false
		}
		// MySQL connections auto-commit by default; nothing else to do.
		return nil
	}
	// Disabling autocommit: open a transaction if one isn't already open.
	if !c.activeTransaction {
		if _, err := c.Conn.ExecContext(ctx, "BEGIN"); err != nil {
			return c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "BEGIN failed: %v", err)
		}
		c.activeTransaction = true
	}
	return nil
}

// Commit commits the current transaction. It is only invoked when autocommit
// is disabled (the driverbase wrapper short-circuits to StatusInvalidState
// otherwise). After COMMIT a new transaction is immediately begun so the
// next statement continues to run inside a transaction until the caller
// re-enables autocommit (chained-transaction semantics, matching the
// Snowflake driver's pattern).
func (c *mysqlConnectionImpl) Commit(ctx context.Context) error {
	if _, err := c.Conn.ExecContext(ctx, "COMMIT"); err != nil {
		return c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "COMMIT failed: %v", err)
	}
	if _, err := c.Conn.ExecContext(ctx, "BEGIN"); err != nil {
		return c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "BEGIN failed: %v", err)
	}
	return nil
}

// Rollback rolls back the current transaction. Like Commit, it begins a new
// transaction immediately afterwards (chained-transaction semantics).
func (c *mysqlConnectionImpl) Rollback(ctx context.Context) error {
	if _, err := c.Conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		return c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "ROLLBACK failed: %v", err)
	}
	if _, err := c.Conn.ExecContext(ctx, "BEGIN"); err != nil {
		return c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "BEGIN failed: %v", err)
	}
	return nil
}

type mysqlDatabase struct {
	*sqlwrapper.DatabaseImplBase
	zeroDatetimeBehavior zeroDatetimeBehavior
}

func (db *mysqlDatabase) SetOptions(ctx context.Context, opts map[string]string) error {
	for key, value := range opts {
		if err := db.SetOption(ctx, key, value); err != nil {
			return err
		}
	}
	return nil
}

func (db *mysqlDatabase) GetOption(ctx context.Context, key string) (string, error) {
	switch key {
	case OptionKeyZeroDatetimeBehavior:
		return db.zeroDatetimeBehavior.String(), nil
	default:
		return db.DatabaseImplBase.GetOption(ctx, key)
	}
}

func (db *mysqlDatabase) SetOption(ctx context.Context, key, value string) error {
	switch key {
	case OptionKeyZeroDatetimeBehavior:
		behavior, err := parseZeroDatetimeBehavior(value, &db.ErrorHelper)
		if err != nil {
			return err
		}
		db.zeroDatetimeBehavior = behavior
		return nil
	default:
		return db.DatabaseImplBase.SetOption(ctx, key, value)
	}
}

type mysqlStatement struct {
	*sqlwrapper.StatementImplBase
	zeroDatetimeBehavior zeroDatetimeBehavior
}

func (s *mysqlStatement) MakeTypeConverter(vendorName string) sqlwrapper.TypeConverter {
	return makeTypeConverter(s.zeroDatetimeBehavior)
}

func (c *mysqlConnectionImpl) NewStatement(ctx context.Context) (adbc.StatementWithContext, error) {
	stmt, err := c.ConnectionImplBase.NewStatement(ctx)
	if err != nil {
		return nil, err
	}
	if err := stmt.SetOption(ctx, OptionKeyZeroDatetimeBehavior, c.zeroDatetimeBehavior.String()); err != nil {
		closeErr := stmt.Close(ctx)
		if closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		return nil, err
	}
	return stmt, nil
}

func (c *mysqlConnectionImpl) GetOption(ctx context.Context, key string) (string, error) {
	switch key {
	case OptionKeyZeroDatetimeBehavior:
		return c.zeroDatetimeBehavior.String(), nil
	case adbc.OptionKeyIsolationLevel:
		return string(c.currentIsolationLevel), nil
	default:
		return c.ConnectionImplBase.GetOption(ctx, key)
	}
}

func (c *mysqlConnectionImpl) SetOption(ctx context.Context, key, value string) error {
	switch key {
	case OptionKeyZeroDatetimeBehavior:
		behavior, err := parseZeroDatetimeBehavior(value, &c.Base().ErrorHelper)
		if err != nil {
			return err
		}
		c.zeroDatetimeBehavior = behavior
		return nil
	case adbc.OptionKeyIsolationLevel:
		level := adbc.OptionIsolationLevel(value)
		stmt, err := mysqlIsolationLevelStatement(level)
		if err != nil {
			return err
		}
		if _, err := c.Conn.ExecContext(ctx, stmt); err != nil {
			return c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "set isolation level failed: %v", err)
		}
		c.currentIsolationLevel = level
		return nil
	default:
		return c.ConnectionImplBase.SetOption(ctx, key, value)
	}
}

func (s *mysqlStatement) GetOption(ctx context.Context, key string) (string, error) {
	switch key {
	case OptionKeyZeroDatetimeBehavior:
		return s.zeroDatetimeBehavior.String(), nil
	default:
		return s.StatementImplBase.GetOption(ctx, key)
	}
}

func (s *mysqlStatement) SetOption(ctx context.Context, key, value string) error {
	switch key {
	case OptionKeyZeroDatetimeBehavior:
		behavior, err := parseZeroDatetimeBehavior(value, &s.Base().ErrorHelper)
		if err != nil {
			return err
		}
		s.zeroDatetimeBehavior = behavior
		return nil
	default:
		return s.StatementImplBase.SetOption(ctx, key, value)
	}
}

// infoSqlIdentifierQuoteChar is the Flight SQL GetSqlInfo code for
// SQL_IDENTIFIER_QUOTE_CHAR, in the [500, 1000) XDBC range reserved by ADBC.
const infoSqlIdentifierQuoteChar = 504

// NewDriver constructs the ADBC Driver for "mysql".
func NewDriver(alloc memory.Allocator) driverbase.DriverWithContext {
	factory := &mysqlConnectionFactory{}
	driver := sqlwrapper.NewDriver(alloc, "mysql", "MySQL", NewMySQLDBFactory()).
		WithDatabaseFactory(factory).
		WithConnectionFactory(factory).
		WithStatementFactory(factory).
		WithErrorInspector(MySQLErrorInspector{})
	driver.DriverInfo.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoDriverName:                       "ADBC Driver Foundry Driver for MySQL",
		adbc.InfoVendorSql:                        true,
		adbc.InfoVendorSubstrait:                  false,
		adbc.InfoCode(infoSqlIdentifierQuoteChar): "`",
	})
	return driver
}
