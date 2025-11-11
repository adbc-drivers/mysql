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
	"net/url"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/go-sql-driver/mysql"
)

// MySQLDBFactory provides MySQL-specific database connection creation.
// It uses the go-sql-driver/mysql Config struct for proper DSN formatting.
type MySQLDBFactory struct {
	errorHelper driverbase.ErrorHelper
}

// NewMySQLDBFactory creates a new MySQLDBFactory with proper error handling.
func NewMySQLDBFactory() *MySQLDBFactory {
	return &MySQLDBFactory{
		errorHelper: driverbase.ErrorHelper{DriverName: "mysql"},
	}
}

// CreateDB creates a *sql.DB using sql.Open with a MySQL-specific DSN.
func (f *MySQLDBFactory) CreateDB(ctx context.Context, driverName string, opts map[string]string) (*sql.DB, error) {
	dsn, err := f.BuildMySQLDSN(opts)
	if err != nil {
		return nil, err
	}

	return sql.Open(driverName, dsn)
}

// buildMySQLDSN constructs a MySQL DSN from the provided options.
// Handles the following scenarios:
//  1. MySQL URI: "mysql://user:pass@host:port/schema?params" → converted to DSN
//  2. Full DSN: "user:pass@tcp(host:port)/db" → returned as-is or credentials updated
//  3. Plain host + credentials: "localhost:3306" + username/password → converted to DSN
func (f *MySQLDBFactory) BuildMySQLDSN(opts map[string]string) (string, error) {
	baseURI := opts[adbc.OptionKeyURI]
	username := opts[adbc.OptionKeyUsername]
	password := opts[adbc.OptionKeyPassword]

	// If no base URI provided, this is an error
	if baseURI == "" {
		return "", f.errorHelper.InvalidArgument("missing required option %s", adbc.OptionKeyURI)
	}

	// Check if this is a MySQL URI (mysql://)
	if strings.HasPrefix(baseURI, "mysql://") {
		return f.parseToMySQLDSN(baseURI, username, password)
	}

	if username == "" && password == "" {
		return baseURI, nil
	}
	return f.buildFromNativeDSN(baseURI, username, password)
}

// parseToMySQLDSN converts a MySQL URI to MySQL DSN format.
// Examples:
//
//	mysql://root@localhost:3306/demo → root@tcp(localhost:3306)/demo
//	mysql://user:pass@host/db?charset=utf8mb4 → user:pass@tcp(host:3306)/db?charset=utf8mb4
//	mysql://user@(/path/to/socket.sock)/db → user@unix(/path/to/socket.sock)/db
func (f *MySQLDBFactory) parseToMySQLDSN(mysqlURI, username, password string) (string, error) {
	u, err := url.Parse(mysqlURI)
	if err != nil {
		return "", f.errorHelper.InvalidArgument("invalid MySQL URI format: %v", err)
	}

	cfg := mysql.NewConfig()

	if u.User != nil {
		cfg.User = u.User.Username()
		if pass, hasPass := u.User.Password(); hasPass {
			cfg.Passwd = pass
		}
	}

	if username != "" {
		cfg.User = username
	}
	if password != "" {
		cfg.Passwd = password
	}

	var dbPath string

	// MySQL socket URIs have non-standard hostname patterns that require special handling after parsing.
	switch u.Hostname() {
	case "(":
		// Case 1: Socket with parentheses: mysql://user@(/path/to/socket.sock)/db
		cfg.Net = "unix"

		closeParenIndex := strings.Index(u.Path, ")")
		if closeParenIndex == -1 {
			return "", f.errorHelper.InvalidArgument("invalid MySQL URI: missing closing ')' for socket path in %s", u.Path)
		}

		cfg.Addr = u.Path[:closeParenIndex]
		dbPath = u.Path[closeParenIndex+1:]

	case "":
		// Case 2: Empty host, could be socket or default TCP connection
		// e.g., mysql://user@/tmp/mysql.sock/testdb (socket)
		// e.g., mysql://user@/db (default TCP connection)
		if sockIndex := strings.Index(u.Path, ".sock/"); sockIndex != -1 {
			// Socket with a database path: /path/to/socket.sock/db
			socketEnd := sockIndex + 5 // .sock
			cfg.Net = "unix"
			cfg.Addr = u.Path[:socketEnd]
			dbPath = u.Path[socketEnd:]
		} else if strings.HasSuffix(u.Path, ".sock") {
			// Socket with no database path: /path/to/socket.sock
			cfg.Net = "unix"
			cfg.Addr = u.Path
			dbPath = ""
		} else {
			// Fallback: Not a socket, treat as TCP to default host
			cfg.Net = "tcp"
			cfg.Addr = "localhost:3306"
			dbPath = u.Path
		}

	default:
		// Case 3: Regular TCP connection with a hostname
		cfg.Net = "tcp"
		if u.Port() != "" {
			cfg.Addr = u.Host
		} else {
			cfg.Addr = u.Host + ":3306"
		}
		dbPath = u.Path
	}

	// Extract database/schema from path
	if dbPath != "" && dbPath != "/" {
		// u.Path is already URL-decoded by url.Parse()
		// We just need to trim the leading slash.
		// cfg.FormatDSN() will correctly re-encode this if needed.
		cfg.DBName = strings.TrimPrefix(dbPath, "/")
	}

	dsn := cfg.FormatDSN()
	if u.RawQuery != "" {
		dsn += "?" + u.RawQuery
	}

	return dsn, nil
}

// buildFromNativeDSN handles MySQL's native DSN format and plain host strings.
func (f *MySQLDBFactory) buildFromNativeDSN(baseURI, username, password string) (string, error) {
	var cfg *mysql.Config
	var err error

	if strings.Contains(baseURI, "@") || strings.Contains(baseURI, "/") {
		// Try to parse as existing MySQL DSN
		cfg, err = mysql.ParseDSN(baseURI)
		if err != nil {
			return "", f.errorHelper.InvalidArgument("invalid MySQL DSN format: %v", err)
		}
	} else {
		// Treat as plain host string
		cfg = mysql.NewConfig()
		cfg.Addr = baseURI
		cfg.Net = "tcp"
	}

	// Override credentials if provided
	if username != "" {
		cfg.User = username
	}
	if password != "" {
		cfg.Passwd = password
	}

	return cfg.FormatDSN(), nil
}

// Ensure MySQLDBFactory implements sqlwrapper.DBFactory
var _ sqlwrapper.DBFactory = (*MySQLDBFactory)(nil)
