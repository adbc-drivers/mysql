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
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/go-sql-driver/mysql"
)

// MySQLDBFactory provides MySQL-specific database connection creation.
// It uses the go-sql-driver/mysql Config struct for proper DSN formatting.
type MySQLDBFactory struct {
}

// NewMySQLDBFactory creates a new MySQLDBFactory.
func NewMySQLDBFactory() *MySQLDBFactory {
	return &MySQLDBFactory{}
}

// CreateDB creates a *sql.DB using sql.Open with a MySQL-specific DSN.
func (f *MySQLDBFactory) CreateDB(ctx context.Context, driverName string, opts map[string]string, logger *slog.Logger) (*sql.DB, error) {
	dsn, err := f.BuildMySQLDSN(opts)
	if err != nil {
		return nil, err
	}

	// Force UTC timezone for all connections to ensure consistent timestamp handling.
	dsn, err = f.forceUTCTimezone(dsn, logger)
	if err != nil {
		return nil, err
	}

	return sql.Open(driverName, dsn)
}

// forceUTCTimezone parses the DSN and overrides the time_zone and loc parameters to UTC
func (f *MySQLDBFactory) forceUTCTimezone(dsn string, logger *slog.Logger) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("failed to parse DSN for timezone override: %v", err)
	}

	if existingTz, exists := cfg.Params["time_zone"]; exists && existingTz != "'+00:00'" && existingTz != "'UTC'" {
		if logger != nil {
			logger.Warn("time_zone parameter is not supported, overriding to UTC",
				"requested_timezone", existingTz,
				"reason", "UTC is required for ADBC MySQL driver")
		}
	}

	if existingLoc, exists := cfg.Params["loc"]; exists && existingLoc != "UTC" {
		if logger != nil {
			logger.Warn("loc parameter is not supported, overriding to UTC",
				"requested_loc", existingLoc,
				"reason", "UTC is required for ADBC MySQL driver")
		}
	}

	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	cfg.Params["time_zone"] = "'+00:00'"
	cfg.Params["loc"] = "UTC"

	return cfg.FormatDSN(), nil
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
		// Return plain Go error. sqlwrapper will catch and wrap it with ErrorHelper and turn it into adbc error
		return "", fmt.Errorf("missing required option %s", adbc.OptionKeyURI)
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
		return "", fmt.Errorf("invalid MySQL URI format: %v", err)
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
			return "", fmt.Errorf("invalid MySQL URI: missing closing ')' for socket path in %s", u.Path)
		}

		cfg.Addr = u.Path[:closeParenIndex]
		dbPath = u.Path[closeParenIndex+1:]

	case "":
		// Case 2: Empty host is invalid - hostname must be explicit
		// Use parentheses syntax for sockets: mysql://user@(/path/to/socket)/db
		return "", fmt.Errorf("missing hostname in URI: %s. Use explicit hostname or socket syntax: mysql://user@(socketpath)/db", mysqlURI)

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
			return "", fmt.Errorf("invalid MySQL DSN format: %v", err)
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

var _ sqlwrapper.DBFactory = (*MySQLDBFactory)(nil)
