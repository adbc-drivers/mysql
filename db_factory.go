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
	dsn, err := f.buildMySQLDSN(opts)
	if err != nil {
		return nil, err
	}

	return sql.Open(driverName, dsn)
}

// buildMySQLDSN constructs a MySQL DSN from the provided options.
func (f *MySQLDBFactory) buildMySQLDSN(opts map[string]string) (string, error) {
	baseURI := opts[adbc.OptionKeyURI]
	username := opts[adbc.OptionKeyUsername]
	password := opts[adbc.OptionKeyPassword]

	// If no base URI provided, this is an error
	if baseURI == "" {
		return "", f.errorHelper.InvalidArgument("missing required option %s", adbc.OptionKeyURI)
	}

	// If no credentials provided, return original URI
	if username == "" && password == "" {
		return baseURI, nil
	}

	// Handle native MySQL DSN formats
	return f.buildFromNativeDSN(baseURI, username, password)
}

// buildFromNativeDSN handles MySQL's native DSN format and plain host strings.
func (f *MySQLDBFactory) buildFromNativeDSN(baseURI, username, password string) (string, error) {
	var cfg *mysql.Config
	var err error

	// Try to parse as existing MySQL DSN
	if strings.Contains(baseURI, "@") || strings.Contains(baseURI, "/") {
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
