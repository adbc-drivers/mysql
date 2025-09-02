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
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
)

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *mysqlConnectionImpl) GetCurrentCatalog() (string, error) {
	var database string
	err := c.Db.QueryRowContext(context.Background(), "SELECT DATABASE()").Scan(&database)
	if err != nil {
		return "", fmt.Errorf("failed to get current database: %w", err)
	}
	if database == "" {
		return "", fmt.Errorf("no current database set")
	}
	return database, nil
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *mysqlConnectionImpl) GetCurrentDbSchema() (string, error) {
	return "", nil
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *mysqlConnectionImpl) SetCurrentCatalog(catalog string) error {
	_, err := c.Db.ExecContext(context.Background(), "USE "+catalog)
	return err
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *mysqlConnectionImpl) SetCurrentDbSchema(schema string) error {
	if schema != "" {
		return fmt.Errorf("cannot set schema in MySQL: schemas are not supported")
	}
	return nil
}

func (c *mysqlConnectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if c.version == "" {
		var version, comment string
		if err := c.Conn.QueryRowContext(ctx, "SELECT @@version, @@version_comment").Scan(&version, &comment); err != nil {
			return c.ErrorHelper.Errorf(adbc.StatusInternal, "failed to get version: %v", err)
		}
		c.version = fmt.Sprintf("%s (%s)", version, comment)
	}
	return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorVersion, c.version)
}
