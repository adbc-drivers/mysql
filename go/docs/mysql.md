---
# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
{}
---

{{ cross_reference|safe }}
# MySQL Driver {{ version }}

{{ version_header|safe }}

This driver provides access to [MySQL][mysql]{target="_blank"}, a free and
open-source relational database management system.

## Installation & Quickstart

The driver can be installed with `dbc`.

To use the driver, provide a MySQL connection string as the `url` option. The driver supports both standard MySQL URIs and DSN-style connection strings but standard URIs are recommended.

## Connection String Format

MySQL's standard URI syntax:

```
mysql://[user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...]
```

This follows MySQL's official URI-like connection string format [MySQL URI-Like Connection String](https://dev.mysql.com/doc/refman/8.4/en/connecting-using-uri-or-key-value-pairs.html#connecting-using-uri). Also see [MySQL Connection Parameters](https://dev.mysql.com/doc/refman/8.4/en/connecting-using-uri-or-key-value-pairs.html#connection-parameters-base) for the complete specification.

Components:
- Scheme: mysql:// (required)
- User: Optional (for authentication)
- Password: Optional (for authentication, requires user)
- Host: Required (defaults to localhost, or socket path)
- Port: Optional (defaults to 3306)
- Schema: Optional (can be empty, MySQL database name)
- Query params: MySQL connection attributes

**Important**: Percent encoding must be used for reserved characters in URI elements. For example, `@` becomes `%40`. If you include a zone ID in an IPv6 address, the `%` character used as the separator must be replaced with `%25`.

**Socket Connections**: Use either percent encoding (`/path%2Fto%2Fsocket.sock`) or parentheses (`(/path/to/socket.sock)`).

Examples:
- mysql://localhost/mydb
- mysql://user:pass@localhost:3306/mydb
- mysql://user:pass@host/db?charset=utf8mb4&timeout=30s
- mysql://user@/path%2Fto%2Fsocket.sock/db (socket with percent encoding)
- mysql://user@(/path/to/socket.sock)/db (socket with parentheses)
- mysql://user@localhost/mydb (no password)

The driver also supports traditional MySQL DSN format (see [Go MySQL Driver documentation](https://github.com/go-sql-driver/mysql?tab=readme-ov-file#dsn-data-source-name)), but standard URIs are recommended.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

{{ footnotes|safe }}

[mysql]: https://www.mysql.com/
