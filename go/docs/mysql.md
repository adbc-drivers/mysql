---
# Copyright (c) 2026 ADBC Drivers Contributors
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

{{ heading|safe }}

This driver provides access to [MySQL][mysql]{target="_blank"}, a free and
open-source relational database management system.

## Installation

The MySQL driver can be installed with [dbc](https://docs.columnar.tech/dbc):

```bash
dbc install mysql
```

## Connecting

To connect, edit the `uri` option below to match your environment and run the following:

```python
from adbc_driver_manager import dbapi

conn = dbapi.connect(
  driver="mysql",
  db_kwargs = {
    "uri": "mysql://root@localhost:3306/demo"
  }
)
```

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers. See [adbc-quickstarts](https://github.com/columnar-tech/adbc-quickstarts).

### Connection String Format

Connection strings are passed with the `uri` option. The driver supports two formats:

#### MySQL URI Format (Recommended)

The standard MySQL URI format, following MySQL's official specification:

```text
mysql://[user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...]
```

Examples:

- `mysql://localhost/mydb`
- `mysql://user:pass@localhost:3306/mydb`
- `mysql://user:pass@host/db?charset=utf8mb4&timeout=30s`
- `mysql://user@(/path/to/socket.sock)/db` (Unix domain socket)
- `mysql://user@localhost/mydb` (no password)

URI Components:
- `scheme`: `mysql://` (required)
- `user`: Optional (for authentication)
- `password`: Optional (for authentication, requires user)
- `host`: Required (must be explicitly specified)
- `port`: Optional (defaults to 3306)
- `schema`: Optional (can be empty, MySQL database name)
- Query params: MySQL connection attributes

:::{note}
Reserved characters in URI elements must be URI-encoded. For example, `@` becomes `%40`. If you include a zone ID in an IPv6 address, the `%` character used as the separator must be replaced with `%25`.
:::

Unix Domain Sockets:
When connecting via Unix domain sockets, use the parentheses syntax to wrap the socket path: `mysql://user@(/path/to/socket.sock)/db`

For complete details, see MySQL's [URI-like connection string format](https://dev.mysql.com/doc/refman/8.4/en/connecting-using-uri-or-key-value-pairs.html#connecting-using-uri) and [Connection Parameters](https://dev.mysql.com/doc/refman/8.4/en/connecting-using-uri-or-key-value-pairs.html#connection-parameters-base) documentation.

#### Go MySQL Driver DSN Format (Alternative)

The driver also accepts the [Go MySQL Driver DSN format](https://github.com/go-sql-driver/mysql?tab=readme-ov-file#dsn-data-source-name):

```text
[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
```

Examples:

- `user:pass@tcp(localhost:3306)/mydb`
- `user@tcp(127.0.0.1:3306)/mydb`
- `user:pass@unix(/tmp/mysql.sock)/mydb`

This format is used internally by the driver and may be convenient if you're already familiar with the Go MySQL driver. However, the MySQL URI format is recommended for better alignment with MySQL's official documentation and tooling.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

## Compatibility

{{ compatibility_info|safe }}

## Previous Versions

To see documentation for previous versions of this driver, see the following:

- [v0.3.0](./v0.3.0.md)
- [v0.2.0](./v0.2.0.md)
- [v0.1.0](./v0.1.0.md)

{{ footnotes|safe }}

[mysql]: https://www.mysql.com/
