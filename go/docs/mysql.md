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
# MySQL/MariaDB Driver {{ version }}

{{ heading|safe }}

This driver provides access to [MySQL][mysql] and [MariaDB][mariadb], free and
open-source relational database management systems.

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
    driver="mysql", db_kwargs={"uri": "mysql://root@localhost:3306/demo"}
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

## Feature & Type Support

{{ features|safe }}

### Catalog and Schema Support

MySQL only has databases. The driver treats MySQL databases as catalogs and leaves schemas empty. To explicitly specify the database name when querying the catalog, bulk ingesting, or in other operations, pass it as the ADBC catalog.

### Substrait

The driver accepts Substrait plans via `AdbcStatementSetSubstraitPlan` by translating them to SQL. The supported relations, expressions, functions and types are listed in the driver's [Substrait dialect](https://github.com/adbc-drivers/mysql/blob/main/go/substrait_dialect.yaml). Plans that use anything else return `ADBC_STATUS_NOT_IMPLEMENTED`, and malformed plans return `ADBC_STATUS_INVALID_ARGUMENT`.

In addition, the following restrictions apply, which a dialect can't express:

- A plan is a `RelRoot` over any combination of `FilterRel`s and `FetchRel`s over a `ReadRel` over a `NamedTable`, whose name is either `[table]` (resolved against the current database) or `[database, table]`. The `ReadRel` can't have a projection. Its `best_effort_filter` is applied in full if all of it is supported, and ignored otherwise.
- A `FetchRel`'s offset and count must be integer literals (or null).
- Output column names must be unique, ignoring case.
- Types can't have type variations.
- Only `boolean`, `i8`, `i16`, `i32`, `i64`, `fp32` and `fp64` (except infinity and NaN), `decimal` and `date` (from 1000-01-01 to 9999-12-31) values can be literals. Null literals can have any supported type.
- Comparisons are supported for booleans, integers, floating point numbers, decimals, dates and timestamps, and both sides must have the same type, including decimal precision and scale and timestamp precision.

The column types and nullability in the `ReadRel`'s `base_schema` are not checked against the table: result types are those MySQL reports for the selected columns, and filters rely on the declared types. Filters compare the values stored in MySQL, which may differ from the declared types:

- A `boolean` column is usually a `TINYINT(1)` in MySQL, which can hold values other than 0 and 1. `equal(b, true)` only matches 1, while `is_true(b)` and `not(b)` treat any non-zero value as true.
- Zero dates (e.g. `0000-00-00`) are compared as dates before any other date, and are never NULL in filters, including with `mysql.query.zero_datetime_behavior=convert_to_null`, which only converts them to NULL in results.

### Types

{{ types|safe }}

## Options

`mysql.query.zero_datetime_behavior`
: **Values:** `error`, `convert_to_null`. **Default:** `error`.

  Control what to do with DATE and TIMESTAMP values that contain zero components in the date (e.g. `0000-00-00`), which MySQL allows for backwards compatibility. By default, this will error; `convert_to_null` will instead treat these values as equivalent to null.

`mysql.vendor`
: **Values:** `mysql`, `mariadb`, `databend`, or another backend identifier. **Default:** `mysql`.

  Controls whether the driver injects MySQL's UTC session timezone parameter into the DSN. Leave this unset for MySQL and MariaDB to use MySQL's `time_zone`. Other non-MySQL backends, including Databend, still get UTC client-side timestamp parsing, but no session timezone parameter is injected.

## Compatibility

{{ compatibility_info|safe }}

## Previous Versions

To see documentation for previous versions of this driver, see the following:

- [v0.3.1](./v0.3.1.md)
- [v0.3.0](./v0.3.0.md)
- [v0.2.0](./v0.2.0.md)
- [v0.1.0](./v0.1.0.md)

{{ footnotes|safe }}

[mariadb]: https://mariadb.org/
[mysql]: https://www.mysql.com/
