<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Connection string format for MySQL

mysql://[username[:password]@]host[:port][/database][?param1=value1&param2=value2]

- Scheme: mysql:// (required)
- Username: Optional (for authentication)
- Password: Optional (for authentication, requires username)
- Host: Required (no default)
- Port: Optional (defaults to 3306)
- Database: Optional (can be empty)
- Query params: All MySQL DSN parameters supported

# Examples

## Basic
mysql://localhost/mydb

## With auth
mysql://user:pass@localhost:3306/mydb

## With parameters
mysql://user:pass@host/db?charset=utf8mb4&timeout=30s

## Unix socket (special syntax)
mysql://user@unix(/tmp/mysql.sock)/db

## No database
mysql://user:pass@host/
