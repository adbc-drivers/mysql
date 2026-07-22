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

# Validation Suite Setup

1. Start the Docker container:

   ```shell
   docker compose up --detach --wait
   ```
2. Set the environment variable (supports both DSN and MySQL URI formats):

   ```shell
   export MYSQL_DSN="my:password@tcp(localhost:3306)/db"
   # OR
   export MYSQL_DSN="mysql://my:password@localhost:3306/db"
   ```

   Alternate targets:

   ```shell
   export MARIADB_DSN="my:password@tcp(localhost:3307)/db"
   export DATABEND_DSN="root@tcp(localhost:3308)/default"
   ```
3. Run the validation suite:

   ```shell
   pixi run validate --vendor-version mysql
   pixi run validate --vendor-version mariadb
   pixi run validate --vendor-version databend
   ```
