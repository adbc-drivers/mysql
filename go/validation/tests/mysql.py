# Copyright (c) 2025-2026 ADBC Drivers Contributors
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

import functools
from pathlib import Path

from adbc_drivers_validation import model, quirks


class MySQLQuirks(model.DriverQuirks):
    name = "mysql"
    driver = "adbc_driver_mysql"
    driver_name = "ADBC Driver Foundry Driver for MySQL"
    vendor_name = "MySQL"
    vendor_version = "9.7.1 (MySQL Community Server - GPL)"
    short_version = "9.7"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_transactions=False,
        get_objects=True,
        get_objects_constraints_foreign=False,
        get_objects_constraints_primary=False,
        get_objects_constraints_unique=False,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_bulk_ingest_catalog=False,
        statement_bulk_ingest_schema=False,
        statement_bulk_ingest_temporary=True,
        statement_execute_schema=True,
        statement_get_parameter_schema=False,
        statement_prepare=True,
        statement_rows_affected=True,
        statement_rows_affected_ddl=True,
        quirk_bulk_ingest_temporary_shares_namespace=True,
        current_catalog="db",  # MySQL treats databases as catalogs (also JDBC behavior)
        current_schema="",  # getSchemas() returns empty - no schema concept (also JDBC behavior)
        supported_xdbc_fields=[],
    )
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("MYSQL_DSN"),
        },
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries/mysql",)

    def bind_parameter(self, index: int) -> str:
        return "?"

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        # Check if the error indicates a table not found condition
        error_str = str(error).lower()
        return (
            "table" in error_str
            and (
                "does not exist" in error_str
                or "doesn't exist" in error_str
                or "not found" in error_str
            )
            and table_name.lower() in error_str
        )

    def query_override(self, context: str, default: str) -> str:
        if context == "TestStatement.sample_table":
            return "CREATE TABLE `sample_table` (id INT, value TEXT)"
        return super().query_override(context, default)

    def quote_one_identifier(self, identifier: str) -> str:
        identifier = identifier.replace("`", "``")
        return f"`{identifier}`"

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement)

    def qualify_temp_table(self, _cursor, name: str) -> str:
        return name


class MariaDBQuirks(MySQLQuirks):
    name = "mariadb"
    vendor_version = "12.2.2-MariaDB-ubu2404 (mariadb.org binary distribution)"
    short_version = "12.2"

    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("MARIADB_DSN"),
        },
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return super().queries_paths + (
            Path(__file__).parent.parent / "queries/mariadb",
        )


@functools.cache
def get_quirks(test_config: str) -> model.DriverQuirks:
    if test_config == "mysql":
        return MySQLQuirks()
    elif test_config == "mariadb":
        return MariaDBQuirks()
    else:
        raise ValueError(f"unsupported test config: {test_config}")
