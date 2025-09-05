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

from pathlib import Path

from adbc_drivers_validation import model, quirks


class MySQLQuirks(model.DriverQuirks):
    name = "mysql"
    driver = "adbc_driver_mysql"
    driver_name = "ADBC Driver Foundry Driver for MySQL"
    vendor_name = "MySQL"
    vendor_version = "9.4.0 (MySQL Community Server - GPL)"
    short_version = "9.4"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_transactions=False,
        get_objects_constraints_foreign=False,
        get_objects_constraints_primary=False,
        get_objects_constraints_unique=False,
        statement_bulk_ingest=True,
        statement_bulk_ingest_catalog=True,
        statement_bulk_ingest_schema=True,
        statement_bulk_ingest_temporary=False,
        statement_execute_schema=True,
        statement_get_parameter_schema=False,
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
        return (Path(__file__).parent.parent / "queries",)

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

    def quote_identifier(self, *identifiers: str) -> str:
        # Filter out None and empty values (MySQL doesn't have separate schemas/catalogs)
        valid_identifiers = [
            self.quote_one_identifier(ident)
            for ident in identifiers
            if ident is not None and ident != ""
        ]
        return ".".join(valid_identifiers)

    def quote_one_identifier(self, identifier: str) -> str:
        if identifier is None:
            return None
        identifier = identifier.replace("`", "``")
        return f"`{identifier}`"

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement)


QUIRKS = [MySQLQuirks()]
