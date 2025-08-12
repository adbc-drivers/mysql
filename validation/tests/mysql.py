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

from adbc_drivers_validation import model


class MySQLQuirks(model.DriverQuirks):
    name = "mysql"
    driver = "adbc_driver_mysql"
    driver_name = "ADBC Driver for MySQL"
    vendor_name = "MySQL"
    features = model.DriverFeatures(
        connection_get_table_schema=False,
        connection_transactions=False,
        get_objects_constraints_foreign=False,
        get_objects_constraints_primary=False,
        get_objects_constraints_unique=False,
        statement_bulk_ingest=False,
        statement_bulk_ingest_catalog=False,
        statement_bulk_ingest_schema=False,
        statement_bulk_ingest_temporary=False,
        statement_execute_schema=False,
        statement_get_parameter_schema=False,
        current_catalog="dev",
        current_schema="public",
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
    def queries_path(self) -> Path:
        return Path(__file__).parent.parent / "queries"

    def bind_parameter(self, index: int) -> str:
        return f"${index}"

    # def drop_table(
    #     self,
    #     *,
    #     table_name: str,
    #     schema_name: str | None = None,
    #     catalog_name: str | None = None,
    #     if_exists: bool = True,
    #     temporary: bool = False,
    # ) -> None:
    #     if temporary:
    #         if catalog_name or schema_name:
    #             raise ValueError("Cannot pass catalog/schema name for temporary table")
    #         table_name = self.qualify_temp_table(table_name)
    #         if if_exists:
    #             return f"DROP TABLE IF EXISTS {table_name}"
    #         return f"DROP TABLE {table_name}"

    #     return super().drop_table(
    #         table_name=table_name,
    #         schema_name=schema_name,
    #         catalog_name=catalog_name,
    #         if_exists=if_exists,
    #         temporary=temporary,
    #     )

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        raise error

    # def split_statement(self, statement: str) -> list[str]:
    #     return quirks.split_statement(statement)
