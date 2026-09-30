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

import adbc_drivers_validation.tests.ingest
import pytest

from . import mysql


def pytest_generate_tests(metafunc) -> None:
    quirks = [mysql.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.ingest.generate_tests(quirks, metafunc)


class TestIngest(adbc_drivers_validation.tests.ingest.TestIngest):
    def test_temporary_get_objects(self, driver, conn_factory, query) -> None:
        if driver.vendor_name == "MySQL":
            pytest.xfail(
                reason="MySQL INFORMATION_SCHEMA.TABLES does not list temporary tables: "
                "https://dev.mysql.com/doc/refman/9.7/en/information-schema-tables-table.html"
            )
        super().test_temporary_get_objects(driver, conn_factory, query)
