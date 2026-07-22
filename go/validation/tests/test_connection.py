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

import adbc_drivers_validation.tests.connection
import pytest
from adbc_drivers_validation.tests.connection import (
    TestConnection,  # noqa: F401
)

from . import mysql


def pytest_generate_tests(metafunc) -> None:
    test_config = metafunc.config.getoption("vendor_version")
    quirks = [mysql.get_quirks(test_config)]
    if (
        metafunc.definition.name == "test_get_objects_table_not_exist"
        and test_config == "databend"
    ):
        metafunc.parametrize(
            "driver",
            [
                pytest.param(
                    "databend:12.2",
                    id="databend:12.2",
                    marks=pytest.mark.skip("get_objects is disabled for Databend MySQL wire"),
                )
            ],
            scope="module",
            indirect=["driver"],
        )
        return
    return adbc_drivers_validation.tests.connection.generate_tests(quirks, metafunc)
