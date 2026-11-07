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

import argparse
import functools
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

from adbc_drivers_validation import generate_documentation, model

from . import mysql


@functools.cache
def get_quirks(version: str, *, vendor: str) -> model.DriverQuirks:
    if vendor == "mysql" and version == "9.7":
        return mysql.MySQLQuirks()
    elif vendor == "databend" and version == "12.2":
        return mysql.DatabendQuirks()
    elif vendor not in ("mysql", "databend"):
        raise ValueError(f"unsupported vendor: {vendor}")
    else:
        raise ValueError(f"unsupported {vendor} version: {version}")


def filter_reports(reports: list[Path], output: Path) -> list[Path]:
    """Copy reports for documentation, excluding MariaDB test cases."""
    filtered_reports = []
    for report in reports:
        tree = ET.parse(report)
        has_results = False
        for suite in tree.iter("testsuite"):
            for testcase in list(suite.findall("testcase")):
                driver = testcase.find("./properties/property[@name='driver']")
                if driver is None:
                    continue
                vendor = driver.get("value", "").partition(":")[0]
                if vendor == "mariadb":
                    suite.remove(testcase)
                else:
                    has_results = True
        if has_results:
            filtered = output / report.name
            tree.write(filtered, encoding="utf-8", xml_declaration=True)
            filtered_reports.append(filtered)
    return filtered_reports


def generate_docs(output: Path) -> None:
    """Generate MySQL and Databend documentation from validation reports."""

    template = Path(__file__).parent.parent.parent / "docs/mysql.md"
    template = template.resolve()

    reports = sorted(Path(".").glob("validation-report*.xml"))
    with tempfile.TemporaryDirectory(prefix="mysql-docs-") as temporary:
        reports = filter_reports(reports, Path(temporary))
        if not reports:
            print("No MySQL or Databend validation results; skipping documentation.")
            return
        generate_documentation.generate(
            "mysql",
            get_quirks,
            [("mysql", "MySQL"), ("databend", "Databend")],
            reports,
            template,
            output.resolve(),
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    generate_docs(args.output)
