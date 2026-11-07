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

import xml.etree.ElementTree as ET
from pathlib import Path

from . import generate_documentation as docs


def write_report(path: Path, drivers: list[str]) -> None:
    root = ET.Element("testsuites")
    suite = ET.SubElement(root, "testsuite", name="validation")
    for driver in drivers:
        case = ET.SubElement(suite, "testcase", name=f"test_get_info[{driver}]")
        properties = ET.SubElement(case, "properties")
        ET.SubElement(properties, "property", name="driver", value=driver)
    ET.ElementTree(root).write(path)


def report_drivers(path: Path) -> list[str]:
    return [
        prop.get("value")
        for prop in ET.parse(path).findall(".//property[@name='driver']")
    ]


def test_filter_mixed_reports_preserves_originals(tmp_path):
    mixed = tmp_path / "validation-report.xml"
    mariadb = tmp_path / "validation-report-mariadb.xml"
    drivers = ["mysql:9.7", "mariadb:12.2", "databend:12.2"]
    write_report(mixed, drivers)
    write_report(mariadb, ["mariadb:12.2"])
    filtered = tmp_path / "filtered"
    filtered.mkdir()

    reports = docs.filter_reports([mixed, mariadb], filtered)

    assert len(reports) == 1
    assert report_drivers(reports[0]) == ["mysql:9.7", "databend:12.2"]
    assert report_drivers(mixed) == drivers
    assert report_drivers(mariadb) == ["mariadb:12.2"]


def test_mariadb_only_skips_documentation(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    write_report(tmp_path / "validation-report.xml", ["mariadb:12.2"])

    def unexpected_generation(*args):
        raise AssertionError("MariaDB results must not generate MySQL documentation")

    monkeypatch.setattr(docs.generate_documentation, "generate", unexpected_generation)

    docs.generate_docs(tmp_path / "generated")

    assert not (tmp_path / "generated").exists()
    assert "skipping documentation" in capsys.readouterr().out


def test_combined_docs_only_include_mysql_and_databend(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for driver in ["mysql:9.7", "mariadb:12.2", "databend:12.2"]:
        vendor = driver.partition(":")[0]
        write_report(tmp_path / f"validation-report-{vendor}.xml", [driver])
    included = []

    def generate(driver, get_quirks, vendor_mapping, reports, template, output):
        assert vendor_mapping == [("mysql", "MySQL"), ("databend", "Databend")]
        for report in reports:
            included.extend(report_drivers(report))

    monkeypatch.setattr(docs.generate_documentation, "generate", generate)

    docs.generate_docs(tmp_path / "generated")

    assert sorted(included) == ["databend:12.2", "mysql:9.7"]
