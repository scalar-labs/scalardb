from pathlib import Path

from config_loader import load_monitors_config
from pin_reader import PinReader


def test_resolve_postgresql_jdbc_driver(repo_root: Path):
    reader = PinReader(repo_root)
    component = {
        "current_version": {
            "source": "gradle",
            "file": "build.gradle",
            "property": "postgresqlDriverVersion",
        }
    }
    pin = reader.resolve(component["current_version"])
    assert pin


def test_resolve_postgresql_server_highest(repo_root: Path):
    reader = PinReader(repo_root)
    component = {
        "current_version": {
            "source": "tests-config",
            "file": "ci/tests-config.yaml",
            "filter": "postgresql",
            "strategy": "highest",
        }
    }
    pin = reader.resolve(component["current_version"])
    assert pin == "17"


def test_build_pin_snapshot_loads_all_components(repo_root: Path):
    config = load_monitors_config(repo_root / "ci/db-update-monitor/config/monitors.yaml")
    reader = PinReader(repo_root)
    snapshot = reader.build_pin_snapshot(config["databases"])
    assert "postgresql:jdbc-driver" in snapshot
    assert "cosmos:sdk" in snapshot
