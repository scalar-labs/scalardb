"""Checks of the real configuration, so mistakes fail on the pull request, not the weekly run."""

import copy
from pathlib import Path

import pytest

from config_loader import load_monitors_config, load_yaml
from config_schema import ConfigError, validate_monitors_config, validate_relevance_rules
from pin_reader import PinReader

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"


def _real_config():
    return load_yaml(CONFIG_DIR / "monitors.yaml")


def test_monitors_yaml_is_valid():
    assert validate_monitors_config(_real_config()) == []


def test_relevance_rules_yaml_is_valid():
    assert validate_relevance_rules(load_yaml(CONFIG_DIR / "relevance-rules.yaml")) == []


def test_every_pin_resolves(repo_root: Path):
    """Fails when a Gradle property is renamed or a tests-config label disappears."""
    reader = PinReader(repo_root)
    for database in load_monitors_config(CONFIG_DIR / "monitors.yaml")["databases"]:
        for component in database.get("components", []):
            pin = reader.resolve(component["current_version"])
            assert pin, f"{database['id']}/{component['id']} has an empty pin"


def _broken(change):
    config = copy.deepcopy(_real_config())
    change(config)
    return validate_monitors_config(config)


def _cosmos(config):
    return next(db for db in config["databases"] if db["id"] == "cosmos")


def test_unknown_key_is_reported():
    errors = _broken(lambda c: _cosmos(c)["sources"][0].update(match_scop="title"))
    assert errors == ["databases[cosmos].sources[0]: unknown key 'match_scop'"]


def test_missing_required_key_is_reported():
    errors = _broken(lambda c: _cosmos(c)["components"][0]["upstream"].pop("artifact"))
    assert "databases[cosmos].components[sdk].upstream: missing required key 'artifact'" in errors


def test_unknown_types_are_reported():
    def change(config):
        _cosmos(config)["components"][0]["type"] = "database"
        _cosmos(config)["components"][0]["upstream"]["type"] = "npm"
        _cosmos(config)["sources"][0]["type"] = "atom"

    errors = _broken(change)
    assert len(errors) == 3
    assert all("must be one of" in error for error in errors)


def test_bad_values_are_reported():
    def change(config):
        _cosmos(config)["components"][0]["version_granularity"] = "patch"
        _cosmos(config)["components"][0]["upstream"]["version_pattern"] = "("
        _cosmos(config)["sources"][0]["match_scope"] = "body"

    errors = _broken(change)
    assert len(errors) == 3


def test_tests_config_pin_needs_exactly_one_of_path_or_filter():
    def change(config):
        cassandra = next(db for db in config["databases"] if db["id"] == "cassandra")
        cassandra["components"][1]["current_version"]["filter"] = "cassandra"

    errors = _broken(change)
    assert errors == [
        "databases[cassandra].components[server].current_version: set exactly one of 'path' or 'filter'"
    ]


def test_duplicate_ids_are_reported():
    errors = _broken(lambda c: c["databases"].append(copy.deepcopy(_cosmos(c))))
    assert "databases: duplicate id 'cosmos'" in errors


def test_gap_keywords_must_name_an_adapter_capability():
    errors = _broken(lambda c: _cosmos(c)["adapter_gap_keywords"].update(consistency=["strong"]))
    assert errors == [
        "databases[cosmos].adapter_gap_keywords: 'consistency' is not in adapter_capabilities"
    ]


def test_loading_an_invalid_config_raises_with_every_problem(tmp_path: Path):
    path = tmp_path / "monitors.yaml"
    path.write_text(
        "databases:\n  - id: x\n    name: X\n    colour: blue\n    sources:\n      - type: rss\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError) as error:
        load_monitors_config(path)
    message = str(error.value)
    assert "unknown key 'colour'" in message
    assert "missing required key 'url'" in message
