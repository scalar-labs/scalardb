#!/usr/bin/env python3
"""CLI entry point for the ScalarDB database update monitor."""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

import click

SRC_DIR = Path(__file__).resolve().parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from engine import MonitorEngine
from notifier.slack import build_slack_payload, write_slack_payload


@click.command()
@click.option("--config", "config_path", type=click.Path(path_type=Path), required=True)
@click.option("--state", "state_path", type=click.Path(path_type=Path), required=True)
@click.option("--output-dir", type=click.Path(path_type=Path), required=True)
@click.option("--repo-root", type=click.Path(path_type=Path), default=None)
@click.option("--filter", "database_filter", default=None, help="Run for one database id")
@click.option("--dry-run", is_flag=True, help="Do not persist state changes")
@click.option("--verbose", is_flag=True)
def main(
    config_path: Path,
    state_path: Path,
    output_dir: Path,
    repo_root: Path | None,
    database_filter: str | None,
    dry_run: bool,
    verbose: bool,
) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # config lives at <repo>/ci/db-update-monitor/config/monitors.yaml
    resolved_repo_root = repo_root or config_path.resolve().parents[3]
    engine = MonitorEngine(
        config_path=config_path,
        state_path=state_path,
        repo_root=resolved_repo_root,
        database_filter=database_filter or os.environ.get("DATABASE_FILTER") or None,
    )

    report = engine.run(dry_run=dry_run or os.environ.get("DRY_RUN", "").lower() == "true")
    notify_version_updates = report.pop("notify_version_updates")
    notify_feature_updates = report.pop("notify_feature_updates")
    errors = report.get("errors", [])

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    slack_payload = build_slack_payload(
        notify_version_updates,
        notify_feature_updates,
        errors=errors,
        run_url=os.environ.get("GITHUB_RUN_URL"),
        branch_name=os.environ.get("GITHUB_REF_NAME"),
    )
    write_slack_payload(output_dir / "slack-payload.json", slack_payload)
    (output_dir / "notify-slack").write_text(
        "true" if slack_payload.get("blocks") else "false",
        encoding="utf-8",
    )

    logging.info(
        "Run complete: status=%s, new_versions=%s, new_features=%s, errors=%s",
        report["last_run_status"],
        report["new_version_updates"],
        report["new_feature_updates"],
        len(errors),
    )

    if report["last_run_status"] == "FAILURE":
        sys.exit(1)


if __name__ == "__main__":
    main()
