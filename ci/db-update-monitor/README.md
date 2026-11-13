# ScalarDB Database Update Monitor

Monitors upstream database-related **version updates** (drivers, SDKs, server images) and **feature/API announcements** for all ScalarDB-supported backends. Notifies Slack when new, relevant updates are detected.

## Features

- Version drift detection (Maven, Docker Hub, GitHub Releases)
- Feature announcement monitoring (RSS feeds, GitHub release notes)
- Rules-based relevance filtering (`watch_features`, `relevance-rules.yaml`)
- Component-based monitoring (e.g., PostgreSQL JDBC driver vs server)
- Config-driven pin resolution — add databases via YAML only
- First-run baseline (no historical alert flood)
- Deduplication via state file for versions and features
- Partial failure reporting (`SUCCESS` / `PARTIAL_SUCCESS` / `FAILURE`)

## Quick start

```bash
cd ci/db-update-monitor
pip install -r requirements.txt

python src/main.py \
  --config config/monitors.yaml \
  --state state/last-run.json \
  --output-dir output \
  --repo-root ../.. \
  --dry-run \
  --verbose
```

Local runs write to `state/last-run.json`. After testing, restore a first-run baseline with:

```bash
cp state/last-run.clean.json state/last-run.json
```

The next real run then establishes a new baseline (`baseline_established: false`) and does not send a historical Slack flood. Do not copy a populated test `last-run.json` into CI or the committed state file.

`--filter` is for validating one database. Do not use it for the first production run: `baseline_established` is only set after an unfiltered run, so a filtered first run would leave the next full run silent as well.

## Configuration

- `config/monitors.yaml` — database registry with `components`, optional `sources` and `watch_features`
- `config/relevance-rules.yaml` — global keyword rules for feature relevance

Each component defines:

- `current_version` — where ScalarDB pins the version (`gradle` or `tests-config`)
- `upstream` — where to check for version updates (`maven`, `docker`, `github_releases`)

Each optional `sources` entry checks for feature updates (`rss`, `github_releases`, `webpage`).

Set `match_scope: title` on a source when it is a shared vendor feed covering many
products (for example the AWS "what's new" firehose). Article bodies there routinely
name unrelated services, so matching the full text produces heavy false positives.

### Known source limitations

- **Db2** — IBM blocks automated access to `ibm.com/docs`, so the Db2 server "what's
  new" page cannot be fetched. Feature monitoring falls back to the `ibmdb` driver
  repositories, which cover the shared CLI driver layer rather than server features.
  Version monitoring of the JCC JDBC driver via Maven is unaffected.
- **Oracle** — the new-features guide renders its content through JavaScript, so the
  monitor reads `nfcoa/toc.htm`, which lists every feature by name as plain text.

## Documentation

- [Project plan](../../docs/db-update-monitor-plan.md)
- [Design document](../../docs/db-update-monitor-design.md)
- [Alternatives](../../docs/db-update-monitor-alternatives.md)

## Tests

```bash
pip install -r requirements.txt
pytest
```
