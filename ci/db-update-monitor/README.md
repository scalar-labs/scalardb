# ScalarDB Database Update Monitor

A weekly job that watches every database ScalarDB supports and posts to Slack when there is something new:

- **New versions** of what ScalarDB depends on: JDBC drivers, SDKs, connectors, and database server images.
- **Feature or API announcements** from the vendors that may affect a ScalarDB adapter, such as new capabilities, deprecations, or breaking changes.

**Why:** each vendor ships on its own schedule, and it is easy to miss changes. For example, Cosmos DB added hierarchical (v2) partition keys after the ScalarDB adapter was built on v1, and the team noticed late. This monitor makes sure such news reaches the team.

The monitor only reports. It does not upgrade dependencies, change code, or open tickets.

## Documentation

| Document | Read it to learn |
|----------|------------------|
| This README | How it runs, how to add a database, how to run it locally |
| [SPEC.md](./SPEC.md) | What the monitor must do, and the full configuration reference |
| [HOW-IT-WORKS.md](./HOW-IT-WORKS.md) | How the code works, step by step, and how to trace a Slack message to its cause |

## How it runs

| | |
|---|---|
| **Workflow** | [`.github/workflows/db-update-monitor.yaml`](../../.github/workflows/db-update-monitor.yaml) |
| **Schedule** | Every Sunday 17:00 UTC (Monday 02:00 JST), on the default branch only |
| **Manual run** | GitHub → Actions → *Database Update Monitor* → *Run workflow*. Options: one database only (`database_filter`), dry run (`dry_run`) |
| **Slack** | Posted with the repository secret `SLACK_SCALARDB_WEBHOOK_URL` (Slack incoming webhook) |
| **Memory** | `state/last-run.json` records what was already sent. The workflow commits it to the default branch after each run |
| **Report** | Each run uploads `output/` as the `db-monitor-report` artifact (kept 90 days) |
| **Pull requests** | Changes under `ci/db-update-monitor/` run the unit tests only |

## When a Slack message is sent

A run sends **one** message, or none.

- **A new version is sent** when a newer version than ScalarDB's current one exists and that version was not sent before. Each component has a *granularity*: drivers and SDKs report every newer version, server images report only a new major version (e.g. PostgreSQL `18` when we test `17`, but not `17.6`).
- **A feature announcement is sent** when it is new and passes the relevance rules: it mentions something on the database's watch list, or a global keyword like "deprecated" or "breaking", and nothing on the global ignore list like "pricing" or "webinar".
- **Check failures are sent** even when there is nothing new, so broken checks get noticed.
- **Nothing is sent** in a week with no new updates and no failures.
- **The first run sends nothing.** It records everything that already exists as the starting point (the *baseline*), so the team is not flooded with old news.

The exact rules are in [SPEC.md section 4](./SPEC.md#4-functional-requirements).

## What is monitored

| Database | Versions | Feature announcements |
|----------|----------|-----------------------|
| Azure Cosmos DB | SDK | Cosmos DB blog |
| Apache Cassandra | Driver, server image | Cassandra GitHub releases |
| Amazon DynamoDB | SDK | AWS "What's New" (titles with "DynamoDB") |
| PostgreSQL | JDBC driver, server image | postgresql.org news |
| MySQL | JDBC driver (MariaDB Connector/J, as used by ScalarDB), server image | MySQL GitHub releases |
| MariaDB | JDBC driver, server image | mariadb.org blog |
| Oracle Database | JDBC driver | New-features guide page |
| SQL Server | JDBC driver | Azure SQL blog (titles with "SQL Server") |
| SQLite | JDBC driver | SQLite GitHub releases |
| YugabyteDB | JDBC driver | YugabyteDB release notes |
| IBM Db2 | JDBC driver | IBM driver GitHub releases (driver only, see below) |
| TiDB | Server | TiDB GitHub releases |
| AlloyDB | JDBC connector, AlloyDB Omni image | AlloyDB release notes |
| Cloud Spanner | JDBC driver | Spanner release notes |
| Amazon S3 | SDK | AWS "What's New" (titles with "Amazon S3") |
| Google Cloud Storage | SDK | Cloud Storage release notes |
| Azure Blob Storage | SDK | Azure blog (titles with blob storage keywords) |

The list is defined in [`config/monitors.yaml`](./config/monitors.yaml).

### Known limitations

- **Db2**: IBM blocks automated access to `ibm.com/docs`, so Db2 server announcements cannot be read. Feature monitoring uses IBM's driver repositories on GitHub instead, which cover the driver, not the server.
- **Oracle**: the new-features guide is built with JavaScript, so the monitor reads the plain table-of-contents page (`nfcoa/toc.htm`) instead.
- **Server images outside Docker Hub** (Oracle, SQL Server, Db2) are not supported, so their server versions are not monitored.
- The current code has some gaps against the specification, listed in [SPEC.md section 10](./SPEC.md#10-known-deviations-from-this-specification).

## Adding a database

Most databases need a change to [`config/monitors.yaml`](./config/monitors.yaml) only.

**1. Add an entry.** Example:

```yaml
databases:
  - id: exampledb                    # unique, lowercase; used in state and --filter
    name: ExampleDB                  # shown in Slack
    adapter_package: core/src/main/java/com/scalar/db/storage/jdbc
    watch_features:                  # words that make an announcement relevant
      - jdbc
      - deprecated
      - breaking

    components:                      # version checks
      - id: jdbc-driver
        type: jdbc-driver            # sdk | jdbc-driver | server | connector | driver
        current_version:             # where ScalarDB's version is written
          source: gradle
          file: build.gradle
          property: exampleDriverVersion
        upstream:                    # where new versions are published
          type: maven
          artifact: com.example:example-jdbc   # the same artifact ScalarDB depends on

      - id: server
        type: server
        version_granularity: major   # report new major versions only
        current_version:
          source: tests-config       # highest version tested in ci/tests-config.yaml
          filter: exampledb
        upstream:
          type: docker
          image: exampledb
          tag_pattern: '^\d+(\.\d+)?$'   # plain version tags only
          exclude_tags: [latest]

    sources:                         # feature announcements
      - type: rss
        url: https://example.com/blog/feed/
```

**2. Choose the types.**

| To check | Use | Options |
|----------|-----|---------|
| ScalarDB's current version | `current_version.source` | `gradle` (a property in a Gradle file), `tests-config` (versions in `ci/tests-config.yaml`) |
| New versions | `upstream.type` | `maven`, `docker` (Docker Hub only), `github_releases` |
| Announcements | `sources[].type` | `rss` (RSS or Atom feed), `github_releases`, `webpage` (plain HTML page) |

**3. For feeds that cover many products** (e.g. AWS or Azure blogs), always set `keywords` and `match_scope: title`. Otherwise every announcement on the feed is considered, and articles that mention your product in passing match too:

```yaml
      - type: rss
        url: https://aws.amazon.com/about-aws/whats-new/recent/feed/
        keywords: ["Amazon DynamoDB", "DynamoDB"]
        match_scope: title
```

**4. Test it locally** with a dry run for just that database (see below), and check `output/report.json`:

- `errors` is empty.
- `detected_versions` shows the right current version.
- `detected_features` shows sensible items, and `relevance_reason` explains each decision.

**5. Open a pull request.** The new database is baselined silently on its first weekly run. New announcements from it are sent after that.

Every field is described in [SPEC.md section 6](./SPEC.md#6-configuration-reference). To pause a database without deleting it, set `enabled: false`.

**New kind of source?** If the vendor publishes in a form not listed above, add a plugin class in [`src/plugins/`](./src/plugins/) and register it in [`src/plugins/__init__.py`](./src/plugins/__init__.py).

## Running locally

Requires Python 3.12.

```bash
cd ci/db-update-monitor
pip install -r requirements.txt
```

Dry run for one database. This calls the real upstream services but does **not** change `state/last-run.json`:

```bash
python src/main.py --config config/monitors.yaml --state state/last-run.json --output-dir output --repo-root ../.. --filter cosmos --dry-run --verbose
```

Remove `--filter cosmos` to check every database.

The results are in `output/`:

| File | Contents |
|------|----------|
| `report.json` | Everything found, what would be sent, and any errors |
| `slack-payload.json` | The Slack message that would be posted |
| `notify-slack` | `true` if there is something to post |

The program never posts to Slack itself. To preview the message, post `slack-payload.json` to a **test** webhook:

```bash
curl -X POST -H 'Content-Type: application/json' --data @output/slack-payload.json "$TEST_SLACK_WEBHOOK_URL"
```

Because the committed state has no baseline yet, a local run with the real state produces a message only if checks failed. To preview version updates, pass a throwaway state file containing `{"baseline_established": true}` with `--state`. Feature items still do not appear in that preview: every feed looks newly added, and new feeds are recorded silently on their first run.

**If you ran without `--dry-run`,** `state/last-run.json` was changed. Reset it before committing:

```bash
cp state/last-run.clean.json state/last-run.json
```

Never commit a state file produced by a local run. The next weekly run would treat everything in it as already sent.

**Do not use `--filter` for the first production run.** A filtered run never completes the baseline, so the next full run would also send nothing.

## Tests

Run from `ci/db-update-monitor`:

```bash
pip install -r requirements.txt
pytest
```
