# ScalarDB Database Update Monitor

A weekly job that watches every database ScalarDB supports and posts to Slack when there is something new:

- **New versions** of what ScalarDB depends on: JDBC drivers, SDKs, connectors, and database server images.
- **Feature or API announcements** from the vendors that may affect a ScalarDB adapter, such as new capabilities, deprecations, or breaking changes.

**Why:** each vendor ships on its own schedule, and it is easy to miss changes. For example, Cosmos DB added hierarchical (v2) partition keys after the ScalarDB adapter was built on v1, and the team noticed late. This monitor makes sure such news reaches the team.

The monitor only reports. It does not upgrade dependencies, change code, or open tickets.

## Inputs and outputs

The monitor compares **what ScalarDB uses** and **what vendors have published** with **what it already reported**, and outputs **only what is new**.

**Inputs**

| Input | What it provides |
|-------|------------------|
| [`config/monitors.yaml`](./config/monitors.yaml) | The databases to watch: which versions to check, which feeds to read, and which words make an announcement relevant |
| [`config/relevance-rules.yaml`](./config/relevance-rules.yaml) | Global words that always or never make an announcement relevant ("deprecated", "pricing", ...) |
| `build.gradle` and `ci/tests-config.yaml` | The versions ScalarDB uses today: drivers and SDKs from `build.gradle`, database servers from the CI test matrix. Read only. |
| Vendor data | Published versions and announcements from Maven Central, Docker Hub, GitHub, RSS/Atom feeds, and web pages |
| `last-run.json` | What earlier runs already reported. Kept on the `db-update-monitor-state` branch. |
| Options | Which files to use, one database only, dry run. See [Running locally](#running-locally). |

**Outputs**

| Output | What it is |
|--------|------------|
| **Slack message** | One message per run, only when there are new updates or failed checks: a table of new versions, a list of relevant announcements, and any failed checks |
| `output/report.json` | Everything that was found, what was sent, why, and any errors |
| `output/slack-payload.json` | The Slack message as JSON (the workflow posts this file) |
| `output/notify-slack` | `true` if there is something to send |
| Updated `last-run.json` | So next week's run does not repeat this week's updates |
| Exit code | `0` success (also when some checks failed), `1` every check failed or the configuration is invalid, `2` unknown database id |

The full description, with every command-line option, environment variable, and a real example of each output, is in [SPEC.md section 4](./SPEC.md#4-inputs-and-outputs).

## Documentation

| Document | Read it to learn |
|----------|------------------|
| This README | What goes in and comes out, how it runs, how to add a database, how to run it locally |
| [SPEC.md](./SPEC.md) | What the monitor must do, every input and output in detail, and the full configuration reference |
| [HOW-IT-WORKS.md](./HOW-IT-WORKS.md) | How the code works, step by step, and how to trace a Slack message to its cause |

## How it runs

| | |
|---|---|
| **Workflows** | [`db-update-monitor.yaml`](../../.github/workflows/db-update-monitor.yaml): the weekly monitor (schedule and manual runs only). [`db-update-monitor-tests.yaml`](../../.github/workflows/db-update-monitor-tests.yaml): the unit tests, on pull requests. Both are in `.github/workflows/`. |
| **Schedule** | Every Sunday 17:00 UTC (Monday 02:00 JST), on the default branch only |
| **Manual run** | GitHub → Actions → *Database Update Monitor* → *Run workflow*. Options: one database only (`database_filter`), dry run (`dry_run`) |
| **Slack** | Posted with the repository secret `SLACK_SCALARDB_WEBHOOK_URL` (Slack incoming webhook) |
| **Memory** | `last-run.json` records what was already sent. It lives on the branch `db-update-monitor-state`, because the default branch is protected. The workflow pushes it there at the end of each run, but only once the Slack message (if there was one) was delivered. Details: [HOW-IT-WORKS.md section 7](./HOW-IT-WORKS.md#7-the-state-file) |
| **Report** | Each run uploads `output/` as the `db-monitor-report` artifact (kept 90 days) |
| **Pull requests** | Pull requests that change `ci/db-update-monitor/`, either workflow, `build.gradle`, or `ci/tests-config.yaml` show one check, **Database Update Monitor Tests / Unit tests**. It runs the unit tests only, including a check of the configuration and of every version the monitor reads from those two files. The monitor itself never runs on pull requests. |

## When a Slack message is sent

A run sends **one** message, or none.

- **A new version is sent** when a newer version than ScalarDB's current one exists and that version was not sent before. Each component has a *granularity*: drivers and SDKs report every newer version, server images report only a new major version (e.g. PostgreSQL `18` when we test `17`, but not `17.6`).
- **A feature announcement is sent** when it is new and passes the relevance rules: it mentions something on the database's watch list, or a global keyword like "deprecated" or "breaking", and nothing on the global ignore list like "pricing" or "webinar".
- **Check failures are sent** even when there is nothing new, so broken checks get noticed.
- **Nothing is sent** in a week with no new updates and no failures.
- **The first run sends nothing.** It records everything that already exists as the starting point (the *baseline*), so the team is not flooded with old news.

The exact rules are in [SPEC.md section 5](./SPEC.md#5-functional-requirements).

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
- Known sources of noisy messages (for example the AWS SDK appearing almost every week) are listed in [SPEC.md section 11](./SPEC.md#11-known-deviations-from-this-specification).

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

**If the vendor publishes several builds of every release** (for example `13.6.0.jre8` and `13.6.0.jre11` for the SQL Server driver), add `version_pattern` to the upstream so only the build ScalarDB uses is compared:

```yaml
        upstream:
          type: maven
          artifact: com.microsoft.sqlserver:mssql-jdbc
          version_pattern: '\.jre8$'
```

**4. Run the unit tests** (see [Tests](#tests)). They check the whole configuration: misspelled or missing keys, unknown types, invalid patterns, duplicate ids, and that every `current_version` can be read from `build.gradle` or `ci/tests-config.yaml`. Each problem is reported with its location, for example `databases[exampledb].sources[0]: unknown key 'match_scop'`.

**5. Test it locally** with a dry run for just that database (see below), and check `output/report.json`:

- `errors` is empty.
- `detected_versions` shows the right current version.
- `detected_features` shows sensible items, and `relevance_reason` explains each decision.

**6. Open a pull request.** On the first weekly run after it is merged:

- Its **feeds** are recorded silently, so old announcements are not sent. New announcements are sent from the next run on.
- Its **version checks** report straight away if ScalarDB is already behind upstream.

Every field is described in [SPEC.md section 7](./SPEC.md#7-configuration-reference). To pause a database without deleting it, set `enabled: false`.

**New kind of source?** If the vendor publishes in a form not listed above, add a plugin class in [`src/plugins/`](./src/plugins/) and register it in [`src/plugins/__init__.py`](./src/plugins/__init__.py).

## Running locally

Requires Python 3.10 or newer. GitHub Actions uses 3.12.

```bash
cd ci/db-update-monitor
pip install -r requirements.txt
```

Dry run for one database. This calls the real upstream services but does **not** write the state file:

```bash
python src/main.py --config config/monitors.yaml --state state/last-run.json --output-dir output --repo-root ../.. --filter cosmos --dry-run --verbose
```

Remove `--filter cosmos` to check every database.

| Option | Required | Meaning |
|--------|----------|---------|
| `--config` | yes | Path to `monitors.yaml`. `relevance-rules.yaml` is read from the same folder. |
| `--state` | yes | Path to the state file. Missing means first run. Written at the end unless `--dry-run`. |
| `--output-dir` | yes | Folder for the output files |
| `--repo-root` | no | Repository root, where `build.gradle` and `ci/tests-config.yaml` are. Default: found from the `--config` path. |
| `--filter ID` | no | Check one database only (also `DATABASE_FILTER`) |
| `--dry-run` | no | Do not write the state file (also `DRY_RUN=true`) |
| `--verbose` | no | Debug logging |

`python src/main.py --help` prints the same list. All options and environment variables are described in [SPEC.md section 4.2](./SPEC.md#42-command-line-options-and-environment-variables).

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

**About the local state file.** Locally, the state is `state/last-run.json`. It does not exist until you run without `--dry-run`, and git ignores it, so it can never be committed by accident. It is separate from the production state on the `db-update-monitor-state` branch. To reset it, delete it.

With no local state, a run is a baseline run, so the message contains only check failures, if any. To preview version updates, pass a throwaway state file containing `{"baseline_established": true}` with `--state`. Feature items still do not appear in that preview: every feed looks newly added, and new feeds are recorded silently on their first run.

**Do not use `--filter` for the first production run.** A filtered run never completes the baseline, so the next full run would also send nothing.

## Production state

To see what the monitor has already sent:

```bash
git fetch origin db-update-monitor-state
git show origin/db-update-monitor-state:last-run.json
```

To start over (for example after a large configuration change), delete the `db-update-monitor-state` branch. The next run is then a silent baseline run that recreates it.

The `db-update-monitor-state` branch must not be covered by branch protection or rulesets. Otherwise the workflow cannot save the state, the job fails, and the same updates are sent again every week.

## Tests

Run from `ci/db-update-monitor`:

```bash
pip install -r requirements.txt
pytest
```
