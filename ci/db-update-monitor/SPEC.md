# Database Update Monitor — Specification

This document says **what the monitor must do**. For how the code does it, see [HOW-IT-WORKS.md](./HOW-IT-WORKS.md). For day-to-day use, see [README.md](./README.md).

Where the current code does not yet meet this specification, the gap is listed in [section 11](#11-known-deviations-from-this-specification).

---

## 1. Background and goal

ScalarDB supports many databases. Each vendor ships new versions and new features on its own schedule, and it is hard for the team to keep up. For example, when the Cosmos DB adapter was written, Cosmos DB only had v1 (single-path) partition keys. Hierarchical (v2) partition keys arrived later, and the team noticed late.

The original requirement:

> Design and create a system that checks the target databases regularly and shares/notifies the updates somewhere like Slack. It should be scalable, so we can add new databases to the system easily.

The goal of the monitor is therefore:

1. Check every ScalarDB-supported database **regularly** (weekly).
2. Tell the team about **new versions** and **new features or API changes** that may matter to ScalarDB.
3. Post those updates to **Slack**.
4. Make it possible to **add a database by editing configuration only**, without changing Python code, as long as the vendor publishes updates in a supported form.

## 2. Scope

### In scope

- **Version updates**: a newer release exists than the version ScalarDB currently uses ("pins"). This covers drivers, SDKs, connectors, and database server images.
- **Feature updates**: the vendor announced something (new capability, deprecation, breaking change) that may affect a ScalarDB adapter.
- A weekly Slack digest of those updates.
- A report file for every run, so anyone can see what was found and why it was or was not sent.

### Out of scope

- Upgrading dependencies or opening pull requests. Dependabot does Gradle dependency bumps.
- Changing adapter code, running tests, or creating tickets.
- Real-time alerts. Weekly is intentional.
- Deciding with an AI model whether an announcement matters. Relevance uses keyword rules kept in YAML, so anyone can read and change them.

## 3. Terms

| Term | Meaning |
|------|---------|
| **Database** | One ScalarDB-supported backend, e.g. `cosmos`, `postgresql`. One entry in `monitors.yaml`. |
| **Component** | One versioned thing ScalarDB depends on for a database: a JDBC driver, an SDK, a connector, or a server image. A database can have several. |
| **Pin** | The version of a component that ScalarDB uses today, read from `build.gradle` or `ci/tests-config.yaml`. |
| **Upstream** | Where new versions of a component are published: Maven Central, Docker Hub, or GitHub Releases. |
| **Source** | A place where a vendor announces features: an RSS/Atom feed, GitHub release notes, or a web page. |
| **Relevant** | A feature announcement that matches the relevance rules and is therefore allowed to reach Slack. |
| **Baseline** | The first full run. It records everything that already exists and sends nothing, so the team is not flooded with old news. |
| **State** | The file `last-run.json`, kept on the `db-update-monitor-state` branch. It remembers what was already reported, so the same update is not sent twice. |
| **Digest** | The single Slack message a run sends. |

## 4. Inputs and outputs

This section is the overview of what goes into the monitor and what comes out. Each row links to the detailed section.

```
INPUTS                                              OUTPUTS

What to watch      config/monitors.yaml       ──┐          ┌──►  Slack message (posted by the workflow)
                   config/relevance-rules.yaml──┤          ├──►  output/report.json
ScalarDB versions  build.gradle               ──┤          ├──►  output/slack-payload.json
                   ci/tests-config.yaml       ──┼─► monitor┼──►  output/notify-slack
Vendor data        Maven, Docker Hub, GitHub, ──┤          ├──►  last-run.json (updated)
                   RSS/Atom feeds, web pages    │          └──►  exit code and log line
Memory             last-run.json (previous)   ──┤
How to run         options, environment vars  ──┘
```

In one sentence: the monitor compares **what ScalarDB uses** (pins) and **what vendors have published** with **what it already reported** (the state), and outputs **only what is new**.

### 4.1 Inputs

| Input | What it provides | Maintained by | Read or written | Details |
|-------|------------------|---------------|-----------------|---------|
| `config/monitors.yaml` | The databases to watch: their components (version checks), sources (feature checks), watch lists, and adapter capabilities | The team, by pull request | Read | [7.1](#71-configmonitorsyaml) |
| `config/relevance-rules.yaml` | Global keywords: never notify / notify | The team, by pull request | Read | [7.2](#72-configrelevance-rulesyaml) |
| `build.gradle` | The versions of the drivers, SDKs, and connectors ScalarDB uses (Gradle properties such as `postgresqlDriverVersion`) | ScalarDB's normal build; the monitor never changes it | Read | [7.1, `current_version`](#71-configmonitorsyaml) |
| `ci/tests-config.yaml` | The database server versions ScalarDB's CI tests against | ScalarDB's normal CI; the monitor never changes it | Read | [7.1, `current_version`](#71-configmonitorsyaml) |
| Vendor data | Published versions and announcements, fetched over HTTPS: `maven-metadata.xml` from Maven Central, tags from the Docker Hub API, releases from the GitHub API, RSS/Atom feeds, and web pages | The vendors | Read | [6](#6-supported-upstream-and-source-types) |
| `last-run.json` (state) | What earlier runs already reported, feed bookmarks, and the previous pins. A missing file means "first run". | The monitor itself | Read at the start, written at the end | [9](#9-state) |
| Command-line options and environment variables | Which files to use, one database only, dry run, and the run link shown in Slack | Whoever runs it: the workflow, or a developer locally | Read | [4.2](#42-command-line-options-and-environment-variables) |
| Workflow inputs and secrets | Manual-run options, the Slack webhook, the GitHub token | Repository settings | Read | [4.3](#43-github-actions-inputs-and-secrets) |

### 4.2 Command-line options and environment variables

The program is `src/main.py`. Example:

```bash
python src/main.py --config config/monitors.yaml --state state/last-run.json --output-dir output --repo-root ../.. --filter cosmos --dry-run
```

| Option | Required | Environment variable | Meaning | Value in the workflow |
|--------|----------|----------------------|---------|-----------------------|
| `--config PATH` | yes | | Path to `monitors.yaml`. `relevance-rules.yaml` is read from the same folder. | `ci/db-update-monitor/config/monitors.yaml` |
| `--state PATH` | yes | | Path to the state file. Read if it exists (missing means first run). Written at the end unless it is a dry run; its folder is created if needed. | `$RUNNER_TEMP/db-update-monitor-state/last-run.json`, a checkout of the state branch |
| `--output-dir PATH` | yes | | Folder for the three output files. Created if needed. | `ci/db-update-monitor/output` |
| `--repo-root PATH` | no | | Repository root, used to find `build.gradle` and `ci/tests-config.yaml`. Default: three folders above the folder of `--config`, which is the repository root when the configuration is in its usual place. | The checkout folder |
| `--filter ID` | no | `DATABASE_FILTER` | Check only the database with this `id`. An unknown id stops the run (exit code 2). A filtered run never completes the baseline. If both are set, the option wins. | The `database_filter` manual input (empty means all) |
| `--dry-run` | no | `DRY_RUN=true` | Do not write the state file. Output files are still written. Dry run is on if either is set. | The `dry_run` manual input |
| `--verbose` | no | | Debug-level logging. | Not set |

Environment variables that only change the content of the outputs:

| Variable | Effect | Needed locally? |
|----------|--------|-----------------|
| `GITHUB_TOKEN` | Sent with GitHub API calls (TiDB and Db2 releases), which raises GitHub's rate limit | No. Without it, GitHub allows 60 calls per hour, which is enough. |
| `GITHUB_RUN_URL` | Adds a **View run** link to the Slack message | No |
| `GITHUB_REF_NAME` | Adds the branch name to the Slack message | No |

### 4.3 GitHub Actions inputs and secrets

| Name | Kind | Meaning |
|------|------|---------|
| `database_filter` | Manual-run input (text, default empty) | Passed as `DATABASE_FILTER`. Empty means every database. |
| `dry_run` | Manual-run input (true/false, default false) | Passed as `DRY_RUN`. A dry run also skips the Slack post and the state save. |
| `SLACK_SCALARDB_WEBHOOK_URL` | Repository secret | Slack incoming webhook the message is posted to |
| `GITHUB_TOKEN` | Provided automatically by GitHub Actions | GitHub API calls, and pushing the state branch |
| `STATE_BRANCH` | Setting in the workflow file (`db-update-monitor-state`) | Branch that holds `last-run.json` |

### 4.4 Outputs

| Output | Produced by | When | Details |
|--------|-------------|------|---------|
| **Slack message** | The workflow, which posts `slack-payload.json` | Not a dry run, and `notify-slack` is `true` (there are new updates or failed checks) | FR-23 to FR-28 |
| `output/report.json` | The program | Every run, including dry runs | [8](#8-output-files) |
| `output/slack-payload.json` | The program | Every run, including dry runs. `{"blocks": []}` when there is nothing to send. | [8](#8-output-files) |
| `output/notify-slack` | The program | Every run, including dry runs. `true` or `false`. | [8](#8-output-files) |
| **Updated `last-run.json`** | Written by the program; pushed to the `db-update-monitor-state` branch by the workflow | Written unless it is a dry run. Pushed only if the run succeeded and the Slack message (if any) was delivered. | [9](#9-state) |
| `db-monitor-report` artifact | The workflow | Every run; a copy of `output/`, kept 90 days | [10](#10-operational-requirements) |
| **Exit code** | The program | Every run | `0` = `SUCCESS` or `PARTIAL_SUCCESS`; `1` = `FAILURE` or invalid configuration; `2` = unknown `--filter` id (FR-31) |
| **Log line** | The program | Every run that gets as far as the checks | `Run complete: status=SUCCESS, new_versions=11, new_features=7, errors=0` |

When the configuration is invalid or the `--filter` id is unknown, the run stops before any check, prints the problem, and writes **no** output files.

### 4.5 Example

A real run against the current configuration, with the state rewound by 30 days so there is something to report.

**Log line:**

```
INFO root: Run complete: status=SUCCESS, new_versions=11, new_features=7, errors=0
```

**`report.json`** (the summary fields, then one version update and one feature update from the lists):

```json
{
  "last_run": "2026-11-18T12:41:36Z",
  "last_run_status": "SUCCESS",
  "baseline_established": true,
  "checked_databases": 17,
  "detected_version_updates": 11,
  "detected_feature_updates": 61,
  "new_version_updates": 11,
  "new_feature_updates": 7,
  "skipped_version_updates": 0,
  "skipped_feature_updates": 54,
  "errors": []
}
```

```json
{
  "database_id": "sqlserver",
  "database_name": "Microsoft SQL Server",
  "component_id": "jdbc-driver",
  "component_type": "jdbc-driver",
  "source_type": "maven",
  "current_version": "13.6.0.jre8",
  "latest_version": "13.6.2.jre8",
  "new_versions": ["13.6.2.jre8", "13.6.1.jre8"],
  "update_level": "patch_update",
  "url": "https://search.maven.org/artifact/com.microsoft.sqlserver/mssql-jdbc/13.6.2.jre8",
  "adapter_package": "core/src/main/java/com/scalar/db/storage/jdbc",
  "dedup_key": "sqlserver:jdbc-driver:13.6.2.jre8"
}
```

```json
{
  "database_id": "dynamo",
  "database_name": "Amazon DynamoDB",
  "title": "Amazon DynamoDB caching is now available with Amazon ElastiCache Serverless for Valkey",
  "description": "Today, AWS announces the general availability of DynamoDB caching with ...",
  "source_type": "rss",
  "matched_keywords": ["Amazon DynamoDB", "DynamoDB"],
  "relevant": true,
  "relevance_reason": "matches watch_features: Amazon DynamoDB",
  "url": "https://aws.amazon.com/about-aws/whats-new/2026/11/dynamodb-caching-elasticache/",
  "adapter_capabilities": null,
  "version": null,
  "dedup_key": "dynamo:rss:https://aws.amazon.com/about-aws/whats-new/2026/11/dynamodb-caching-elasticache/"
}
```

**Slack message** (shortened; this is roughly how it looks in the channel):

> **ScalarDB Database Update Monitor — 18 new update(s)**
>
> **VERSION updates**
> ```
> Database           Component    Current    Latest     Level  Source
> ----------------------------------------------------------------------------
> Apache Cassandra   Server       5.0        6.0        major  Docker
> Amazon DynamoDB    SDK          2.58.0     2.59.3     minor  Maven
>   └ 4 new versions: 2.59.3, 2.59.2, 2.59.1, 2.59.0
> Microsoft SQL Serv JDBC Driver  13.6.0.jre8 13.6.2.jre8 patch  Maven
>   └ 2 new versions: 13.6.2.jre8, 13.6.1.jre8
> ...
> ```
> **FEATURE updates**
> • **Amazon DynamoDB** — Amazon DynamoDB caching is now available with Amazon ElastiCache Serverless for Valkey
>   Today, AWS announces the general availability of DynamoDB caching with ...
>   View announcement
> ...
>
> View run | Branch: `master`

In notifications, where the formatting is not shown, the same message appears as: *ScalarDB Database Update Monitor: 18 new update(s)*.

## 5. Functional requirements

### 5.1 Schedule and triggers

- **FR-1** The monitor runs automatically once a week: Sunday 17:00 UTC (Monday 02:00 JST).
- **FR-2** It can be started manually from GitHub Actions, optionally for one database only, and optionally as a dry run. A database id that does not match any enabled database stops the run with an error listing the valid ids.
- **FR-3** Scheduled and manual runs only work on the repository's default branch.
- **FR-4** Pull requests that change the monitor run its unit tests only. They never post to Slack or change state.

### 5.2 Version updates

- **FR-5** For every component, the monitor reads the pin and the list of upstream versions, and reports the component if a newer version exists.
- **FR-6** Pre-releases (alpha, beta, rc, preview, snapshot, milestone) are ignored unless the pin itself is a pre-release.
- **FR-7** Each component has a **granularity** that decides which newer versions count:

  | Granularity | Reports | Typical use |
  |-------------|---------|-------------|
  | `exact` | Any newer version | Drivers, SDKs |
  | `minor` | Only a newer minor or major version | SDKs that release very often |
  | `major` | Only a newer major version | Server images (pin `17` does not report `17.6`, but does report `18`) |

  Granularity is taken from the first place that sets it: the component, its `upstream`, the database (including `defaults`), and finally the default of the upstream type (`exact` for Maven and GitHub, `major` for Docker).

- **FR-7a** Versions are compared by their numbers, including vendor formats that are not standard version numbers, such as `13.6.0.jre8` or `42.7.3-yb-4` (`13.10.0.jre8` is newer than `13.6.0.jre8`; `42.7.3-yb-10` is newer than `42.7.3-yb-4`).
- **FR-7b** When a vendor publishes several builds of each release (e.g. `jre8` and `jre11`), the component can be limited to the build ScalarDB uses (`version_pattern`).
- **FR-8** One report per component, showing the pin, the newest version, the update level (major / minor / patch), and the list of newer versions.
- **FR-9** A given newest version is reported **once**. If an even newer version appears later, that is reported once too.
- **FR-10** When ScalarDB upgrades a pin and the new pin is already the newest version, nothing is reported.

### 5.3 Feature updates

- **FR-11** For every source, the monitor fetches items published since the last run.
- **FR-12** A source may list `keywords`. Then only items that match one of them are considered. A source without keywords considers every item.
- **FR-13** For shared vendor feeds that cover many products (e.g. AWS "What's New"), keyword matching can be limited to the item title (`match_scope: title`).
- **FR-14** Every considered item goes through the **relevance rules** (section 5.4). Only relevant items can reach Slack.
- **FR-15** Each relevant item is reported **once**.
- **FR-16** For a web page source, the monitor finds the parts of the page (list items, paragraphs, headings) that are new since the last run, and reports them if they match the keywords. Each new change is reported once; later changes are reported again.
- **FR-17** If the same GitHub release is both a version update and a feature item, and the feature item has no feature-specific keyword match, only the version update is reported.

### 5.4 Relevance rules

Rules are applied in this order. The first rule that matches decides.

1. **Global ignore list** (`irrelevant_keywords` in `relevance-rules.yaml`, e.g. "pricing", "webinar") → not relevant.
2. **Adapter gap**: the item mentions one of the words listed in `adapter_gap_keywords` for a capability the adapter has (e.g. `hierarchical` for `partition_key_model: v1_single_path`) → relevant, and the reason names the gap. This is the Cosmos v1 vs v2 case.
3. **Database watch list** (`watch_features` on the database) → relevant.
4. **Global watch list** (`relevant_keywords`, e.g. "deprecated", "breaking") → relevant.
5. Otherwise → not relevant.

Keyword matching ignores case. Keywords with a space or longer than 4 characters match anywhere in the text. Keywords of 4 characters or fewer (e.g. `fts`, `json`, `v2`) must match a whole word.

### 5.5 First run and new entries

- **FR-18** The first full run records all current versions and announcements as already seen and sends no Slack message.
- **FR-19** A run limited to one database (`--filter`) never completes the baseline. The first production run must be a full run.
- **FR-20** A feature source added after the baseline is baselined silently on its first run.
- **FR-21** A component added after the baseline reports its update on its first run.
- **FR-22** A database with `enabled: false` is skipped.

### 5.6 Slack notification

- **FR-23** A run sends at most one Slack message.
- **FR-24** The message contains:
  - a header with the number of new updates,
  - a version table (database, component, current, latest, level, source),
  - a feature list (database, title, short description, ScalarDB adapter context if configured, link),
  - up to five check failures, if any,
  - a link to the GitHub Actions run,
  - a short plain-text summary (e.g. *ScalarDB Database Update Monitor: 18 new update(s)*), shown in notifications and previews where the blocks are not rendered.
- **FR-25** No message is sent when there are no new updates and no failures.
- **FR-26** A message is sent when checks failed, even if there are no new updates.
- **FR-27** Dry runs never post to Slack and never save state.
- **FR-28** The message must stay within Slack's limits (3,000 characters per section, 50 blocks per message). Long content is split or shortened, never rejected.

### 5.7 Failure handling

- **FR-29** A failure in one component or source must not stop the other checks.
- **FR-30** Every failure is recorded in the report and in the Slack message, and in the state file when the state is saved (see FR-37 and FR-38).
- **FR-31** The run status is:

  | Status | When | Exit code |
  |--------|------|-----------|
  | `SUCCESS` | No check failed | 0 |
  | `PARTIAL_SUCCESS` | Some checks failed | 0 |
  | `FAILURE` | Every check failed, or there was nothing to check | 1 |
  | (usage error) | The database id given to `--filter` does not exist | 2 |
  | (configuration error) | `monitors.yaml` or `relevance-rules.yaml` is invalid | 1 |

- **FR-32** An update must not be marked as reported unless the Slack message was delivered. How the state is saved is in [section 9](#9-state).

### 5.8 Extensibility

- **FR-33** Adding a database whose updates come from a supported upstream or source type needs a change to `monitors.yaml` only.
- **FR-34** Adding a new upstream or source type needs one new plugin class and one line in the plugin registry. No other code changes.
- **FR-35** Database-specific rules (watch lists, adapter capabilities) live in configuration, not in code.
- **FR-36** A configuration mistake (unknown key, missing required key, unknown type or value, invalid regular expression, duplicate id, pin that cannot be found) is caught by the unit tests on the pull request, not on the next weekly run. The pull request tests also run when `build.gradle` or `ci/tests-config.yaml` change, since the pins are read from them.
- **FR-36a** Every run checks the configuration first. If it is invalid, the run stops before any check, lists every problem with its location (e.g. `databases[cosmos].sources[0]: unknown key 'match_scop'`), and exits with code 1.

## 6. Supported upstream and source types

### Version upstreams

| `type` | Reads | Notes |
|--------|-------|-------|
| `maven` | `maven-metadata.xml` from Maven Central (or another Maven repository) | Drivers, SDKs, connectors |
| `docker` | Tags from Docker Hub | Docker Hub only. Images in other registries (Microsoft, Oracle, IBM) are not supported. |
| `github_releases` | GitHub Releases API | Servers released on GitHub, e.g. TiDB |

### Feature sources

| `type` | Reads | Notes |
|--------|-------|-------|
| `rss` | An RSS or Atom feed | Vendor blogs, release-note feeds, GitHub `releases.atom` |
| `github_releases` | GitHub Releases API (title and release notes) | |
| `webpage` | A plain HTML page | For vendors without a feed. Pages built by JavaScript cannot be read. |

### Pin sources

| `source` | Reads |
|----------|-------|
| `gradle` | A property in a Gradle file, written as `name = 'value'` (single quotes) |
| `tests-config` | Versions listed for a label in `ci/tests-config.yaml` |

## 7. Configuration reference

### 7.1 `config/monitors.yaml`

```yaml
defaults:                      # copied onto every database entry
  first_run_policy: baseline_silent

databases:
  - id: postgresql             # required, unique, lowercase
    name: PostgreSQL           # required, shown in Slack
    enabled: true              # optional, default true
    adapter_package: core/src/main/java/com/scalar/db/storage/jdbc   # optional, for reference
    watch_features: [jdbc, deprecated, breaking]                     # optional
    adapter_capabilities:      # optional, what the adapter implements today
      partition_key_model: v1_single_path
    adapter_gap_keywords:      # optional, words that mean the vendor moved past it
      partition_key_model: [hierarchical, v2]
    components: [...]          # optional, version checks
    sources: [...]             # optional, feature checks
```

**Database fields**

| Field | Required | Meaning |
|-------|----------|---------|
| `id` | yes | Stable key. Used in state keys, `--filter`, and reports. Changing it resets that database's history. |
| `name` | yes | Label shown in Slack. |
| `enabled` | no | `false` skips the database. Default `true`. |
| `adapter_package` | no | Path of the ScalarDB adapter source. For people triaging an update. |
| `watch_features` | no | Keywords that make an announcement relevant for this database. |
| `adapter_capabilities` | no | Map of capability → value describing what the adapter implements today. Shown in Slack next to that database's feature items. |
| `adapter_gap_keywords` | no | Map of capability → list of words. An announcement containing one of the words is relevant as an adapter gap (rule 2 in 5.4). Each capability must also be in `adapter_capabilities`. |
| `version_granularity` | no | Granularity for every component of this database that does not set its own. |
| `components` | no | List of version checks. |
| `sources` | no | List of feature checks. |

**`defaults` fields**

| Field | Values | Meaning |
|-------|--------|---------|
| `first_run_policy` | `baseline_silent` (default) | First full run sends nothing. Any other value sends everything on the first run. |
| `version_granularity` | `exact`, `minor`, `major` | Optional. Applies to every component that does not set its own, **including Docker server images**, which otherwise default to `major`. Not set in the current configuration. |

**Component fields**

| Field | Required | Meaning |
|-------|----------|---------|
| `id` | yes | Unique within the database, e.g. `jdbc-driver`, `server`. |
| `type` | no | `sdk`, `jdbc-driver`, `server`, `connector`, or `driver`. Shown in Slack. Default `sdk` for Maven, `server` for Docker and GitHub. |
| `version_granularity` | no | `exact`, `minor`, or `major`. If not set: the database or `defaults` value, otherwise `exact` for Maven and GitHub and `major` for Docker. |
| `current_version` | yes | Where to read the pin (below). |
| `upstream` | yes | Where to look for new versions (below). |

**`current_version` (pin)**

| Field | Used with | Meaning |
|-------|-----------|---------|
| `source` | all | `gradle` or `tests-config`. |
| `file` | all | File path from the repository root. `gradle` needs it. `tests-config` defaults to `ci/tests-config.yaml`. |
| `property` | `gradle` | Gradle property name. |
| `path` | `tests-config` | Exact label, after removing `_%VERSION%`. E.g. `cassandra_%VERSION%` → `cassandra`. |
| `filter` | `tests-config` | Label prefix. Collects versions of every label that starts with it. Use either `path` or `filter`. |
| `strategy` | `tests-config` | Optional. The only value is `highest` (the default): use the highest version found. |

Only labels that list `versions` in `ci/tests-config.yaml` can be used.

**`upstream` — `maven`**

| Field | Required | Meaning |
|-------|----------|---------|
| `artifact` | yes | `groupId:artifactId`. Must be the same artifact ScalarDB depends on. |
| `version_pattern` | no | Regular expression; only versions containing a match are compared. E.g. `'\.jre8$'` keeps only the jre8 builds of the SQL Server driver. |
| `repository` | no | Maven repository URL. Default Maven Central. |
| `timeout_seconds` | no | Default 30. |

**`upstream` — `docker`**

| Field | Required | Meaning |
|-------|----------|---------|
| `image` | yes | `postgres` (official image) or `owner/image`. |
| `namespace` | no | Owner, if not written in `image`. |
| `tag_pattern` | no | Regular expression a tag must match, e.g. `'^\d+(\.\d+)?$'` for numeric tags only. Strongly recommended. |
| `exclude_tags` | no | Exact tags to ignore, e.g. `[latest, alpine]`. |
| `exclude_pattern` | no | Regular expression; matching tags are ignored (case-insensitive). |
| `max_pages` | no | Pages of 100 tags to read, newest first. Default 5. |
| `timeout_seconds` | no | Default 30. |

**`upstream` — `github_releases`**

| Field | Required | Meaning |
|-------|----------|---------|
| `repo` | yes | `owner/name`. |
| `tag_strip_prefix` | no | Text to remove from tags before comparing, e.g. `release-`. A leading `v` is always removed. |
| `version_pattern` | no | Regular expression; only versions (after removing the prefix) containing a match are compared. |
| `include_prerelease` | no | Default `false`. |
| `timeout_seconds` | no | Default 30. |

**Source fields**

| Field | Used with | Meaning |
|-------|-----------|---------|
| `type` | all | `rss`, `github_releases`, or `webpage`. |
| `url` | `rss`, `webpage` | Feed or page URL. |
| `repo` | `github_releases` | `owner/name`. |
| `keywords` | all | Only items matching one of these are considered. Omit to consider every item. **Required for shared vendor feeds.** For `webpage`, the database `watch_features` are added to this list. |
| `include_all` | `rss`, `github_releases` | `true` to consider every item even if `keywords` is set. |
| `match_scope` | `rss` | `full` (default): match title and summary. `title`: match title only. Use `title` for shared vendor feeds. |
| `include_prerelease` | `github_releases` | Default `false`. |
| `per_page` | `github_releases` | Number of recent releases to read. Default 20. |
| `timeout_seconds` | all | Default 30. |

Being "considered" does not mean being sent. Every considered item still goes through the relevance rules.

### 7.2 `config/relevance-rules.yaml`

```yaml
irrelevant_keywords: [pricing, webinar, ...]   # never notify
relevant_keywords: [deprecated, breaking, ...] # notify if nothing more specific matched
```

These apply to every database.

## 8. Output files

Every run writes three files to the output directory, including dry runs. The only exception is a run stopped by an invalid configuration or an unknown `--filter` id: it writes nothing. For an overview of all outputs, see [section 4.4](#44-outputs).

| File | Contents |
|------|----------|
| `report.json` | Run status, counts, errors, everything detected, and what was (or would be) sent. |
| `slack-payload.json` | The Slack message: a plain-text `text` summary and Block Kit `blocks`. `{"blocks": []}` when there is nothing to send. |
| `notify-slack` | `true` if the message has content, otherwise `false`. |

**`report.json` fields**

| Field | Meaning |
|-------|---------|
| `last_run`, `last_run_status` | Time and status of this run |
| `baseline_established` | Whether the baseline is complete after this run |
| `checked_databases` | Number of databases checked |
| `detected_version_updates`, `detected_feature_updates` | Number found by the checks |
| `new_version_updates`, `new_feature_updates` | Number sent (or that would be sent) to Slack |
| `skipped_version_updates`, `skipped_feature_updates` | Number not sent: already reported, not relevant, or baseline |
| `errors` | Failed checks: database, component or source, type, error text, time |
| `detected_versions`, `detected_features` | Full details of everything found |
| `version_updates`, `feature_updates` | Full details of what was sent |

The Python program does not post to Slack. The GitHub Actions workflow posts `slack-payload.json`.

## 9. State

The state is the file `last-run.json`. In production it is kept on its own branch, `db-update-monitor-state`, so that weekly runs share memory without committing to the protected default branch. That branch holds only `last-run.json` and a short README. Locally, the state is `state/last-run.json`, which git ignores.

- **FR-37** The state is saved only after the Slack message was delivered, or when there was nothing to send. If delivery fails, the state is not saved, and the next run sends the same updates again. An update may therefore be sent twice, but is never lost.
- **FR-38** The state is not saved when the run status is `FAILURE`, or on a dry run.
- **FR-39** Deleting the state branch resets the monitor. The next run is a silent baseline.
- **FR-40** Announcements first seen more than 365 days ago are removed from the state, so the file does not grow forever. Feed cursors stop dated items that old from coming back. A rare feed item without a date could be sent once more after a year.

| Field | Meaning |
|-------|---------|
| `baseline_established` | `false` until the first full, non-dry run |
| `components` | Per component (`database:component`): last reported newest version, last check time |
| `features` | Relevant announcements already reported or baselined |
| `seen_irrelevant` | Announcements judged not relevant |
| `source_cursors` | Per source: time of the newest item seen. For a web page: a hash of the page plus a short hash of each part, used to find new parts |
| `pin_snapshot` | Pin of each component at the last run |
| `errors` | Failures of the last run |
| `last_run`, `last_run_status` | Time and status of the last run |

An announcement is identified by database, source type, and URL (or title and date when there is no URL). A web page change is identified by the URL plus a hash of that version of the page.

To reset the local state, delete `state/last-run.json`. A missing file means empty state.

## 10. Operational requirements

| Item | Requirement |
|------|-------------|
| Runtime | Python 3.12 on GitHub Actions (`ubuntu-latest`). Python 3.10 or newer works locally. |
| Secrets | `SLACK_SCALARDB_WEBHOOK_URL`: Slack incoming webhook. `GITHUB_TOKEN`: provided by Actions, used for the GitHub API. |
| Permissions | The monitor job needs `contents: write` to push the `db-update-monitor-state` branch. The test job only needs `contents: read`. |
| Branch rules | The default branch is protected, so the monitor never writes to it. The `db-update-monitor-state` branch must **not** be covered by branch protection or rulesets, or the state cannot be saved. |
| Concurrency | Only one monitor run at a time, so two runs never write state together. |
| Report retention | `output/` is uploaded as the `db-monitor-report` artifact and kept 90 days. |
| Network | Outgoing HTTPS to Maven Central, Docker Hub, the GitHub API, and each feed or page in `monitors.yaml`. |

## 11. Known deviations from this specification

This section lists places where the code does not meet the specification. Each one should be fixed, or the specification changed.

There are currently no known deviations. All twelve found in the first review (D-1 to D-12) have been fixed. New ones will be numbered from D-13.

Coverage gaps that are not deviations, but worth knowing:

- Server versions of Oracle, SQL Server, Db2, and YugabyteDB are not monitored, because their images are not on Docker Hub or no server component is configured.
- Db2 feature monitoring only covers IBM's driver repositories, because IBM blocks automated access to its documentation site.

Known sources of noise. These meet the specification but may send more than the team wants. They are best tuned after a few weekly messages show which ones matter:

- **Frequent SDK releases.** With `exact` granularity, the AWS SDK (released almost daily) appears nearly every week. Dependabot already raises pull requests for these.
- **Shared versions reported twice.** DynamoDB and S3 share the AWS SDK version, and MySQL and MariaDB share the MariaDB driver, so each update appears in two rows.
- **Watch lists that match almost everything.** The S3 and DynamoDB watch lists contain the product name, so every announcement on their feeds is relevant. Cassandra's `partition` matches most release notes. TiDB's `tidb` makes releases on older TiDB release lines (e.g. v7.5.x) appear as feature items.
- **Uninformative titles.** Google Cloud release-note feeds (AlloyDB, Spanner, Cloud Storage) use the date as the item title; the content is only in the description.
