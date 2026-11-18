# Database Update Monitor — Specification

This document says **what the monitor must do**. For how the code does it, see [HOW-IT-WORKS.md](./HOW-IT-WORKS.md). For day-to-day use, see [README.md](./README.md).

Where the current code does not yet meet this specification, the gap is listed in [section 10](#10-known-deviations-from-this-specification).

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
| **State** | The file `state/last-run.json`. It remembers what was already reported, so the same update is not sent twice. |
| **Digest** | The single Slack message a run sends. |

## 4. Functional requirements

### 4.1 Schedule and triggers

- **FR-1** The monitor runs automatically once a week: Sunday 17:00 UTC (Monday 02:00 JST).
- **FR-2** It can be started manually from GitHub Actions, optionally for one database only, and optionally as a dry run.
- **FR-3** Scheduled and manual runs only work on the repository's default branch.
- **FR-4** Pull requests that change the monitor run its unit tests only. They never post to Slack or change state.

### 4.2 Version updates

- **FR-5** For every component, the monitor reads the pin and the list of upstream versions, and reports the component if a newer version exists.
- **FR-6** Pre-releases (alpha, beta, rc, preview, snapshot, milestone) are ignored unless the pin itself is a pre-release.
- **FR-7** Each component has a **granularity** that decides which newer versions count:

  | Granularity | Reports | Typical use |
  |-------------|---------|-------------|
  | `exact` | Any newer version | Drivers, SDKs |
  | `minor` | Only a newer minor or major version | SDKs that release very often |
  | `major` | Only a newer major version | Server images (pin `17` does not report `17.6`, but does report `18`) |

- **FR-8** One report per component, showing the pin, the newest version, the update level (major / minor / patch), and the list of newer versions.
- **FR-9** A given newest version is reported **once**. If an even newer version appears later, that is reported once too.
- **FR-10** When ScalarDB upgrades a pin and the new pin is already the newest version, nothing is reported.

### 4.3 Feature updates

- **FR-11** For every source, the monitor fetches items published since the last run.
- **FR-12** A source may list `keywords`. Then only items that match one of them are considered. A source without keywords considers every item.
- **FR-13** For shared vendor feeds that cover many products (e.g. AWS "What's New"), keyword matching can be limited to the item title (`match_scope: title`).
- **FR-14** Every considered item goes through the **relevance rules** (section 4.4). Only relevant items can reach Slack.
- **FR-15** Each relevant item is reported **once**.
- **FR-16** For a web page source, a change to the page is reported when the page matches the configured keywords. Each new change can be reported again.
- **FR-17** If the same GitHub release is both a version update and a feature item, and the feature item has no feature-specific keyword match, only the version update is reported.

### 4.4 Relevance rules

Rules are applied in this order. The first rule that matches decides.

1. **Global ignore list** (`irrelevant_keywords` in `relevance-rules.yaml`, e.g. "pricing", "webinar") → not relevant.
2. **Adapter gap**: the database's `adapter_capabilities` record a legacy model (e.g. `partition_key_model: v1_single_path`) and the item mentions a watched feature or a newer model → relevant. This is the Cosmos v1 vs v2 case.
3. **Database watch list** (`watch_features` on the database) → relevant.
4. **Global watch list** (`relevant_keywords`, e.g. "deprecated", "breaking") → relevant.
5. Otherwise → not relevant.

Keyword matching ignores case. Keywords with a space or longer than 4 characters match anywhere in the text. Keywords of 4 characters or fewer (e.g. `fts`, `json`, `v2`) must match a whole word.

### 4.5 First run and new entries

- **FR-18** The first full run records all current versions and announcements as already seen and sends no Slack message.
- **FR-19** A run limited to one database (`--filter`) never completes the baseline. The first production run must be a full run.
- **FR-20** A feature source added after the baseline is baselined silently on its first run.
- **FR-21** A component added after the baseline reports its update on its first run.
- **FR-22** A database with `enabled: false` is skipped.

### 4.6 Slack notification

- **FR-23** A run sends at most one Slack message.
- **FR-24** The message contains:
  - a header with the number of new updates,
  - a version table (database, component, current, latest, level, source),
  - a feature list (database, title, short description, ScalarDB adapter context if configured, link),
  - up to five check failures, if any,
  - a link to the GitHub Actions run.
- **FR-25** No message is sent when there are no new updates and no failures.
- **FR-26** A message is sent when checks failed, even if there are no new updates.
- **FR-27** Dry runs never post to Slack.
- **FR-28** The message must stay within Slack's limits (3,000 characters per section, 50 blocks per message). Long content is split or shortened, never rejected.

### 4.7 Failure handling

- **FR-29** A failure in one component or source must not stop the other checks.
- **FR-30** Every failure is recorded in the report, in the state file, and in the Slack message.
- **FR-31** The run status is:

  | Status | When | Exit code |
  |--------|------|-----------|
  | `SUCCESS` | No check failed | 0 |
  | `PARTIAL_SUCCESS` | Some checks failed | 0 |
  | `FAILURE` | Every check failed, or there was nothing to check | 1 |

- **FR-32** An update must not be marked as reported unless the Slack message was delivered.

### 4.8 Extensibility

- **FR-33** Adding a database whose updates come from a supported upstream or source type needs a change to `monitors.yaml` only.
- **FR-34** Adding a new upstream or source type needs one new plugin class and one line in the plugin registry. No other code changes.
- **FR-35** Database-specific rules (watch lists, adapter capabilities) live in configuration, not in code.
- **FR-36** A configuration mistake (unknown key, missing required key, unknown type, pin that cannot be found) should be caught by the unit tests on the pull request, not on the next weekly run.

## 5. Supported upstream and source types

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

## 6. Configuration reference

### 6.1 `config/monitors.yaml`

```yaml
defaults:                      # copied onto every database entry
  first_run_policy: baseline_silent

databases:
  - id: postgresql             # required, unique, lowercase
    name: PostgreSQL           # required, shown in Slack
    enabled: true              # optional, default true
    adapter_package: core/src/main/java/com/scalar/db/storage/jdbc   # optional, for reference
    watch_features: [jdbc, deprecated, breaking]                     # optional
    adapter_capabilities:      # optional, only when an adapter uses a legacy model
      partition_key_model: v1_single_path
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
| `adapter_capabilities` | no | Map of capability → value describing what the adapter implements today. A value containing `v1`, `single_path`, `single-path`, or `legacy` marks it as legacy (see rule 2 in 4.4). |
| `components` | no | List of version checks. |
| `sources` | no | List of feature checks. |

**`defaults` fields**

| Field | Values | Meaning |
|-------|--------|---------|
| `first_run_policy` | `baseline_silent` (default) | First full run sends nothing. Any other value sends everything on the first run. |

**Component fields**

| Field | Required | Meaning |
|-------|----------|---------|
| `id` | yes | Unique within the database, e.g. `jdbc-driver`, `server`. |
| `type` | no | `sdk`, `jdbc-driver`, `server`, `connector`, or `driver`. Shown in Slack. Default `sdk` for Maven, `server` for Docker and GitHub. |
| `version_granularity` | no | `exact`, `minor`, or `major`. Default `exact` for Maven and GitHub, `major` for Docker. |
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
| `strategy` | `tests-config` | `highest` (default): use the highest version found. |

Only labels that list `versions` in `ci/tests-config.yaml` can be used.

**`upstream` — `maven`**

| Field | Required | Meaning |
|-------|----------|---------|
| `artifact` | yes | `groupId:artifactId`. Must be the same artifact ScalarDB depends on. |
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

### 6.2 `config/relevance-rules.yaml`

```yaml
irrelevant_keywords: [pricing, webinar, ...]   # never notify
relevant_keywords: [deprecated, breaking, ...] # notify if nothing more specific matched
```

These apply to every database.

## 7. Outputs

Every run writes three files to the output directory, including dry runs.

| File | Contents |
|------|----------|
| `report.json` | Run status, counts, errors, everything detected, and what was (or would be) sent. |
| `slack-payload.json` | The Slack message (Block Kit). `{"blocks": []}` when there is nothing to send. |
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

## 8. State

`state/last-run.json` is committed to the repository so that weekly runs share memory.

| Field | Meaning |
|-------|---------|
| `baseline_established` | `false` until the first full, non-dry run |
| `components` | Per component (`database:component`): last reported newest version, last check time |
| `features` | Relevant announcements already reported or baselined |
| `seen_irrelevant` | Announcements judged not relevant |
| `source_cursors` | Per source: time of the newest item seen, or a hash of the web page |
| `pin_snapshot` | Pin of each component at the last run |
| `errors` | Failures of the last run |
| `last_run`, `last_run_status` | Time and status of the last run |

An announcement is identified by database, source type, and URL (or title and date when there is no URL).

`state/last-run.clean.json` is an empty state file. Copy it over `last-run.json` to reset the state after local testing. Never commit a state file produced by local tests.

## 9. Operational requirements

| Item | Requirement |
|------|-------------|
| Runtime | Python 3.12 on GitHub Actions (`ubuntu-latest`) |
| Secrets | `SLACK_SCALARDB_WEBHOOK_URL`: Slack incoming webhook. `GITHUB_TOKEN`: provided by Actions, used for the GitHub API. |
| Permissions | The workflow must be able to save `state/last-run.json` after each run. |
| Concurrency | Only one monitor run at a time, so two runs never write state together. |
| Report retention | `output/` is uploaded as the `db-monitor-report` artifact and kept 90 days. |
| Network | Outgoing HTTPS to Maven Central, Docker Hub, the GitHub API, and each feed or page in `monitors.yaml`. |

## 10. Known deviations from this specification

The current implementation does not yet meet the specification in these places. Each one should be fixed or the specification changed.

| # | Requirement | Current behavior |
|---|-------------|------------------|
| D-1 | FR-5, FR-9 | Versions that are not standard version numbers fall back to text comparison, which orders them wrongly. Affects SQL Server (`13.6.0.jre8`: `13.10.0` is treated as older than `13.6.0`, and jre8 and jre11 builds are both reported) and YugabyteDB (`42.7.3-yb-4`). |
| D-2 | FR-5 | The Cassandra driver is checked against `com.datastax.cassandra:cassandra-driver-core`, but ScalarDB uses `org.apache.cassandra:cassandra-driver-core`, so new versions are never found. |
| D-3 | FR-16 | A web page source reports only once in its lifetime: every change gets the same identity (the URL), so later changes are treated as already seen. It also matches keywords against the whole page, not the change. Affects Oracle. |
| D-4 | FR-28 | The failures section is not shortened and can exceed 3,000 characters, and the number of blocks is not capped at 50. Slack then rejects the whole message. |
| D-5 | FR-32 | The workflow saves state before posting to Slack. If the post fails, those updates are never reported. |
| D-6 | Section 9 | The workflow pushes state directly to the default branch. If that branch is protected, the push fails and Slack is skipped. |
| D-7 | Section 9 | The pull request test job shares the monitor's concurrency group, so test runs of different pull requests can cancel each other. |
| D-8 | FR-7 | `version_granularity` set under `defaults` has no effect. Only the component or upstream setting is used. |
| D-9 | FR-35 | The adapter-gap rule also matches the words `hierarchical` and `v2`, which are hard-coded for the Cosmos case. |
| D-10 | FR-24 | RSS descriptions are sent with their HTML tags, and titles are not escaped for Slack (`&`, `<`, `>`). |
| D-11 | FR-36 | There is no check of `monitors.yaml`. Misspelled keys are ignored, and missing pins are only found on the weekly run. |
| D-12 | FR-31 | A `--filter` value that matches no database gives `SUCCESS` with nothing checked. |

Coverage gaps that are not deviations, but worth knowing:

- Server versions of Oracle, SQL Server, Db2, and YugabyteDB are not monitored, because their images are not on Docker Hub or no server component is configured.
- Db2 feature monitoring only covers IBM's driver repositories, because IBM blocks automated access to its documentation site.
