# How the Database Update Monitor works

This document explains how the code works today: what happens on each run, how it decides what to send to Slack, and how to trace a Slack message back to its cause.

- For **what the monitor must do** (requirements and full configuration reference), see [SPEC.md](./SPEC.md).
- For **setup and everyday use**, see [README.md](./README.md).

If the code ever stops matching the specification, the gap is listed in [SPEC.md section 11](./SPEC.md#11-known-deviations-from-this-specification). There are none at the moment.

---

## 1. The big picture

Each week the monitor answers two questions for every ScalarDB-supported database:

1. **Is there a newer version** of something ScalarDB depends on (driver, SDK, connector, server image)?
2. **Did the vendor announce something** (new feature, deprecation, breaking change) that might affect a ScalarDB adapter?

It then sends one Slack message with anything new. It only reports. People decide what to do.

```
GitHub Actions (Sunday 17:00 UTC)  or  python src/main.py on your machine
        |
        v
1. Load monitors.yaml, relevance-rules.yaml, last-run.json (the state)
        |
        v
2. For each database:
     Version checks                     Feature checks
     - read ScalarDB's pin              - fetch new feed items / releases / page
     - list upstream versions           - keep items matching source keywords
     - keep versions newer than pin     - apply relevance rules
        |
        v
3. Drop GitHub "feature" items that only repeat a version update
4. Compare with state: what is new, what was already sent
5. Write output/report.json, output/slack-payload.json, output/notify-slack
6. Write the updated last-run.json (skipped on dry run)
        |
        v
7. GitHub Actions posts the Slack message, then (if that worked)
   pushes last-run.json to the db-update-monitor-state branch
```

Two points that clear up most confusion:

- **Detecting is not the same as notifying.** A run can detect many updates and send none, because they were already sent, are not relevant, or this is the baseline run. `report.json` shows both.
- **The Python program never talks to Slack.** It only writes `slack-payload.json`. The GitHub Actions workflow posts it.

---

## 2. Where the files are

```
ci/db-update-monitor/
  config/monitors.yaml           What to watch: databases, pins, upstreams, feeds
  config/relevance-rules.yaml    Global "never notify" / "notify" keywords
  src/main.py                    Command-line entry point; writes output files
  src/config_loader.py           Loads monitors.yaml and applies defaults
  src/config_schema.py           Checks monitors.yaml and relevance-rules.yaml for mistakes
  src/engine.py                  Runs all checks and puts the results together
  src/pin_reader.py              Reads ScalarDB's current versions (pins)
  src/version_normalizer.py      Parses and compares versions, applies granularity
  src/relevance.py               Decides whether an announcement is relevant
  src/diff.py                    Baseline and "was this already sent?" logic
  src/state.py                   Loads and saves last-run.json
  src/text_util.py               Keyword matching and other text helpers
  src/notifier/slack.py          Builds the Slack message
  src/plugins/                   One class per upstream or source type
  tests/                         Unit tests (pytest); tests/test_config.py checks the real configuration
  state/last-run.json            Local state only; ignored by git, created on first local run
  output/                        Output files of a run; ignored by git

.github/workflows/db-update-monitor.yaml   The weekly job and the PR test job

Branch db-update-monitor-state            Production state (last-run.json), written by the workflow
```

In GitHub Actions, `output/` is uploaded as the `db-monitor-report` artifact.

---

## 3. Configuration in short

The full field reference is in [SPEC.md section 7](./SPEC.md#7-configuration-reference). This section explains the ideas.

A database entry in `monitors.yaml` has two independent parts:

```yaml
- id: postgresql
  name: PostgreSQL
  watch_features: [jdbc, deprecated, breaking]

  components:                  # version checks
    - id: jdbc-driver
      type: jdbc-driver
      current_version:         # the pin: what ScalarDB uses today
        source: gradle
        file: build.gradle
        property: postgresqlDriverVersion
      upstream:                # where new versions appear
        type: maven
        artifact: org.postgresql:postgresql

  sources:                     # feature checks
    - type: rss
      url: https://www.postgresql.org/news.rss
```

### 3.1 Pins (`current_version`)

- **`gradle`** reads a line like `postgresqlDriverVersion = '42.7.14'` from the given file. Only single quotes are recognized.
- **`tests-config`** reads `ci/tests-config.yaml`. The `%VERSION%` placeholder and the trailing `_` are removed from labels, so `postgresql_%VERSION%` becomes `postgresql`.
  - `path: cassandra` takes the versions of that exact label.
  - `filter: postgresql` takes the versions of every label starting with `postgresql`.
  - The highest version found is the pin. For PostgreSQL that is "the newest server version we test against".
  - Labels without a `versions` list (e.g. `cosmos`, `sqlite_3`) cannot be used.

### 3.2 Upstreams

| `type` | What it reads | Default granularity |
|--------|---------------|---------------------|
| `maven` | All versions in `maven-metadata.xml` | `exact` |
| `docker` | Docker Hub tags, newest first, up to 5 pages of 100 | `major` |
| `github_releases` | The 30 most recent GitHub releases | `exact` |

`version_granularity` can be set on the component, on its `upstream`, on the database, or under `defaults`. The most specific setting wins; if none is set, the default in the table applies. A value under `defaults` also applies to Docker images, replacing their `major` default, so the current configuration does not set one there.

Some vendors publish versions that ScalarDB should not compare against. Set `version_pattern` (a regular expression) on a `maven` or `github_releases` upstream to keep only the versions that matter. The current configuration uses it twice:

- **SQL Server driver:** every release comes as a `jre8` and a `jre11` build (`13.6.0.jre8`, `13.6.0.jre11`). `'\.jre8$'` keeps the build ScalarDB uses.
- **Db2 driver:** IBM also publishes one-off builds such as `12.1.5.0_special_89304`. `'^\d+(\.\d+)*$'` keeps regular releases only.

Docker images publish many non-version tags (`latest`, `alpine`, `17-bookworm`). Use `tag_pattern` (e.g. `'^\d+(\.\d+)?$'`) and `exclude_tags` so only plain version tags are compared.

### 3.3 Feature sources

| `type` | What it reads | How it knows what is new |
|--------|---------------|--------------------------|
| `rss` | Items of an RSS or Atom feed | Items published after the newest item seen last time |
| `github_releases` | Recent releases (title and notes) | Releases published after the newest release seen last time |
| `webpage` | The page, split into parts (list items, paragraphs, headings) | Parts that were not on the page last time |

`keywords` on a source decides which items are **considered** at all:

- No `keywords` → every item is considered. Use this for feeds about one product only (PostgreSQL news, the Cosmos DB blog).
- With `keywords` → only matching items are considered. **Shared vendor feeds must set keywords**, otherwise every AWS or Azure announcement would be considered.
- `match_scope: title` (RSS only) matches keywords against the title only. Use it for shared feeds, because article bodies often mention other products in passing.
- For `webpage`, the database's `watch_features` are added to the source keywords, and keywords are matched against the **new parts** of the page only. All new matching parts of one page change become one item, titled e.g. *Oracle Database: page updated (JSON, partition)*, with up to five of the new parts as its description.

RSS titles and descriptions are converted from HTML to plain text before they are used.

Being considered does **not** mean being sent. Every considered item still goes through the relevance rules (section 6).

### 3.4 Adding a database

Add an entry to `monitors.yaml`. No Python change is needed if the vendor publishes on Maven, Docker Hub, GitHub Releases, an RSS/Atom feed, or a plain HTML page. See the README for a step-by-step guide.

A new kind of upstream or source needs a new plugin class in `src/plugins/` and one line in the registry in `src/plugins/__init__.py`.

---

## 4. One run, step by step

The entry point is `src/main.py`. It creates a `MonitorEngine` (`src/engine.py`) and calls `run()`.

### Step 1 — Load configuration and state

- `monitors.yaml` and `relevance-rules.yaml` are checked first (`src/config_schema.py`): unknown or missing keys, unknown types and values, invalid regular expressions, duplicate ids, and `adapter_gap_keywords` that name a capability the database does not list. If anything is wrong, the run stops with exit code 1 and lists every problem with its location, for example:

  ```
  Error: Invalid monitors.yaml:
    - databases[dynamo].sources[0]: unknown key 'match_scop'
  ```

  The same checks run in the unit tests (`tests/test_config.py`), together with a check that every pin can be read from `build.gradle` and `ci/tests-config.yaml`. So most mistakes are caught on the pull request.
- Databases with `enabled: false` are dropped. The `defaults` section is copied onto each remaining database.
- With `--filter <id>` (or the `DATABASE_FILTER` environment variable), only that database is kept. If no enabled database has that id, the run stops with an error that lists the valid ids (exit code 2). If the baseline is not complete yet, a warning is logged, because a filtered run never completes the baseline.
- The state file given with `--state` is loaded. If it does not exist, the run starts with empty state.

### Step 2 — Version checks

For each component:

1. Read the pin.
2. Ask the upstream plugin for its versions.
3. Remove pre-releases, unless the pin is itself a pre-release.
4. Keep only versions newer than the pin, then apply the granularity.
5. If anything is left, create one **version update** with the pin, the newest version, the level (major / minor / patch), and the full list of newer versions.

"No version update" means "nothing newer than the pin". It does not mean the check failed. If reading the pin or calling the upstream fails, the error is recorded and the run continues with the next component.

### Step 3 — Feature checks

For each source:

1. Look up the source's **cursor** in state: the time of the newest item seen last time. For a web page, the cursor is a hash of the whole page plus a short hash of each part, so the next run can tell which parts are new. The cursor key is `{database id}:{type}:{url or repo}`.
2. Fetch the source and keep only items newer than the cursor. Items without a date are always kept; the "already sent" check in step 5 stops repeats.
3. Keep items that match the source `keywords` (if any).
4. Move the cursor forward past **every** item fetched, matched or not. Otherwise a feed that rarely matches would never move its cursor.
5. Apply the relevance rules (section 6) to each kept item.
6. If the source has no cursor yet but the baseline is already complete, this is a **newly added source**. Its items are recorded as seen and not sent. This avoids a flood from a new feed.

If fetching fails, the error is recorded and the run continues.

### Step 4 — Drop feature items that only repeat a version update

For TiDB, the same GitHub release is both a new version and a feature item. To avoid listing it twice, a feature item is dropped when **all** of these are true:

- it comes from `github_releases`,
- its tag matches one of the newer versions found for that database in step 2,
- it matched no watch feature other than the database's own id or name (e.g. "TiDB" in "TiDB v8.5.9" does not count),
- its relevance reason is not an adapter gap.

RSS and web page items are never dropped here.

Note: TiDB releases on older release lines (e.g. v7.5.x) are older than the pin, so they are not version updates and are not dropped. Because `tidb` is in TiDB's `watch_features`, they are sent as feature items.

### Step 5 — Decide what to send

**Version updates** are checked in this order (`partition_updates` in `src/diff.py`):

1. ScalarDB's pin **changed** since the last run and the new pin is already the newest version → we upgraded. Record it and do not send.
2. Baseline not complete yet → record the newest version as "sent", do not send.
3. This newest version was already sent → do not send.
4. Otherwise → **send**.

A component added after the baseline has no history, so its update is sent on its first run.

**Feature updates** (`partition_feature_updates`):

1. Already judged not relevant before → do not send.
2. Baseline not complete yet → record it, do not send.
3. Already sent before → do not send.
4. Not relevant → record as not relevant, do not send.
5. Otherwise → **send**.

Only relevant feature items can reach Slack.

### Step 6 — Complete the baseline

On a run without `--filter`, `baseline_established` is set to `true`. A filtered run never sets it.

### Step 7 — Run status

| Status | When | Exit code |
|--------|------|-----------|
| `SUCCESS` | No check failed | 0 |
| `PARTIAL_SUCCESS` | Some checks failed | 0 |
| `FAILURE` | Every check failed, or there was nothing to check | 1 |

A `--filter` value that matches no database never gets this far: the run stops in step 1 with exit code 2.

### Step 8 — Write outputs and save state

Always written, even on a dry run:

| File | Contents |
|------|----------|
| `output/report.json` | Status, counts, errors, everything detected, and what was sent |
| `output/slack-payload.json` | The Slack message. `{"blocks": []}` when there is nothing to send |
| `output/notify-slack` | `true` if the message has content, otherwise `false` |

Unless it is a dry run (`--dry-run` or `DRY_RUN=true`), the state file is written with what was sent, the cursors, the pins, the errors, and the run time. The Python program only writes the file; in GitHub Actions, the workflow decides afterwards whether to push it (section 9).

---

## 5. How versions are compared

`src/version_normalizer.py`:

- Any text before the first digit is removed (`v8.5.9` → `8.5.9`), then the version is parsed with Python's `packaging` library.
- Some vendors use formats `packaging` cannot parse, such as the SQL Server driver (`13.6.0.jre8`) and the YugabyteDB driver (`42.7.3-yb-4`). These are split into numbers and words and compared part by part, with numbers compared as numbers. So `13.10.0.jre8` is newer than `13.6.0.jre8`, and `42.7.3-yb-10` is newer than `42.7.3-yb-4`. The leading numbers (`13.6.0`) are used for the update level and the granularity.
- A version counts as a pre-release if `packaging` says so, or if it contains `alpha`, `beta`, `rc`, `preview`, `snapshot`, `milestone`, or a milestone marker such as `-M1`.
- Granularity:
  - `exact` — every newer version.
  - `minor` — only versions with a higher major or minor number.
  - `major` — only versions with a higher major number. A Docker pin of `17` does not report `17.6`, but does report `18`.

---

## 6. How relevance is decided

`evaluate_relevance` in `src/relevance.py` checks these rules in order. The first match decides.

| # | Rule | Result |
|---|------|--------|
| 1 | Text contains a global `irrelevant_keywords` entry | Not relevant |
| 2 | **Adapter gap** (see below) | Relevant |
| 3 | Text contains one of the database's `watch_features` | Relevant |
| 4 | A source keyword that matched is also in `watch_features` | Relevant |
| 5 | Text contains a global `relevant_keywords` entry | Relevant |
| 6 | Nothing matched | Not relevant |

"Text" is the item's title plus its description.

**Adapter gap.** This rule exists for the Cosmos DB case. A database records what its adapter supports today in `adapter_capabilities`, and the words that mean the vendor has moved past it in `adapter_gap_keywords`:

```yaml
adapter_capabilities:
  partition_key_model: v1_single_path
adapter_gap_keywords:
  partition_key_model: [hierarchical, v2]
```

An item that mentions one of those words is relevant, with a reason such as `adapter partition_key_model=v1_single_path vs hierarchical`. Both settings are plain configuration, so the same idea works for any database: add a capability and its words, and nothing in the code changes.

**Keyword matching** (`keyword_in_text` in `src/text_util.py`) ignores case:

- A keyword with a space, or longer than 4 characters, matches anywhere in the text. `partition key` matches "Hierarchical partition keys".
- A keyword of 4 characters or fewer must match a whole word. `fts` does not match "drafts", and `json` does not match "jsonb".

Because rule 1 runs first, an important announcement that also mentions "pricing" is dropped.

---

## 7. The state file

`last-run.json` is how each weekly run knows what earlier runs sent.

- **In GitHub Actions** it lives on the branch `db-update-monitor-state`, not on the default branch (which is protected). That branch holds only `last-run.json` and a short README, and each run that changes the state adds one commit to it. Section 9 explains how the workflow reads and saves it.
- **Locally** it is `state/last-run.json`. Git ignores it, so a local file can never be committed by accident.

To look at the production state:

```bash
git fetch origin db-update-monitor-state
git show origin/db-update-monitor-state:last-run.json
```

| Field | Meaning |
|-------|---------|
| `baseline_established` | `false` until the first full, non-dry run |
| `components` | Per `database:component`: newest version already sent, and last check time |
| `features` | Announcements already sent or recorded during a baseline, with title and first-seen time |
| `seen_irrelevant` | Announcements judged not relevant, so they are not checked again |
| `source_cursors` | Per source: time of the newest item seen. For a web page: the page hash and a short hash of each part (about 7 KB for the Oracle page) |
| `pin_snapshot` | Each component's pin at the last run, used to notice upgrades |
| `errors` | Failed checks of the last run |
| `last_run`, `last_run_status` | Time and status of the last run |

An announcement is identified by `{database id}:{source type}:{url}`, or by title and date if it has no URL. A web page always has the same URL, so a web page change is identified by the URL plus a hash of that version of the page (`{url}#{page hash}`). Each change is therefore reported once, and the next change is reported again.

**Old entries are removed.** Before the state is written, announcements in `features` and `seen_irrelevant` that were first seen more than 365 days ago are dropped, so the file does not grow forever. This is safe because the feed cursors already stop dated items that old from being fetched again. Only a feed item without a date could be sent once more after a year.

**To reset:**

- Locally: delete `state/last-run.json`. A missing file means empty state.
- In production: delete the `db-update-monitor-state` branch. The next run starts with empty state, so it is a silent baseline run that recreates the branch.

---

## 8. The Slack message

Built by `src/notifier/slack.py`. Nothing is sent if there are no new version updates, no new feature updates, **and** no errors.

Otherwise the message contains, in order:

1. A header: *ScalarDB Database Update Monitor — N new update(s)*. Left out when only errors are reported.
2. **VERSION updates**: a fixed-width table with database, component, current, latest, level, and source. If there are several newer versions, a line below lists up to five.
3. **FEATURE updates**: one bullet per item with database, title, description (up to 500 characters), the ScalarDB adapter capabilities if configured, and a link.
4. **Check failures**: up to five failed checks.
5. A link to the GitHub Actions run and the branch name.

The message also has a plain-text summary, for example *ScalarDB Database Update Monitor: 18 new update(s), 1 check failure(s)*. Slack shows it in notifications and previews, where the blocks above are not rendered.

The message is kept within Slack's limits so Slack never rejects it:

- Long sections are split into several, to stay under 3,000 characters each. Each failure message is cut to 300 characters.
- Slack allows at most 50 blocks per message. If there are more, the update list is shortened and ends with a note pointing to `report.json` in the run's artifact. The header, the failures, and the run link are always kept.
- `&`, `<`, and `>` in database names, titles, descriptions, and failure messages are escaped, so they show as text instead of breaking the formatting.

To preview a message locally, post `output/slack-payload.json` to a **test** webhook with `curl`.

---

## 9. The GitHub Actions workflow

File: `.github/workflows/db-update-monitor.yaml`.

| Trigger | Job | What it does |
|---------|-----|--------------|
| Pull request changing `ci/db-update-monitor/**`, the workflow file, `build.gradle`, or `ci/tests-config.yaml` | Unit tests | Runs `pytest`, including the configuration and pin checks. Never posts to Slack. |
| Schedule `0 17 * * 0` (Sunday 17:00 UTC, Monday 02:00 JST) | Check database updates | Full run. Default branch only. |
| Manual "Run workflow" | Check database updates | Same, with optional `database_filter` and `dry_run` inputs. Default branch only. |

The monitor job steps:

1. Check out the default branch, set up Python 3.12, install `requirements.txt`.
2. **Check out the state branch** into a separate folder (`$RUNNER_TEMP/db-update-monitor-state`), using a git worktree. If the branch does not exist yet, create it there as an empty branch with a short README. Then the run starts with empty state, which makes it the baseline run.
3. **Run** `src/main.py` with `--state` pointing at `last-run.json` in that folder, and `GITHUB_TOKEN` for the GitHub API.
4. **Upload** `output/` as the `db-monitor-report` artifact (always, even if the run failed). When the workflow runs locally under act, this step is allowed to fail, because act's artifact server does not support this action version.
5. **Post to Slack**: send `slack-payload.json` to `SLACK_SCALARDB_WEBHOOK_URL` if it is not a dry run and `notify-slack` is `true`. The step fails if Slack rejects the message (`errors: true`).
6. **Save state**: commit `last-run.json` on the state branch and push it, only if all of these are true:
   - it is not a dry run,
   - the monitor run succeeded (status `SUCCESS` or `PARTIAL_SUCCESS`),
   - the Slack post succeeded, or there was nothing to post.

   If the state did not change, nothing is committed.

Why this order: Slack is posted **before** the state is saved. If posting fails, the state is not saved, so the next run sends the same updates again. An update can be sent twice (for example, if Slack accepted the message but the push then failed), but it is never lost. A failed post or push fails the job, so GitHub shows the run as failed.

Things to know:

- The default branch is never written to. The workflow needs `contents: write` only to push the state branch, and that branch must not be covered by branch protection or rulesets.
- Pushes made with `GITHUB_TOKEN` do not start other workflows, so state commits do not trigger CI.
- Only the monitor job uses the concurrency group `db-update-monitor`, so two monitor runs never write the state at the same time. The test job has one group per pull request; a new push cancels that pull request's outdated test run.
- When the run status is `FAILURE`, the run step fails, the Slack message with the failures is still posted, and the state is not saved.

**First production run:** the state branch does not exist yet, so the run records everything, posts nothing (unless checks failed), and creates the branch. From the second weekly run on, only new updates are posted.

---

## 10. Finding out what happened in a run

1. **Log line** at the end of the run step:
   `Run complete: status=…, new_versions=…, new_features=…, errors=…`
2. **`report.json`** in the `db-monitor-report` artifact:
   - `detected_versions`, `detected_features` — everything the checks found.
   - `version_updates`, `feature_updates` — what was sent.
   - For a feature, `relevant` and `relevance_reason` explain the relevance decision.
   - `errors` — which checks failed and why.
3. **`notify-slack`** — whether the workflow would post.
4. **`last-run.json`** on the `db-update-monitor-state` branch — what the next run will treat as already sent. If the "Save state" step was skipped or failed, the next run will send this week's updates again.

**No Slack message this week?** One of these is true:

- Nothing new was found (`notify-slack` is `false`).
- The run was a dry run.
- The run was on a branch other than the default branch, so the job did not run.
- The "Notify Slack" step failed, for example because the webhook secret is wrong. The job is shown as failed, and the updates will be sent again next run.
- This was the first (baseline) run.

**The same update was sent twice?** The previous run's "Save state" step was skipped or failed after Slack was posted. Check that run's log; the most likely cause is that the state branch could not be pushed.

**An expected update was not sent?** Find it in `report.json`:

- Not in `detected_*` → the check did not find it. Check the pin, the upstream settings, the source keywords, and `errors`.
- In `detected_features` with `relevant: false` → no relevance rule matched. Add a keyword to the database's `watch_features` or to `relevant_keywords`.
- In `detected_*` and relevant, but not in `version_updates` / `feature_updates` → it was already sent, or it was recorded during a baseline.

---

## 11. Worked examples

### Cosmos DB hierarchical partition keys

1. The Cosmos entry in `monitors.yaml` reads the Cosmos DB blog feed with no keywords, so every post is considered. `adapter_capabilities` has `partition_key_model: v1_single_path`, and `adapter_gap_keywords` lists `hierarchical` and `v2` for it.
2. A blog post about hierarchical partition keys is published.
3. Relevance: the post contains `hierarchical`, one of the `adapter_gap_keywords` for `partition_key_model` → relevant, with reason `adapter partition_key_model=v1_single_path vs hierarchical`.
4. After the baseline, and if the post's URL was not sent before → it appears under FEATURE updates, with the line *ScalarDB adapter — partition_key_model: v1_single_path*, so readers see the adapter is still on v1.
5. The URL is recorded. The same post is not sent again next week.

A new Cosmos SDK release on Maven would appear separately under VERSION updates.

### PostgreSQL JDBC driver patch

1. `build.gradle` pins `postgresqlDriverVersion = '42.7.14'`.
2. Maven Central lists `42.7.15`. Granularity is `exact` → version update `42.7.14 → 42.7.15`, level patch.
3. `42.7.15` was not sent before → it appears under VERSION updates.
4. Next week, nothing changed → not sent again.
5. Someone bumps `build.gradle` to `42.7.15`. Next run: the pin changed and is now the newest version → recorded quietly, nothing sent.

For the PostgreSQL server image, the pin is the highest PostgreSQL version in `ci/tests-config.yaml` (e.g. `17`) with granularity `major`. `17.6` is not reported. `18` is.

---

## 12. What this tool is not

- Not Dependabot. Dependabot still opens the Gradle upgrade pull requests.
- Not an automatic upgrader or adapter fixer.
- Not real-time. Weekly is intentional.
- Not an AI classifier. Relevance is keyword rules in YAML.
- Not a dashboard or ticketing bot.
