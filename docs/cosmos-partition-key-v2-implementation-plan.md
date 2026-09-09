# Cosmos Partition Key V2 — Implementation Plan

Short summary and step-by-step guide for **ScalarDB product changes** required to support V1 → V2 migration. For **detailed core and Schema Loader specs**, see [cosmos-partition-key-v2-core-and-schema-loader-changes.md](./cosmos-partition-key-v2-core-and-schema-loader-changes.md). For **end-user migration steps** (Azure Portal, cutover), see [cosmos-partition-key-v2-migration-guide.md](./cosmos-partition-key-v2-migration-guide.md).

---

## Summary

ScalarDB’s Cosmos adapter creates **V1** containers (101-byte hash) with **no partition key length validation**. Existing production tables on Azure Cosmos DB are affected — not just test containers.

**Migration has two parts:**

| Part | Who | How |
|------|-----|-----|
| **Data migration** (existing V1 tables) | Operators / end users | Azure Portal **Change partition key** or CLI **container copy** — no custom ScalarDB copy tool required in most cases |
| **Product changes** (new tables + safety) | ScalarDB developers | Enable V2 creation, add validation, Schema Loader support, documentation |

**Key facts:**

- V1 → V2 **cannot be done in-place**; `repairTable()` does **not** upgrade version.
- Partition key **path stays** `/concatenatedPartitionKey`; only the **hash version** changes.
- Full partition key values are **stored in documents**; V1 only affects Cosmos routing (collisions past 101 bytes).
- Document **`id`** limit (255 chars) still applies on V2.
- ScalarDB Java v4 SDK (`4.81.0`) already supports V2; no SDK upgrade needed.

---

## Implementation phases (step-by-step)

### Phase 0 — Prerequisites (before coding)

| Step | Action |
|------|--------|
| 0.1 | Review [cosmos-partition-key-length.md](./cosmos-partition-key-length.md) and integration test results |
| 0.2 | Decide: **V2 by default** for all new tables vs **opt-in** via Schema Loader flag |
| 0.3 | Decide: **hard reject** vs **warn** when partition key exceeds 101 bytes on V1 containers |
| 0.4 | Confirm Azure migration limits with ops (4 TB, 1M RU/s, supported regions) |

---

### Phase 1 — Enable V2 for new containers

**Goal:** New ScalarDB tables are created with `PartitionKeyDefinitionVersion.V2`.

| Step | File | Change |
|------|------|--------|
| 1.1 | `core/.../CosmosAdmin.java` | Update `computeContainerProperties()` to set V2 explicitly (see snippet below) |
| 1.2 | `core/.../CosmosAdmin.java` | If opt-in model: read `large_partition_key` (or similar) from `createTable` / `repairTable` `options` map |
| 1.3 | `CosmosPartitionKeyVersionIntegrationTest.java` | Update Exp 1 to expect V2 after change; keep V1 tests using direct SDK for regression |
| 1.4 | Run integration tests | `./gradlew integrationTestCosmos --tests '...CosmosPartitionKeyVersionIntegrationTest'` |

**Code change (V2 default):**

```java
PartitionKeyDefinition partitionKeyDefinition =
    new PartitionKeyDefinition()
        .setKind(PartitionKind.HASH)
        .setPaths(Collections.singletonList(PARTITION_KEY_PATH))
        .setVersion(PartitionKeyDefinitionVersion.V2);
return new CosmosContainerProperties(table, partitionKeyDefinition)
    .setIndexingPolicy(indexingPolicy);
```

**Do not change:** `repairTable()` behavior for **existing** containers — `createContainerIfNotExists` must remain a no-op when the container already exists.

---

### Phase 2 — Length validation

**Goal:** Fail fast with ScalarDB-native errors instead of silent V1 collisions or opaque Cosmos `BadRequest`.

| Step | File | Change |
|------|------|--------|
| 2.1 | `core/.../CoreError.java` | Add e.g. `COSMOS_CONCATENATED_PARTITION_KEY_TOO_LONG`, `COSMOS_DOCUMENT_ID_TOO_LONG` |
| 2.2 | `core/.../CosmosOperationChecker.java` | Validate UTF-8 byte length of concatenated partition key |
| 2.3 | `CosmosOperationChecker.java` | Validate document `id` length ≤ 255 chars (PK + CK joined) |
| 2.4 | `CosmosAdmin` or storage layer | Read container PK version at metadata load; apply limit **101 (V1)** or **2048 (V2)** |
| 2.5 | Unit tests | Boundary cases: 101/102 bytes, 2048/2049 bytes, 255/256 char id |
| 2.6 | Integration tests | Add tests for validation rejection paths |

**Limits to enforce:**

| Check | V1 container | V2 container |
|-------|--------------|--------------|
| `concatenatedPartitionKey` (UTF-8 bytes) | ≤ 101 (or warn) | ≤ 2048 |
| Document `id` (chars) | ≤ 255 | ≤ 255 |

---

### Phase 3 — Schema Loader support

**Goal:** Operators can create V2 tables via the standard Schema Loader path.

| Step | File | Change |
|------|------|--------|
| 3.1 | `schema-loader/.../CosmosCommand.java` | Add `--large-partition-key` flag (skip if Phase 1 makes V2 the default) |
| 3.2 | `CosmosCommand.java` | Pass flag into `createTable` / `repairTable` options map |
| 3.3 | Schema Loader docs / `--help` | Document flag and when to use it |
| 3.4 | Test | Schema Loader integration test creating V2 container |

---

### Phase 4 — Documentation

**Goal:** Users and operators know limits, migration path, and post-migration steps.

| Step | Document | Content |
|------|----------|---------|
| 4.1 | Official Cosmos storage docs | 101 / 2048 byte limits, BLOB Base64 overhead, composite key `:` separators |
| 4.2 | [cosmos-partition-key-v2-migration-guide.md](./cosmos-partition-key-v2-migration-guide.md) | Already created — review and publish |
| 4.3 | [cosmos-partition-key-length.md](./cosmos-partition-key-length.md) | Link to migration guide; mark recommendations A/B/C/D status |
| 4.4 | Release notes | Breaking change notice if V2 default affects expectations |

---

### Phase 5 — Migration support (ops / optional code)

**Goal:** Enable existing V1 deployments to migrate without building a custom bulk-copy tool.

| Step | Owner | Action |
|------|-------|--------|
| 5.1 | Docs (done) | Publish end-user guide: Azure Portal → copy → ScalarDB cutover |
| 5.2 | Operators | Pre-migration assessment: keys >101 bytes, shared prefixes, BLOB PKs |
| 5.3 | Operators | Run Azure Portal **Change partition key** (offline recommended) |
| 5.4 | Operators | **ScalarDB cutover:** `repairTable` or Schema Loader `--repair-all` on destination container |
| 5.5 | Operators | Verification checklist (document count, get/scan/put, stored procedure) |
| 5.6 | *(Optional)* | Schema Loader subcommand wrapping Azure CLI copy + repair |

**Not required:** Custom ScalarDB bulk migration tool (unless Azure limits exceeded).

---

## Implementation order

```
Phase 0  Decide defaults
   ↓
Phase 1  V2 container creation          ← unblocks new deployments
   ↓
Phase 2  Length validation              ← prevents new silent collisions
   ↓
Phase 3  Schema Loader flag             ← if opt-in; skip if V2 default
   ↓
Phase 4  Documentation
   ↓
Phase 5  Migration ops (parallel)       ← existing tables via Azure Portal
```

Phases 1–2 are **required** for product completeness. Phase 5 data migration can start **before** Phase 1 ships (using Portal + manual V2 destination + repairTable), but new tables will remain V1 until Phase 1 lands.

---

## Checklist (shareable)

### Developer deliverables

- [ ] `CosmosAdmin` creates V2 containers (or opt-in via options)
- [ ] `CosmosOperationChecker` validates PK length and document `id` length
- [ ] `CoreError` codes for length violations
- [ ] Container PK version read for version-aware limits
- [ ] Schema Loader flag (if not V2-by-default)
- [ ] Unit + integration tests updated
- [ ] User-facing docs published

### Operator deliverables (per existing V1 table)

- [ ] Assess collision / length risk
- [ ] Azure Portal or CLI container copy (V2 destination, same `/concatenatedPartitionKey` path)
- [ ] Stop writes → copy → verify counts
- [ ] Cutover to final container name
- [ ] Run `repairTable` / Schema Loader repair
- [ ] Smoke test → decommission V1 container

---

## One-line summary

> **Implement V2 container creation + length validation in ScalarDB; use Azure Portal container copy for existing V1 table data migration; run `repairTable` after cutover.** No custom bulk-copy tool needed for most deployments.
