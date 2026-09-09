# Cosmos DB Partition Key V1/V2 — Experiment Guide

This document describes how to set up the environment and run the investigation experiments for ScalarDB's Cosmos DB adapter partition-key behavior. It complements the migration plan and is intended to be executed **before** changing production code.

## Background

ScalarDB's Cosmos adapter stores a synthetic partition key at `/concatenatedPartitionKey`, built by colon-joining logical partition-key column values ([`ConcatenationVisitor`](../core/src/main/java/com/scalar/db/storage/cosmos/ConcatenationVisitor.java)). Containers are created via [`CosmosAdmin`](../core/src/main/java/com/scalar/db/storage/cosmos/CosmosAdmin.java) **without** setting `PartitionKeyDefinitionVersion`, so Cosmos defaults to **V1** (hash based on the first 101 bytes).

| Version | Hash input | Max effective key | Set at |
|---------|------------|-------------------|--------|
| V1 | First 101 bytes | 101 bytes (collision risk beyond) | Container creation only |
| V2 | Full key | 2048 bytes | Container creation only |

References:

- [Cosmos DB limits](https://learn.microsoft.com/en-us/azure/cosmos-db/concepts-limits#per-item-limits)
- [Large partition keys](https://learn.microsoft.com/en-us/azure/cosmos-db/large-partition-keys?tabs=dotnetv3)
- [Partition key collision troubleshooting](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-conflict)

## Prerequisites

### 1. Cosmos DB Emulator (local)

ScalarDB Cosmos integration tests expect the emulator at `https://localhost:8081/`.

**Windows (CI setup — see [`ci/tests-config.yaml`](../ci/tests-config.yaml)):**

```powershell
Import-Module "$env:ProgramFiles\Azure Cosmos DB Emulator\PSModules\Microsoft.Azure.CosmosDB.Emulator"
Start-CosmosDbEmulator
```

**Linux:** Use the [Azure Cosmos DB Emulator for Linux](https://learn.microsoft.com/en-us/azure/cosmos-db/local-testing/emulator-linux) or a dev Azure Cosmos DB account.

Default emulator primary key (used in CI):

```
C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==
```

### 2. Build ScalarDB

From the repository root:

```bash
./gradlew :core:compileIntegrationTestCosmosJava
```

### 3. Run integration tests (sanity check)

```bash
./gradlew integrationTestCosmos \
  -Dscalardb.cosmos.uri=https://localhost:8081/ \
  -Dscalardb.cosmos.password='C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==' \
  -Dfile.encoding=UTF-8 \
  --tests 'com.scalar.db.storage.cosmos.CosmosSinglePartitionKeyIntegrationTest'
```

Integration test properties are wired through [`CosmosEnv`](../core/src/integration-test/java/com/scalar/db/storage/cosmos/CosmosEnv.java):

| System property | Purpose |
|-----------------|---------|
| `scalardb.cosmos.uri` | Cosmos endpoint |
| `scalardb.cosmos.password` | Account key |
| `scalardb.cosmos.create_options` | Optional RU settings (default: `ru:10000`) |

Each test class gets an isolated metadata database via a `testName` suffix on the system namespace.

---

## Recommended approach

Integration tests encode all experiments in:

**[`CosmosPartitionKeyVersionIntegrationTest.java`](../core/src/integration-test/java/com/scalar/db/storage/cosmos/CosmosPartitionKeyVersionIntegrationTest.java)**

Helper utilities live in [`CosmosPartitionKeyTestUtils.java`](../core/src/integration-test/java/com/scalar/db/storage/cosmos/CosmosPartitionKeyTestUtils.java).

**Step-by-step run instructions:** [cosmos-partition-key-v2-test-run.md](./cosmos-partition-key-v2-test-run.md)

**V1 → V2 migration guide (end users):** [cosmos-partition-key-v2-migration-guide.md](./cosmos-partition-key-v2-migration-guide.md)

**Core and Schema Loader changes (developer spec):** [cosmos-partition-key-v2-core-and-schema-loader-changes.md](./cosmos-partition-key-v2-core-and-schema-loader-changes.md)

Run the full investigation suite:

```bash
./gradlew integrationTestCosmos \
  -Dscalardb.cosmos.uri=https://localhost:8081/ \
  -Dscalardb.cosmos.password='C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==' \
  --tests 'com.scalar.db.storage.cosmos.CosmosPartitionKeyVersionIntegrationTest'
```

The sections below describe each experiment's goal, setup, steps, and expected results. Use the **Results log** section at the end to record outcomes.

---

## Helper utilities

Use these when constructing test keys. ASCII characters are 1 byte each in UTF-8, which simplifies byte-length control.

```java
/** Returns a string of exactly {@code numBytes} ASCII characters. */
static String asciiOfLength(int numBytes) {
  if (numBytes <= 0) {
    return "";
  }
  return "x".repeat(numBytes);
}

/**
 * Two distinct keys whose first {@code prefixBytes} bytes are identical
 * but differ afterward — useful for V1 collision tests.
 */
static String keyWithPrefix(String prefix, char differingChar, int totalBytes) {
  assert prefix.length() < totalBytes;
  int suffixLen = totalBytes - prefix.length() - 1;
  return prefix + differingChar + "y".repeat(suffixLen);
}

/** UTF-8 byte length (not char count — important for non-ASCII text). */
static int utf8ByteLength(String s) {
  return s.getBytes(StandardCharsets.UTF_8).length;
}
```

**Suggested table schema for most experiments:**

```java
TableMetadata metadata =
    TableMetadata.newBuilder()
        .addColumn("pk", DataType.TEXT)
        .addColumn("ck", DataType.TEXT)   // only for Experiment 5
        .addColumn("value", DataType.INT)
        .addPartitionKey("pk")
        .addClusteringKey("ck", Scan.Ordering.Order.ASC)  // optional
        .build();
```

**Reading partition key version from a container:**

```java
CosmosContainerProperties props =
    client.getDatabase(namespace).getContainer(table).read().getProperties();
PartitionKeyDefinitionVersion version = props.getPartitionKeyDefinition().getVersion();
// null or V1 for ScalarDB-created containers today; V2 when explicitly set
```

**Creating a V2 container manually (bypass ScalarDB admin for Experiments 3, 4, 7):**

```java
PartitionKeyDefinition pkDef = new PartitionKeyDefinition();
pkDef.setPaths(Collections.singletonList("/concatenatedPartitionKey"));
pkDef.setVersion(PartitionKeyDefinitionVersion.V2);
CosmosContainerProperties containerProps =
    new CosmosContainerProperties(tableName, pkDef);
client.getDatabase(namespace).createContainerIfNotExists(containerProps);
// Then register ScalarDB metadata separately via admin.upsertTableMetadata(...) or createTable repair path
```

---

## Experiment 1 — Confirm V1 default from ScalarDB

**Goal:** Establish baseline — ScalarDB creates V1 (or unset-version) containers today.

**Steps:**

1. Create namespace + table via `DistributedStorageAdmin.createTable()` (standard integration test setup).
2. Read container properties with the Azure SDK (snippet above).
3. Record `getPartitionKeyDefinition().getVersion()` and `getPaths()`.

**Expected result:**

| Field | Expected |
|-------|----------|
| Path | `[/concatenatedPartitionKey]` |
| Version | `null` or `PartitionKeyDefinitionVersion.V1` |

**Pass criteria:** Version is **not** V2.

---

## Experiment 2 — Boundary length behavior on V1

**Goal:** Observe write/read behavior when partition keys cross the 101-byte hash boundary.

**Setup:** Single text partition key column (`pk`), no clustering key. Create table via normal ScalarDB `createTable()` (V1 container).

| Case | Partition key (text) | UTF-8 bytes | What to test |
|------|-------------------|-------------|--------------|
| 2A | `asciiOfLength(101)` | 101 | Put + Get succeeds |
| 2B | `asciiOfLength(102)` | 102 | Put succeeds; hash uses first 101 bytes only |
| 2C | Same first 101 bytes as 2A, different 102nd byte | 102 | **Collision** — same logical partition as 2A |
| 2D | `asciiOfLength(200)` | 200 | Put succeeds; document stores full key |

**Steps (per case):**

1. `Put` with `Key.ofText("pk", keyValue)` and a distinct `value` column.
2. `Get` with the same partition key — verify returned row.
3. For 2C: after inserting both 2A and 2C keys, run a partition scan (`Scan` with that partition key) and check whether **both** rows appear in one partition.
4. Optionally query Cosmos directly:

   ```sql
   SELECT c.id, c.concatenatedPartitionKey FROM c
   WHERE c.concatenatedPartitionKey = '<key>'
   ```

**Expected results:**

- 2A, 2B, 2D: individual point reads succeed.
- 2C: both items share a partition; if `id` values also collide within the merged partition, Cosmos may return **409 Conflict** on the second insert (see [troubleshoot-conflict](https://learn.microsoft.com/en-us/azure/cosmos-db/troubleshoot-conflict)).

**Record:** Actual error messages, HTTP status codes, and whether Get returns the wrong row for colliding keys.

---

## Experiment 3 — V2 container creation and CRUD

**Goal:** Verify ScalarDB read/write works against a V2-configured container.

**Steps:**

1. Create a V2 container manually (helper snippet above) in an isolated namespace/table name, e.g. `pk_v2_test`.
2. Register table metadata so ScalarDB knows the schema (via `createTable` on a fresh namespace, or `repairTable` if metadata container already exists).
3. Execute ScalarDB CRUD:

   | Key length | Operation | Expected |
   |------------|-----------|----------|
   | 101 bytes | Put, Get, Delete | Success |
   | 150 bytes | Put, Get, Delete | Success |
   | Collision pair (Exp 4) | Put both keys | Both succeed, separate partitions |

4. Re-read container version — must remain **V2**.

**Pass criteria:** All CRUD operations succeed; version stays V2; collision pair does **not** co-locate (contrast with Experiment 2C on V1).

---

## Experiment 4 — V1 vs V2 collision reproduction

**Goal:** Demonstrate silent V1 collision and confirm V2 separation.

**Key construction (102 bytes, differ only at byte 102):**

```java
String prefix = asciiOfLength(101);           // bytes 1–101
String key1 = prefix + "A";                   // 102 bytes
String key2 = prefix + "B";                   // 102 bytes — same first 101 bytes as key1
assert utf8ByteLength(key1) == 102;
assert utf8ByteLength(key2) == 102;
```

Use **different clustering keys or value columns** so `id` values differ (avoid `id` collision masking partition-key collision).

**On V1 container:**

1. Put row with `key1`, Put row with `key2`.
2. Partition scan for `key1` — note whether `key2`'s row also appears.
3. Note any 409 errors.

**On V2 container:**

1. Repeat with fresh container.
2. Partition scan for `key1` — should return **only** the `key1` row.

**Expected:**

| Container | Same first 101 bytes, differ at 102 | Outcome |
|-----------|-------------------------------------|---------|
| V1 | key1 vs key2 | Same logical partition (collision) |
| V2 | key1 vs key2 | Different logical partitions |

---

## Experiment 5 — Document `id` length limit (255 chars)

**Goal:** Determine the practical ScalarDB key limit from Cosmos document `id` constraints — independent of partition key version.

ScalarDB sets document `id` = colon-join of **partition + clustering** columns ([`CosmosOperation.getId()`](../core/src/main/java/com/scalar/db/storage/cosmos/CosmosOperation.java)). Cosmos `id` max = **255 characters**.

**Setup:** Table with text partition key `pk` and text clustering key `ck`.

**Steps:**

| Case | pk length | ck length | Total id length (`pk + ":" + ck`) | Expected |
|------|-----------|-----------|-----------------------------------|----------|
| 5A | 127 | 127 | 255 | Put succeeds |
| 5B | 128 | 127 | 256 | Put **fails** |
| 5C | 200 | 1 | 202 | Put succeeds (partition key >101 bytes on V1 — note mis-partitioning) |

**Pass criteria:** Document exact exception type/message at 256 chars. This bounds how much V2's 2048-byte partition key limit helps ScalarDB in practice.

---

## Experiment 6 — `repairTable()` does not upgrade version

**Goal:** Confirm repair is **not** a migration path for V1 → V2.

**Steps:**

1. Create table via unmodified ScalarDB (V1 container). Record version = V1/null.
2. Locally patch `CosmosAdmin.computeContainerProperties()` to emit V2 (dev branch only — do not merge until experiments complete).
3. Call `admin.repairTable(namespace, table, metadata, options)`.
4. Re-read container partition key version.

**Expected:** Version remains V1/null. `createContainerIfNotExists` is a no-op on existing containers; partition key definition is immutable.

**Pass criteria:** Version unchanged after repair.

---

## Experiment 7 — Stored procedure with long partition keys on V2

**Goal:** Ensure batch/conditional mutations ([`mutate.js`](../core/src/main/resources/cosmosdb_stored_procedure/mutate.js)) work with long keys on V2.

**Setup:** V2 container (Experiment 3). Partition key = 150-byte ASCII string.

**Steps:**

1. **Batch mutate** — multiple Puts in one partition (pattern from [`CosmosMutationAtomicityUnitIntegrationTest`](../core/src/integration-test/java/com/scalar/db/storage/cosmos/CosmosMutationAtomicityUnitIntegrationTest.java)).
2. **Conditional Put** — `PutIfNotExists` / `PutIf` with condition on clustering key.
3. **Conditional Delete** — `DeleteIf` with condition.

**Expected:** All succeed without JavaScript errors from the stored procedure. ScalarDB mutation atomicity unit is `PARTITION` ([`Cosmos.java`](../core/src/main/java/com/scalar/db/storage/cosmos/Cosmos.java)).

**Record:** Any stored-procedure or 412 Precondition Failed errors.

---

## Optional — Emulator vs Azure account parity

If the emulator behaves differently from production for V2:

1. Repeat Experiments 3 and 4 against a **real Azure Cosmos DB account** (NoSQL API).
2. Compare `PartitionKeyDefinitionVersion` after container creation and collision behavior.

Record emulator version and any discrepancies in the results log.

---

## Results log

Copy this table and fill in after running experiments.

| Exp | Test method | Environment (emulator / Azure) | Result (pass/fail) | Notes |
|-----|-------------|-------------------------------|--------------------|-------|
| 1 | `createTable_shouldUsePartitionKeyDefinitionV1` | | | Version observed: |
| 2A | `putAndGet_with101BytePartitionKey_onV1Container_shouldSucceed` | | | |
| 2B | `putAndGet_with102BytePartitionKey_onV1Container_shouldSucceed` | | | |
| 2C | `put_withColliding102ByteKeys_onV1Container_shouldSharePartition` | | | Collision observed? |
| 2D | `putAndGet_with200BytePartitionKey_onV1Container_shouldSucceed` | | | |
| 3 | `putAndGet_with150BytePartitionKey_onV2Container_shouldSucceed` | | | |
| 4 V1 | (covered by 2C) | | | |
| 4 V2 | `put_withColliding102ByteKeys_onV2Container_shouldNotSharePartition` | | | |
| 5A | `put_with255CharDocumentId_shouldSucceed` | | | |
| 5B | `put_with256CharDocumentId_shouldFail` | | | Error: |
| 6 | `repairTable_shouldNotUpgradePartitionKeyVersionFromV1` | | | |
| 7 | `mutate_with150BytePartitionKey_onV2Container_shouldSucceed` | | | |

---

## Findings template (post-investigation)

After completing experiments, summarize:

1. **Default version:** ScalarDB creates V1 / V2 / unset?
2. **V1 collision reproduced?** Yes/No — under what key pattern?
3. **V2 CRUD + stored procedure:** Fully compatible? Any limits hit before 2048 bytes?
4. **Effective ScalarDB max key size:** Limited by `id` (255) or partition key (2048)?
5. **Migration recommendation:**
   - New tables: enable V2 immediately? Y/N
   - Existing tables: any production keys >101 bytes or collision risk?
   - `repairTable()` sufficient? (expected: No)

---

## Next steps (after experiments)

See the migration plan for implementation phases:

1. **Phase A** — Set `PartitionKeyDefinitionVersion.V2` in `CosmosAdmin.computeContainerProperties()` for new containers.
2. **Phase B** — Add length validation in `CosmosOperationChecker`.
3. **Phase C** — Manual V1 → V2 migration playbook for existing deployments. See [cosmos-partition-key-v2-migration-guide.md](./cosmos-partition-key-v2-migration-guide.md) (Azure Portal container copy + ScalarDB cutover).

Promote stable experiment cases into permanent tests in `CosmosPartitionKeyVersionIntegrationTest` (or `CosmosLargePartitionKeyIntegrationTest`) so regressions are caught in CI.
