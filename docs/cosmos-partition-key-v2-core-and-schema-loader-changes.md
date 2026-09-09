# Cosmos Partition Key V2 — ScalarDB Core and Schema Loader Changes

Detailed specification for product changes required in **ScalarDB Core** and **Schema Loader** to support Cosmos DB large partition keys (V2). This document is for developers implementing the changes.

**Related documents:**

- [Investigation report](./cosmos-partition-key-length.md)
- [Implementation plan](./cosmos-partition-key-v2-implementation-plan.md)
- [End-user migration guide](./cosmos-partition-key-v2-migration-guide.md)
- [Integration test run guide](./cosmos-partition-key-v2-test-run.md)

---

## Overview

| Area | Goal |
|------|------|
| **Core — container creation** | New ScalarDB tables use `PartitionKeyDefinitionVersion.V2` |
| **Core — validation** | Reject invalid key lengths with ScalarDB-native errors |
| **Schema Loader** | Expose V2 creation via CLI (if opt-in) and pass options to `CosmosAdmin` |
| **Out of scope** | Data migration (Azure Portal), changes to get/scan/put logic |

**Design decisions to make before implementation:**

| Decision | Option A (recommended) | Option B |
|----------|------------------------|----------|
| V2 default | All new containers use V2 | Opt-in only via flag |
| V1 length violation | Hard reject at 101 UTF-8 bytes | Warn in logs, allow write |
| PK version for validation | Read from Cosmos container at check time | Cache in ScalarDB metadata |

---

## Part 1 — ScalarDB Core Changes

### 1.1 Enable V2 partition key on container creation

**File:** `core/src/main/java/com/scalar/db/storage/cosmos/CosmosAdmin.java`

**Current behavior** (`computeContainerProperties`, lines 196–201):

```java
private CosmosContainerProperties computeContainerProperties(
    String table, TableMetadata metadata) {
  IndexingPolicy indexingPolicy = computeIndexingPolicy(metadata);
  return new CosmosContainerProperties(table, PARTITION_KEY_PATH)
      .setIndexingPolicy(indexingPolicy);
}
```

No `PartitionKeyDefinitionVersion` is set → Cosmos defaults to **V1**.

**Required change:**

1. Add a constant for the creation option key:

   ```java
   public static final String LARGE_PARTITION_KEY = "large_partition_key";
   public static final String DEFAULT_LARGE_PARTITION_KEY = "true"; // if V2-by-default
   ```

2. Update `computeContainerProperties` to accept `Map<String, String> options` (or read from a field set by `createTable` / `repairTable` callers).

3. Build an explicit `PartitionKeyDefinition` with V2 when enabled:

   ```java
   import com.azure.cosmos.models.PartitionKeyDefinition;
   import com.azure.cosmos.models.PartitionKeyDefinitionVersion;
   import com.azure.cosmos.models.PartitionKind;

   private CosmosContainerProperties computeContainerProperties(
       String table, TableMetadata metadata, Map<String, String> options) {
     IndexingPolicy indexingPolicy = computeIndexingPolicy(metadata);
     PartitionKeyDefinition partitionKeyDefinition =
         new PartitionKeyDefinition()
             .setKind(PartitionKind.HASH)
             .setPaths(Collections.singletonList(PARTITION_KEY_PATH))
             .setVersion(
                 isLargePartitionKeyEnabled(options)
                     ? PartitionKeyDefinitionVersion.V2
                     : PartitionKeyDefinitionVersion.V1);
     return new CosmosContainerProperties(table, partitionKeyDefinition)
         .setIndexingPolicy(indexingPolicy);
   }

   private boolean isLargePartitionKeyEnabled(Map<String, String> options) {
     // V2-by-default: return true unless explicitly disabled
     return Boolean.parseBoolean(
         options.getOrDefault(LARGE_PARTITION_KEY, DEFAULT_LARGE_PARTITION_KEY));
   }
   ```

4. Thread `options` through:
   - `createTableInternal` → `createContainer` → `computeContainerProperties`
   - `repairTable` → `createContainer(..., ifNotExists=true)` — **only creates if missing**; existing V1 containers are untouched

**Imports to add:** `PartitionKeyDefinition`, `PartitionKeyDefinitionVersion`, `PartitionKind`

**Behavior after change:**

| Operation | Existing V1 container | New / missing container |
|-----------|----------------------|-------------------------|
| `createTable` | Fails (already exists) | Creates **V2** (if default) |
| `repairTable` | No-op on container PK version | Creates **V2** if container missing |

**Important:** `repairTable()` must **not** upgrade V1 → V2 on an existing container. `createContainerIfNotExists` guarantees this — verified by integration test `repairTable_shouldNotUpgradePartitionKeyVersionFromV1`.

---

### 1.2 Pass options through `createContainer`

**File:** `CosmosAdmin.java`

**Current:** `createContainer(database, table, metadata, ifNotExists)` does not receive options.

**Change:** Add `Map<String, String> options` parameter and pass to `computeContainerProperties(table, metadata, options)`.

**Call sites to update:**

| Method | Pass options from |
|--------|-------------------|
| `createTableInternal` | `createTable(..., options)` |
| `repairTable` | `repairTable(..., options)` |
| `importTable` / other admin paths | Same options map |

---

### 1.3 Read container partition key version (for validation)

**Purpose:** `CosmosOperationChecker` needs to know whether a table is V1 (101-byte limit) or V2 (2048-byte limit).

**Option A — Read from Cosmos at validation time (simpler, higher latency):**

Add a package-visible or public method on `CosmosAdmin`:

```java
public Optional<PartitionKeyDefinitionVersion> getPartitionKeyDefinitionVersion(
    String namespace, String table) {
  CosmosContainerProperties props =
      client.getDatabase(namespace).getContainer(table).read().getProperties();
  PartitionKeyDefinition def = props.getPartitionKeyDefinition();
  return Optional.ofNullable(def.getVersion()); // empty or V1 → treat as V1
}
```

**Option B — Cache at metadata load time (better performance):**

Extend `CosmosTableMetadata` or a side cache populated when `TableMetadataManager` loads metadata. More invasive; defer unless performance is a concern.

**Recommendation:** Start with Option A; optimize later if needed.

**V1 detection:** Treat `null` or `PartitionKeyDefinitionVersion.V1` as V1 (matches `CosmosPartitionKeyTestUtils.isV1OrUnset`).

---

### 1.4 Length validation in `CosmosOperationChecker`

**File:** `core/src/main/java/com/scalar/db/storage/cosmos/CosmosOperationChecker.java`

**Current checks** (no length validation):

- Illegal characters in TEXT primary keys: `:`, `/`, `\`, `#`, `?`
- BIGINT range: ±2^53

**New checks to add:**

| Check | Limit | Applies to |
|-------|-------|------------|
| Concatenated partition key (UTF-8 bytes) | 101 (V1) / 2048 (V2) | Get, Scan (with PK), Put, Delete |
| Document `id` (Java `String.length()` or UTF-8 bytes — match Cosmos) | 255 characters | Put, Delete (when full PK+CK specified) |

**Implementation approach:**

1. Add constants:

   ```java
   private static final int V1_PARTITION_KEY_MAX_UTF8_BYTES = 101;
   private static final int V2_PARTITION_KEY_MAX_UTF8_BYTES = 2048;
   private static final int DOCUMENT_ID_MAX_LENGTH = 255;
   ```

2. Add helper using existing `CosmosOperation` logic:

   ```java
   private void checkPartitionKeyAndDocumentIdLengths(Operation operation, TableMetadata metadata)
       throws ExecutionException {
     CosmosOperation cosmosOperation = new CosmosOperation(operation, metadata);
     String concatenatedPk = cosmosOperation.getConcatenatedPartitionKey();
     int pkByteLength = concatenatedPk.getBytes(StandardCharsets.UTF_8).length;

     PartitionKeyDefinitionVersion version =
         resolvePartitionKeyVersion(operation.getNamespace(), operation.getTable());
     int maxPkBytes =
         version == PartitionKeyDefinitionVersion.V2
             ? V2_PARTITION_KEY_MAX_UTF8_BYTES
             : V1_PARTITION_KEY_MAX_UTF8_BYTES;

     if (pkByteLength > maxPkBytes) {
       throw new IllegalArgumentException(
           CoreError.COSMOS_CONCATENATED_PARTITION_KEY_TOO_LONG.buildMessage(
               operation.getNamespace(),
               operation.getTable(),
               pkByteLength,
               maxPkBytes,
               versionName(version)));
     }

     if (cosmosOperation.isPrimaryKeySpecified()) {
       String id = cosmosOperation.getId();
       if (id.length() > DOCUMENT_ID_MAX_LENGTH) {
         throw new IllegalArgumentException(
             CoreError.COSMOS_DOCUMENT_ID_TOO_LONG.buildMessage(
                 operation.getNamespace(), operation.getTable(), id.length(), DOCUMENT_ID_MAX_LENGTH));
       }
     }
   }
   ```

3. Call from `check(Get)`, `check(Scan)`, `check(Put)`, `check(Delete)` after `checkPrimaryKey()`.

4. Inject `CosmosAdmin` or a small `PartitionKeyVersionProvider` interface into `CosmosOperationChecker` to resolve container version.

**Dependency note:** `CosmosOperationChecker` currently receives `DatabaseConfig`, `TableMetadataManager`, `StorageInfoProvider`. Adding version lookup requires either:
- Injecting `CosmosAdmin` (or a narrow interface), or
- Adding version to `TableMetadataManager` cache

**Edge cases:**

| Case | Behavior |
|------|----------|
| Multi-column partition key | Validate **concatenated** result (includes `:` separators) |
| BLOB partition key | Base64 expansion already in `ConcatenationVisitor` — validate output bytes |
| Scan without partition key (full scan) | Skip PK length check |
| Partial clustering key on scan | Skip document `id` check until full key specified |
| Table/container not found during version read | Fail with existing `ExecutionException` path |

---

### 1.5 New error codes in `CoreError`

**File:** `core/src/main/java/com/scalar/db/common/CoreError.java`

Add entries following existing `COSMOS_*` pattern (Category `USER_ERROR`):

```java
COSMOS_CONCATENATED_PARTITION_KEY_TOO_LONG(
    Category.USER_ERROR,
    "<next-id>",
    "The concatenated partition key for table %s.%s exceeds the maximum length for this "
        + "container's partition key version (%s). Length: %d bytes, maximum: %d bytes. "
        + "ScalarDB joins partition key columns with ':' and encodes BLOB columns as Base64.",
    "",
    ""),

COSMOS_DOCUMENT_ID_TOO_LONG(
    Category.USER_ERROR,
    "<next-id>",
    "The document id for table %s.%s exceeds Cosmos DB's maximum of %d characters. "
        + "ScalarDB builds the document id by joining partition key and clustering key columns "
        + "with ':'. Length: %d characters.",
    "",
    ""),
```

Assign the next available error ID numbers in sequence with surrounding entries.

**Add message templates to:** `core/src/main/resources/error_messages.properties` (if the project uses externalized messages — verify project convention).

---

### 1.6 No changes required

| Component | Reason |
|-----------|--------|
| `CosmosOperation.java` | Already builds full concatenated key — version-agnostic |
| `ConcatenationVisitor.java` | No change |
| `SelectStatementHandler.java` | get/scan/put logic unchanged |
| `MutateStatementHandler.java` / `mutate.js` | Stored procedure is PK-format agnostic |
| `CosmosConfig.java` / `CosmosUtils.java` | Java v4 SDK 4.81.0 already supports V2 |
| `repairTable()` upgrade logic | Must **not** add V1→V2 upgrade |

---

### 1.7 Core unit tests

| Test class | Cases |
|------------|-------|
| `CosmosAdminTest` (or new) | `computeContainerProperties` with V2 default; with `large_partition_key=false` → V1 |
| `CosmosOperationCheckerTest` (new or extend) | PK at 101/102 bytes on V1; 2048/2049 on V2; id at 255/256 chars |
| `CosmosOperationTest` | Existing tests unchanged; add long-key cases |

---

### 1.8 Core integration tests

**File:** `core/src/integration-test/java/.../CosmosPartitionKeyVersionIntegrationTest.java`

| Test | Update after V2 default |
|------|-------------------------|
| `createTable_shouldUsePartitionKeyDefinitionV1` | Rename/change to expect **V2** |
| V1-specific tests | Keep using direct SDK to create V1 containers for regression |
| New test | Validation rejects 102-byte key when checker enabled on V1 container |
| New test | Validation rejects 2049-byte key on V2 container |

Run:

```bash
./gradlew integrationTestCosmos \
  -Dscalardb.cosmos.uri=https://localhost:8081/ \
  -Dscalardb.cosmos.password='<key>' \
  --tests 'com.scalar.db.storage.cosmos.CosmosPartitionKeyVersionIntegrationTest'
```

---

## Part 2 — Schema Loader Changes

### 2.1 Add CLI flag for large partition keys

**File:** `schema-loader/src/main/java/com/scalar/db/schemaloader/command/CosmosCommand.java`

**Add option** (required if V2 is opt-in; optional if V2 is always default):

```java
@Option(
    names = {"--large-partition-key"},
    description =
        "Enable Cosmos DB large partition keys (V2, up to 2048 bytes) when creating containers")
private Boolean largePartitionKey;
```

**Wire into options map** in `call()`:

```java
Map<String, String> options = new HashMap<>();
if (ru != null) {
  options.put(CosmosAdmin.REQUEST_UNIT, ru);
}
if (noScaling != null) {
  options.put(CosmosAdmin.NO_SCALING, noScaling.toString());
}
if (largePartitionKey != null) {
  options.put(CosmosAdmin.LARGE_PARTITION_KEY, largePartitionKey.toString());
}
```

If V2 is the default in `CosmosAdmin`, this flag allows **opting out**:

```java
@Option(
    names = {"--legacy-partition-key"},
    description = "Use V1 partition keys (101-byte hash) instead of V2")
private Boolean legacyPartitionKey;
// options.put(LARGE_PARTITION_KEY, "false")
```

---

### 2.2 Options propagation (already in place)

Schema Loader already passes `options` through the full chain:

```
CosmosCommand.call()
  → StorageSpecificCommand.execute(props, options)
    → SchemaParser(tableSchema, options)
      → TableSchema.buildOptions(tableDefinition, options)
        → SchemaOperator.createTable / repairTable
          → CosmosAdmin.createTable(namespace, table, metadata, options)
```

**Per-table overrides:** Schema JSON can include table-level options via `TableSchema.buildOptions` — any key not in `traveledKeys` is passed through. Example schema snippet:

```json
{
  "ns.table_name": {
    "transaction": true,
    "columns": { ... },
    "large_partition_key": "true"
  }
}
```

Document this for users who need V1 on specific tables during transition.

---

### 2.3 Schema Loader modes affected

| Mode | Flag | Effect with V2 changes |
|------|------|------------------------|
| **Create** (default) | none | Creates tables with V2 (if default) |
| **Repair** | `--repair-all` | Creates missing containers with V2; **does not upgrade** existing V1 |
| **Alter** | `--alter` | Unchanged — adds columns/indexes only |
| **Delete** | `--delete-all` | Unchanged |

**Update `--repair-all` description** if needed to clarify it does not upgrade PK version:

> Repairs table metadata and stored procedures. Creates missing containers. Does not change partition key version on existing containers.

---

### 2.4 Schema Loader usage examples

**Create all tables with V2 (after implementation, V2 default):**

```bash
java -jar scalardb-schema-loader-<version>.jar --cosmos \
  -h 'https://<account>.documents.azure.com:443/' \
  -p '<key>' \
  -f schema.json
```

**Explicit opt-in (if V2 is not default):**

```bash
java -jar scalardb-schema-loader-<version>.jar --cosmos \
  -h 'https://<account>.documents.azure.com:443/' \
  -p '<key>' \
  --large-partition-key \
  -f schema.json
```

**Create empty V2 destination for Azure migration:**

```bash
# 1. Add my_table_v2 to schema.json (same columns as my_table)
# 2. Create with V2
java -jar scalardb-schema-loader-<version>.jar --cosmos \
  -h '...' -p '...' -f schema.json

# 3. Azure Portal: copy my_table (V1) → my_table_v2 (V2)
# 4. Repair destination
java -jar scalardb-schema-loader-<version>.jar --cosmos \
  -h '...' -p '...' --repair-all -f schema.json
```

**Opt out of V2 for a specific legacy table (if V2 is default):**

```json
{
  "ns.legacy_table": {
    "large_partition_key": "false",
    "columns": { ... }
  }
}
```

---

### 2.5 Schema Loader tests

**File:** `schema-loader/src/test/java/com/scalar/db/schemaloader/command/CosmosCommandTest.java` (or new)

| Test | Verify |
|------|--------|
| `--large-partition-key` | Options map contains `large_partition_key=true` |
| Default options | Contains expected default when flag omitted |
| Integration (optional) | Create table on emulator; read back V2 version |

---

## Part 3 — File Change Summary

| File | Change type | Description |
|------|-------------|-------------|
| `core/.../CosmosAdmin.java` | **Modify** | V2 in `computeContainerProperties`; `LARGE_PARTITION_KEY` constant; options threading; optional `getPartitionKeyDefinitionVersion` |
| `core/.../CosmosOperationChecker.java` | **Modify** | PK byte length + document id length validation |
| `core/.../CoreError.java` | **Modify** | Two new error codes |
| `core/.../CosmosOperationCheckerTest.java` | **Add/Modify** | Validation unit tests |
| `core/.../CosmosPartitionKeyVersionIntegrationTest.java` | **Modify** | Expect V2 on create; add validation tests |
| `schema-loader/.../CosmosCommand.java` | **Modify** | `--large-partition-key` CLI flag |
| `schema-loader/.../CosmosCommandTest.java` | **Modify** | Flag wiring tests |
| User-facing docs | **Add** | Cosmos storage limits section |

**Files explicitly not changed:**

- `CosmosOperation.java`, `ConcatenationVisitor.java`, `SelectStatementHandler.java`, `mutate.js`
- `CosmosConfig.java`, `CosmosUtils.java`

---

## Part 4 — Implementation Order

```
1. CosmosAdmin — V2 container creation + LARGE_PARTITION_KEY option
2. CoreError — new error codes
3. CosmosOperationChecker — length validation + version lookup
4. Unit tests
5. Integration tests — update expectations
6. Schema Loader — CLI flag
7. Documentation — Schema Loader help + user docs
```

---

## Part 5 — Acceptance Criteria

### Core

- [ ] New tables created via `createTable()` use V2 by default (or when option set)
- [ ] `repairTable()` on existing V1 container does **not** change PK version
- [ ] Put/get/scan with keys ≤ 2048 bytes on V2 succeed
- [ ] Put with PK > 101 bytes on V1 container throws `COSMOS_CONCATENATED_PARTITION_KEY_TOO_LONG`
- [ ] Put with PK > 2048 bytes on V2 container throws same error with V2 limit
- [ ] Put with document id > 255 chars throws `COSMOS_DOCUMENT_ID_TOO_LONG`
- [ ] Existing illegal-character and BIGINT checks unchanged

### Schema Loader

- [ ] `--large-partition-key` flag passes option to `CosmosAdmin`
- [ ] Per-table `large_partition_key` in schema JSON works
- [ ] `--repair-all` deploys stored procedure on portal-migrated V2 containers
- [ ] Help text documents the flag and V1/V2 behavior

---

## Part 6 — Relationship to migration

These changes do **not** perform data migration. They enable:

| Scenario | How |
|----------|-----|
| **New deployments** | Schema Loader creates V2 containers automatically |
| **Existing V1 tables** | Azure Portal container copy (see [migration guide](./cosmos-partition-key-v2-migration-guide.md)) |
| **Post-copy cutover** | Schema Loader `--repair-all` on destination container |
| **Safety during transition** | Validation catches bad keys on both V1 and V2 tables |

---

## Appendix — Constants and limits reference

| Constant | Value | Source |
|----------|-------|--------|
| V1 max PK (UTF-8 bytes) | 101 | [Cosmos limits](https://learn.microsoft.com/en-us/azure/cosmos-db/concepts-limits) |
| V2 max PK (UTF-8 bytes) | 2048 | [Large partition keys](https://learn.microsoft.com/en-us/azure/cosmos-db/large-partition-keys) |
| Max document `id` (chars) | 255 | Cosmos per-item limit |
| Partition key path | `/concatenatedPartitionKey` | `CosmosAdmin.PARTITION_KEY_PATH` |
| Cosmos SDK version | 4.81.0 | `build.gradle` |
