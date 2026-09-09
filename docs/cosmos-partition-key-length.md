# Cosmos DB Partition Key Length Constraints in ScalarDB

## TL;DR

- Azure Cosmos DB imposes a length constraint on partition key values: **101 bytes** by default (V1 hash), or **2048 bytes** if large partition keys (V2 hash) are enabled at container creation time.
- ScalarDB's Cosmos adapter concatenates all partition-key columns into a single string and uses it as the Cosmos partition key value.
- The Java v4 Cosmos SDK (`com.azure:azure-cosmos`, currently pinned to `4.81.0`) does **not** enable large partition keys unless the user explicitly sets `PartitionKeyDefinitionVersion.V2`. ScalarDB does not set it, so **every container ScalarDB creates uses V1 (101-byte) hashing**.
- ScalarDB does **not** validate the length of the concatenated partition key at all. As a result, V1's 101-byte limit produces silent hash collisions (writes succeed, but data-integrity issues occur), and V2's 2048-byte limit — should a user manually enable V2 — surfaces as a raw Cosmos `BadRequest` without a ScalarDB-friendly error.

---

## Background: Cosmos DB Partition Key Length

Azure Cosmos DB uses a hash-based partitioning scheme. Two hash functions exist:

| Version | Bytes hashed | Behavior when value exceeds the limit |
|---|---|---|
| **V1** (default for pre-2019-05-03 containers, and for containers created by SDKs that don't opt in to V2) | first 101 bytes | Writes still succeed, but any two values sharing the first 101 bytes are treated as the **same logical partition**, causing: hash collisions, incorrect partition size quota accounting, incorrect enforcement of unique indexes, and uneven storage distribution. |
| **V2** (large partition keys) | full value up to 2048 bytes | Values exceeding 2048 bytes are **rejected** by the service as `BadRequest`. The full value is used for hashing, so no silent collisions occur. |

Sources:

- [Create containers with large partition key](https://learn.microsoft.com/en-us/azure/cosmos-db/large-partition-keys)
- [Service quotas and default limits](https://learn.microsoft.com/en-us/azure/cosmos-db/concepts-limits) — the per-item limits table states: *"Maximum length of partition key value: 2,048 bytes (101 bytes if large partition-key isn't enabled)."*

Large partition keys can only be enabled at container-creation time; existing containers cannot be upgraded in place.

## Which SDKs default to V2?

According to Microsoft documentation:

- **Azure Portal**: containers created via the portal default to V2.
- **.NET SDK V3**: defaults to V2.
- **.NET SDK V2**: defaults to V1; V2 must be opted in explicitly.
- **Java v4 SDK** (`com.azure:azure-cosmos`): not explicitly discussed in the doc. The Microsoft doc's "Supported SDK versions" table lists Java Sync 2.4.0 and Java Async 2.5.0 as the minimum versions with V2 support, but no version is called out as auto-defaulting to V2.

Inspection of the Java v4 SDK source (`azure-cosmos-4.81.0`) confirms:

1. `PartitionKeyDefinition()` constructor sets `kind=HASH` only; `versionOptional` stays `null`.
2. `CosmosContainerProperties(id, partitionKeyPath)` creates a bare `PartitionKeyDefinition` with `paths` and `kind` only — no version.
3. `PartitionKeyDefinition.populatePropertyBag()` omits the `version` JSON field when `versionOptional` is null or empty, so the version is not sent on the wire.
4. `ModelBridgeInternal.isV2()` treats an unset version as not-V2.

The service-side default when no version is specified is V1 (for backward compatibility). Therefore the Java v4 SDK produces V1 containers unless the caller explicitly sets `PartitionKeyDefinitionVersion.V2`.

The doc statement "All Azure Cosmos DB containers created before May 3, 2019 use a hash function that computes hash based on the first 101 bytes" refers to when V2 became available as an opt-in feature, not to a change in the default. The default did not flip.

## Current ScalarDB Behavior

### Container creation ([`CosmosAdmin`](../../core/src/main/java/com/scalar/db/storage/cosmos/CosmosAdmin.java))

`CosmosAdmin.computeContainerProperties()` uses:

```java
return new CosmosContainerProperties(table, PARTITION_KEY_PATH)
    .setIndexingPolicy(indexingPolicy);
```

No `PartitionKeyDefinitionVersion` is specified, so the resulting Cosmos container is created with V1 hashing (101-byte limit).

### Partition key value assembly ([`CosmosOperation`](../../core/src/main/java/com/scalar/db/storage/cosmos/CosmosOperation.java), [`ConcatenationVisitor`](../../core/src/main/java/com/scalar/db/storage/cosmos/ConcatenationVisitor.java))

When a Cosmos operation runs, `CosmosOperation.getConcatenatedPartitionKey()` walks the partition-key columns in metadata order and concatenates their string representations with `:` as a separator:

- Numeric / boolean types: `String.valueOf(...)`
- TEXT: raw UTF-8 (colons rejected upstream by `CosmosOperationChecker`)
- BLOB: URL-safe Base64 without padding (~1.33× byte inflation)
- Date / Time / Timestamp / TimestampTZ: encoded as numeric epoch values

The resulting string becomes the Cosmos partition key value.

### Validation ([`CosmosOperationChecker`](../../core/src/main/java/com/scalar/db/storage/cosmos/CosmosOperationChecker.java))

`CosmosOperationChecker` performs the following primary-key checks:

- Illegal-character rejection for TEXT columns (`:` `/` `\` `#` `?`)
- BIGINT range check (±2^53)

It performs **no length check** on individual partition-key column values, nor on the concatenated result.

### Schema Loader ([`CosmosCommand`](../../schema-loader/src/main/java/com/scalar/db/schemaloader/command/CosmosCommand.java))

The Cosmos subcommand accepts only:

- `-h/--host` (URI)
- `-p/--password` (key)
- `-r/--ru` (request units)
- `--no-scaling` (disable autoscale)
- `-D/--delete-all`, `--repair-all`, `-A/--alter` (mode flags)

There is no option to enable large partition keys.

## Issues

### 1. Silent partition-key hash collisions (V1 limit)

Because all containers ScalarDB creates use V1 (101-byte) hashing, any two records whose concatenated partition keys share the same first 101 bytes end up in the same logical partition. This causes:

- Two logically distinct partition keys to be treated as one partition, silently.
- Partition storage-quota accounting (20 GB per logical partition) to be measured against the collision-inflated partition rather than the intended one.
- Unique-index enforcement to be applied across values the user considers distinct.
- Uneven storage distribution.

No error surfaces at any layer — the writes succeed, and the problem manifests only later as capacity or consistency anomalies.

Realistic collision scenarios in ScalarDB:

- Long TEXT columns as partition key (e.g., URL, JSON fragment, natural key with a shared prefix).
- BLOB columns as partition key: Base64 encoding inflates each 3 raw bytes to 4 Base64 characters, so raw BLOB values as short as 76 bytes cross the 101-byte threshold on their own.
- Composite partition keys where one component dominates the length budget (e.g., `tenant-long-name:short-user-id`).

### 2. No length pre-check for V2 either

Even if a user manually recreates a container as V2, ScalarDB does not validate the 2048-byte limit. Attempts to write partition keys longer than 2048 bytes surface as a raw Cosmos `BadRequest`, which references the internal `concatenatedPartitionKey` property rather than the user-supplied ScalarDB columns.

### 3. No Schema Loader option for large partition keys

Even users who understand the risk have no first-class way to create ScalarDB tables with large partition keys enabled. Manual container creation (e.g., via the portal) is an out-of-band workaround that bypasses the Schema Loader entirely.

### 4. No documentation of the limit

The 101-byte constraint and its silent-collision behavior are not mentioned in ScalarDB's Cosmos-specific documentation. Users cannot anticipate this limit until they hit it.

## Recommended Actions

Options, in decreasing order of impact and complexity:

### A. Enable V2 by default for newly created containers

Modify `CosmosAdmin.computeContainerProperties()` to set `PartitionKeyDefinitionVersion.V2` explicitly:

```java
PartitionKeyDefinition partitionKeyDefinition =
    new PartitionKeyDefinition()
        .setKind(PartitionKind.HASH)
        .setPaths(Collections.singletonList(PARTITION_KEY_PATH))
        .setVersion(PartitionKeyDefinitionVersion.V2);
return new CosmosContainerProperties(table, partitionKeyDefinition)
    .setIndexingPolicy(indexingPolicy);
```

Trade-offs:

- **Existing containers are unaffected.** V1 vs V2 is fixed at container creation. Users on existing deployments would need to run a container-copy migration to benefit.
- Since Microsoft recommends V2 for any new container and Portal-created containers already default to V2, aligning ScalarDB with V2 by default matches ecosystem best practice.

### B. Add pre-write length validation in `CosmosOperationChecker`

Reject partition keys whose concatenated form exceeds the configured limit (101 for V1, 2048 for V2), with a ScalarDB-native error such as `COSMOS_CONCATENATED_PARTITION_KEY_TOO_LONG`.

This turns silent V1 collisions and opaque V2 `BadRequest`s into explicit, actionable errors. Ideally the check knows which version the container was created with (readable from `CosmosContainerProperties.getPartitionKeyDefinition().getVersion()` at metadata-load time).

### C. Expose Large Partition Key as a Schema Loader option

If a hard default change is deemed too disruptive, add an opt-in option to `CosmosCommand` and pipe it through to `CosmosAdmin.createTable(... , options)` — for example `--large-partition-key`.

### D. Document the constraint

Regardless of which of A/B/C are pursued, add an explicit section to the Cosmos storage docs describing:

- The 101-byte (V1) / 2048-byte (V2) limit.
- That ScalarDB concatenates partition-key columns with `:` and encodes BLOBs as Base64, so the effective raw-value budget is smaller than 101 bytes for composite or BLOB keys.
- Guidance for users who need to work with long partition keys.

## Appendix: Code References

| Concern | File | Key symbol |
|---|---|---|
| Container creation | [core/.../CosmosAdmin.java](../../core/src/main/java/com/scalar/db/storage/cosmos/CosmosAdmin.java) | `computeContainerProperties()`, `PARTITION_KEY_PATH` |
| Partition key concatenation | [core/.../CosmosOperation.java](../../core/src/main/java/com/scalar/db/storage/cosmos/CosmosOperation.java) | `getConcatenatedPartitionKey()` |
| Column-to-string encoding | [core/.../ConcatenationVisitor.java](../../core/src/main/java/com/scalar/db/storage/cosmos/ConcatenationVisitor.java) | `build()`, per-type `visit(...)` |
| Primary-key validation | [core/.../CosmosOperationChecker.java](../../core/src/main/java/com/scalar/db/storage/cosmos/CosmosOperationChecker.java) | `PRIMARY_KEY_COLUMN_CHECKER` |
| Cosmos error codes | [core/.../CoreError.java](../../core/src/main/java/com/scalar/db/common/CoreError.java) | `COSMOS_*` |
| Schema Loader Cosmos command | [schema-loader/.../CosmosCommand.java](../../schema-loader/src/main/java/com/scalar/db/schemaloader/command/CosmosCommand.java) | `call()` |
| Cosmos SDK version | [build.gradle](../../build.gradle) | `azureCosmosVersion = '4.81.0'` |
