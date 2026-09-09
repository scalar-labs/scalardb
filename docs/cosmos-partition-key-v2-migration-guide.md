# Cosmos DB Partition Key V1 → V2 — Migration Guide for ScalarDB Users

This guide explains how to migrate existing ScalarDB tables from Cosmos DB **V1** (101-byte hash) to **V2** (large partition keys, up to 2048 bytes). It incorporates Azure’s managed **Change partition key** / **container copy** feature and ScalarDB-specific cutover steps.

**Related documents:**

- [Investigation report](./cosmos-partition-key-length.md) — background and recommended product changes
- [Experiment guide](./cosmos-partition-key-v2-experiments.md) — technical validation
- [Test run guide](./cosmos-partition-key-v2-test-run.md) — how to run integration tests

---

## Terminology mapping

| Azure Cosmos DB | ScalarDB |
|-----------------|----------|
| Account | Connection endpoint (`scalar.db.contact_points`) |
| Database | **Namespace** (e.g. `ns`) |
| Container | **Table** (e.g. `users`) |
| Partition key path | Always `/concatenatedPartitionKey` (do **not** change this) |
| Partition key version | V1 (101-byte hash) or V2 (2048-byte hash) — **this is what you migrate** |

ScalarDB builds the partition key value by colon-joining your logical partition-key columns into `concatenatedPartitionKey`. The **path stays the same**; only the **hash version** changes from V1 to V2.

---

## Can Azure’s portal migration replace a custom ScalarDB tool?

**Partially yes.** Azure provides a managed migration path that significantly reduces the need for a custom bulk-copy tool in many deployments.

| Approach | When to use |
|----------|-------------|
| **Azure Portal — Change partition key** | Lowest effort; container &lt; 4 TB, &lt; 1,000,000 RU/s, supported region |
| **Azure CLI — container copy jobs** (`cosmosdb-preview` extension) | Same engine as portal; scriptable / CI-friendly |
| **Custom bulk copy (Cosmos SDK, ADF, Spark)** | Above Azure limits, transforms needed, or unsupported account capabilities |
| **ScalarDB-built migration command** | Optional convenience wrapper; **not strictly required** if Azure copy jobs meet your constraints |

**What Azure’s feature handles:**

- Creates a destination container in the same database
- Copies all documents (online or offline mode)
- Replicates indexing policy, throughput settings, etc.

**What ScalarDB still must handle after Azure copy:**

- ScalarDB **table metadata** (stored in Cosmos metadata containers)
- **Stored procedure** (`mutate.js`) on the destination container
- Application **cutover** to the new container name (if changed)
- **Validation** that partition keys and document `id` values are within limits

References:

- [Change partition key (Azure docs)](https://learn.microsoft.com/en-us/azure/cosmos-db/change-partition-key)
- [Large partition keys](https://learn.microsoft.com/en-us/azure/cosmos-db/large-partition-keys)
- [Container copy](https://learn.microsoft.com/en-us/azure/cosmos-db/container-copy)

---

## Prerequisites

### Before you start

- [ ] Confirm the table was created by ScalarDB (V1 container at `/concatenatedPartitionKey`)
- [ ] Assess migration need (see [When to migrate](#when-to-migrate))
- [ ] Verify Azure account/region supports **Change partition key** ([supported regions](https://learn.microsoft.com/en-us/azure/cosmos-db/change-partition-key))
- [ ] Confirm container is under **4 TB** and **1,000,000 RU/s** (contact Microsoft Support if above)
- [ ] Confirm account does **not** have unsupported capabilities (e.g. Merge partition)
- [ ] Plan maintenance window (recommended even for online mode)
- [ ] Backup / export critical data if required by your policy

### ScalarDB / SDK requirements

| Component | Requirement |
|-----------|-------------|
| ScalarDB Cosmos adapter | Java v4 Azure Cosmos SDK (`azure-cosmos` **4.81.0** in current ScalarDB) — **supports V2 reads/writes** once containers are V2 |
| Application change for V2 | **No SDK upgrade needed** for Java v4; ScalarDB must create/repair destination containers with V2 (future product change) |
| Stored procedures | Destination container must have ScalarDB’s `mutate.js` — run `repairTable` after cutover |

> **Note:** Older SDK generations (.NET V2, Java sync/async 2.x without V2 support) must not be used against V2 containers. ScalarDB’s current Java v4 dependency is sufficient.

---

## When to migrate

| Scenario | Urgency |
|----------|---------|
| Partition keys regularly **> 101 UTF-8 bytes** | **High** — silent V1 hash collisions possible |
| Keys with **shared prefixes** (URLs, tenant IDs, natural keys) | **High** — collision risk |
| **BLOB** partition keys (~76+ raw bytes → 101+ Base64 chars) | **High** |
| All keys **well under 101 bytes**, unique prefixes | **Low** — migration optional but V2 still recommended for new tables |
| Document `id` (PK + CK) approaching **255 characters** | Fix schema/key design first — V2 does **not** raise the `id` limit |

---

## Migration overview

```
┌──────────────────────────────────────────────────────────────────────┐
│ Phase 0 — Prepare                                                    │
│   Assess risk · Choose method · Schedule window                      │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Phase 1 — Azure data migration (Portal or CLI)                       │
│   V1 source container  ──copy──►  V2 destination container           │
│   Same path: /concatenatedPartitionKey                               │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Phase 2 — ScalarDB cutover                                           │
│   repairTable · metadata sync · stored procedure · app switch        │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Phase 3 — Verify & decommission                                      │
│   Row counts · sample reads · drop old V1 container                  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Method A — Azure Portal (recommended for most users)

### Step 1 — Open the source container

1. Sign in to the [Azure Portal](https://portal.azure.com).
2. Open your **Azure Cosmos DB account**.
3. Go to **Data Explorer**.
4. Expand the database that matches your ScalarDB **namespace**.
5. Select the **container** that matches your ScalarDB **table**.

### Step 2 — Start partition key change

1. Open **Scale & Settings**.
2. Select the **Partition Keys** tab.
3. Click **Change**.

### Step 3 — Configure the destination container

1. **Destination:** Choose **Create a new container** (recommended) in the same database.
   - Azure copies most settings from the source automatically.
2. **Partition key path:** Keep **`/concatenatedPartitionKey`** — same as ScalarDB uses. **Do not change the path.**
3. **Partition key version:** Enable **V2 / large partition keys** on the destination.
4. **Destination name:** Choose a temporary name, e.g. `my_table_v2` (original: `my_table`).

> If the portal does not expose an explicit “V2” toggle, ensure the destination is created with large partition key support. Portal-created containers default to V2 per Microsoft documentation.

### Step 4 — Choose migration mode

| Mode | Use when |
|------|----------|
| **Offline** | You can stop all writes to the table during migration (simplest) |
| **Online** | Application must keep writing during copy; requires a finalization step |

**Offline (recommended for ScalarDB):**

1. **Stop all application writes** to the ScalarDB table (reads may continue).
2. Select **Offline** mode in the wizard.
3. Start the copy job.

**Online:**

1. Select **Online** mode.
2. Application continues writing to the **source (V1)** container during copy.
3. When processed document count nearly matches the source, **stop writes** temporarily.
4. Click **Complete** in the portal to flush remaining changes.
5. Proceed to cutover.

### Step 5 — Monitor the copy job

1. Monitor progress and document count in **Data Explorer**.
2. Wait until the job reports **Completed**.
3. Optionally compare document counts: source vs destination.

### Step 6 — ScalarDB cutover

Azure leaves you with two containers. ScalarDB expects the **table name** to match the container name.

**Option 6A — Keep original table name (recommended)**

1. **Stop all traffic** to the table.
2. **Verify** destination data (see [Verification](#verification-checklist)).
3. **Delete** the old V1 container (`my_table`) via Portal or ScalarDB `dropTable`.
4. **Rename strategy:** Cosmos DB does not support in-place rename. Either:
   - **Portal/CLI:** Create a second copy from `my_table_v2` → `my_table` (if you deleted the old one first), **or**
   - **Simpler:** Use Schema Loader / `repairTable` to create a new empty V2 container named `my_table`, then run a second Azure copy from `my_table_v2` → `my_table`.
5. Run ScalarDB **repair** on the final container:

   ```bash
   java -jar scalardb-schema-loader-<version>.jar --cosmos \
     -h 'https://<account>.documents.azure.com:443/' \
     -p '<key>' \
     --repair-all \
     -f schema.json
   ```

   Or use the ScalarDB Admin API: `admin.repairTable(namespace, table, metadata, options)`.

   This ensures:
   - ScalarDB metadata is correct
   - Stored procedure `mutate.js` is deployed
   - Indexing policy matches your schema

6. **Drop** the temporary container (`my_table_v2`) after verification.

**Option 6B — Adopt new table name (application change)**

1. Update application config / schema to use `my_table_v2` instead of `my_table`.
2. Run `repairTable` for `my_table_v2`.
3. Delete old `my_table` when satisfied.

### Step 7 — Resume traffic

1. Restart application against the V2 container.
2. Monitor errors, RU consumption, and latency.
3. Run smoke tests (get, scan, put, conditional mutations).

### Step 8 — Decommission

1. After a retention period (e.g. 7–14 days), delete the old V1 container if still present.
2. Document the migration completion date and new container version.

---

## Method B — Azure CLI (container copy jobs)

Use this for automation or when the portal is unavailable.

1. Install the Azure CLI `cosmosdb-preview` extension.
2. Create a V2 destination container with path `/concatenatedPartitionKey`.
3. Create and monitor a container copy job (online or offline).
4. Follow [Step 6 — ScalarDB cutover](#step-6--scalardb-cutover) above.

See: [Container copy documentation](https://learn.microsoft.com/en-us/azure/cosmos-db/container-copy)

---

## Method C — Manual copy (fallback)

Use when Azure copy jobs are unavailable (size limits, region, account capabilities).

1. Stop writes.
2. Bulk-read all documents from the V1 container (Cosmos SDK, Azure Data Factory, Spark, etc.).
3. Create a V2 container (via future Schema Loader `--large-partition-key` or direct SDK).
4. Bulk-write documents to the V2 container.
5. Follow ScalarDB cutover steps above.

---

## Verification checklist

After migration, verify:

- [ ] Document count matches (source vs destination) within expected delta
- [ ] `PartitionKeyDefinitionVersion.V2` on destination container (Data Explorer or SDK)
- [ ] Partition key path is still `/concatenatedPartitionKey`
- [ ] ScalarDB `get` / `scan` / `put` / conditional put / delete work
- [ ] Stored procedure `mutate.js` exists on the container
- [ ] Sample records with **long partition keys** (>101 bytes) read back correctly
- [ ] If applicable: keys that **collided on V1** are now in separate physical partitions (optional Cosmos diagnostic)
- [ ] Document `id` values are all ≤ 255 characters
- [ ] Secondary indexes behave as expected

---

## Azure platform limitations

| Limitation | Detail |
|------------|--------|
| Data size | &lt; **4 TB** per container (contact Microsoft Support if larger) |
| Throughput | &lt; **1,000,000 RU/s** provisioned (contact Support if higher) |
| Regions | Feature available only in [documented regions](https://learn.microsoft.com/en-us/azure/cosmos-db/change-partition-key) |
| Account capabilities | Not supported with some capabilities (e.g. **Merge partition**) |
| In-place upgrade | **Not possible** — always requires a destination container + copy |

---

## What ScalarDB product changes still need (developer checklist)

Azure’s portal migration **reduces** custom migration tooling but does **not** replace these ScalarDB changes:

### Required for new deployments

| # | Change | Owner |
|---|--------|-------|
| 1 | Set `PartitionKeyDefinitionVersion.V2` in `CosmosAdmin.computeContainerProperties()` | Core adapter |
| 2 | Add partition key length validation in `CosmosOperationChecker` (101 V1 / 2048 V2) | Core adapter |
| 3 | Add document `id` length validation (255 chars) | Core adapter |
| 4 | Add `CoreError` code for length violations | Core adapter |

### Required for Schema Loader users

| # | Change | Owner |
|---|--------|-------|
| 5 | Add `--large-partition-key` flag to `CosmosCommand` (if V2 is opt-in) | Schema Loader |
| 6 | Document Schema Loader + migration workflow | Docs |

### Reduced scope (thanks to Azure portal)

| # | Original plan | Revised plan |
|---|---------------|--------------|
| 7 | Build custom bulk migration tool | **Optional** — use Azure Portal / CLI container copy for most users |
| 8 | Dual-write migration framework | **Optional** — Azure online mode covers incremental sync |

### Still required for migration users

| # | Change | Owner |
|---|--------|-------|
| 9 | Post-migration `repairTable` runbook (this document) | Docs |
| 10 | Pre-migration assessment guide (key length, collision risk) | Docs |
| 11 | User-facing Cosmos storage docs (101 / 2048 byte limits) | Docs |

### Optional enhancements

| # | Change | Benefit |
|---|--------|---------|
| 12 | Schema Loader subcommand wrapping Azure CLI copy + repair | Single-command migration |
| 13 | Admin API to read/report container PK version per table | Operational visibility |
| 14 | Warn at startup if V1 container detected | Proactive alerting |

---

## FAQ

### Does migration change my ScalarDB schema?

No. Column names, types, and partition-key column definitions stay the same. Only the Cosmos container’s hash version changes.

### Is the full partition key still stored after migration?

Yes. Documents still contain the complete `concatenatedPartitionKey` string and individual `partitionKey` columns. V2 only changes how Cosmos **routes** keys to physical partitions.

### Can I use `repairTable()` instead of migrating?

No. `repairTable()` fixes ScalarDB metadata and indexing policy but **does not** upgrade V1 → V2 on an existing container.

### Do I need a new Cosmos DB account?

No. Migration happens within the same account and namespace (database).

### Will migration fix document `id` values longer than 255 characters?

No. If existing records violate the 255-character `id` limit, fix key design before or during migration.

---

## Rollback plan

1. Keep the V1 source container read-only until V2 is verified (do not delete immediately).
2. If V2 cutover fails, point the application back to the V1 container.
3. Delete the failed V2 destination container and retry after fixing the issue.

---

## Summary

| Question | Answer |
|----------|--------|
| Can we use Azure Portal migration? | **Yes**, for most deployments under Azure size/region limits |
| Do we still need ScalarDB code changes? | **Yes** — V2 for new tables, validation, Schema Loader, docs |
| Do we need a custom copy tool? | **Usually no** — Azure container copy handles data movement |
| What is ScalarDB-specific? | Metadata sync, stored procedure, table name cutover, validation |
