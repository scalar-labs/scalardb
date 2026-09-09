# Running Cosmos Partition Key V1/V2 Integration Tests

This guide describes how to run [`CosmosPartitionKeyVersionIntegrationTest`](../core/src/integration-test/java/com/scalar/db/storage/cosmos/CosmosPartitionKeyVersionIntegrationTest.java) locally. For experiment background and expected outcomes, see [cosmos-partition-key-v2-experiments.md](./cosmos-partition-key-v2-experiments.md).

## What gets tested

The suite runs 11 integration tests covering partition key definition V1 vs V2 behavior (container version, key length boundaries, hash collisions, document `id` limits, `repairTable`, and stored-procedure mutations).

| Requirement | Why |
|-------------|-----|
| Cosmos DB endpoint | Tests create/read containers and records |
| **Strong** consistency | ScalarDB Cosmos adapter only supports Strong and Bounded Staleness |
| Stored procedures | Put/Delete/Mutate use server-side scripts (classic emulator only) |
| Java truststore with emulator cert | HTTPS to `https://localhost:8081/` |

## Prerequisites

- JDK 17+ (project uses Gradle wrapper)
- Docker (for Linux emulator)
- Repository cloned and at the project root

Default emulator primary key (same as CI):

```
C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==
```

---

## Step 1 — Start the Cosmos DB emulator (Linux)

Use the **classic** Linux emulator image. Do **not** use `vnext-latest` — it does not support stored procedures, which ScalarDB requires.

On multi-core hosts, limit CPU to avoid emulator crashes:

```bash
docker rm -f scalardb-cosmos-emulator 2>/dev/null

docker run -d \
  --name scalardb-cosmos-emulator \
  --cpus 2 \
  --cpuset-cpus 0-1 \
  --publish 8081:8081 \
  --publish 10250-10255:10250-10255 \
  --memory 4g \
  --env AZURE_COSMOS_EMULATOR_ARGS="/Consistency=Strong" \
  mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest
```

`AZURE_COSMOS_EMULATOR_ARGS="/Consistency=Strong"` is required because the emulator defaults to Session consistency, which ScalarDB rejects.

Wait until the emulator responds (may take 15–30 seconds):

```bash
curl -k https://localhost:8081/_explorer/emulator.pem -o /tmp/cosmos-emulator.pem
openssl x509 -in /tmp/cosmos-emulator.pem -noout -subject
# Expected: subject=CN = localhost
```

### Windows

```powershell
Import-Module "$env:ProgramFiles\Azure Cosmos DB Emulator\PSModules\Microsoft.Azure.CosmosDB.Emulator"
Start-CosmosDbEmulator
```

See also [`ci/tests-config.yaml`](../ci/tests-config.yaml) for the CI emulator + cert setup on Windows.

---

## Step 2 — Trust the emulator SSL certificate (Linux)

Java must trust the emulator’s self-signed certificate. Create a truststore once per emulator start (the cert changes when the container is recreated):

```bash
curl -k https://localhost:8081/_explorer/emulator.pem -o /tmp/cosmos-emulator.pem
rm -f /tmp/cosmos-truststore.jks
keytool -importcert \
  -alias cosmos-emulator \
  -file /tmp/cosmos-emulator.pem \
  -keystore /tmp/cosmos-truststore.jks \
  -storepass changeit \
  -noprompt
```

Export this before running Gradle:

```bash
export JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=/tmp/cosmos-truststore.jks -Djavax.net.ssl.trustStorePassword=changeit"
```

If you restart or recreate the emulator container, repeat this step.

---

## Step 3 — Compile the integration tests

From the repository root:

```bash
./gradlew :core:compileIntegrationTestCosmosJava
```

---

## Step 4 — Run the partition key test suite

```bash
./gradlew integrationTestCosmos \
  -Dscalardb.cosmos.uri=https://localhost:8081/ \
  -Dscalardb.cosmos.password='C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==' \
  -Dfile.encoding=UTF-8 \
  --tests 'com.scalar.db.storage.cosmos.CosmosPartitionKeyVersionIntegrationTest'
```

Optional: override provisioned throughput (default is `ru:10000`):

```bash
-Dscalardb.cosmos.create_options='ru:10000'
```

### View results

- HTML report: `core/build/reports/tests/integrationTestCosmos/index.html`
- JUnit XML: `core/build/test-results/integrationTestCosmos/`

---

## Step 5 — Sanity check (optional)

To verify the emulator and Gradle wiring before running the full suite:

```bash
./gradlew integrationTestCosmos \
  -Dscalardb.cosmos.uri=https://localhost:8081/ \
  -Dscalardb.cosmos.password='C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==' \
  -Dfile.encoding=UTF-8 \
  --tests 'com.scalar.db.storage.cosmos.CosmosSinglePartitionKeyIntegrationTest'
```

---

## Using a real Azure Cosmos DB account

Replace the URI and key with your account values:

```bash
./gradlew integrationTestCosmos \
  -Dscalardb.cosmos.uri='https://<account>.documents.azure.com:443/' \
  -Dscalardb.cosmos.password='<primary-or-secondary-key>' \
  -Dfile.encoding=UTF-8 \
  --tests 'com.scalar.db.storage.cosmos.CosmosPartitionKeyVersionIntegrationTest'
```

No custom truststore is needed for Azure-hosted accounts. Ensure the account uses **Strong** (or Bounded Staleness) consistency.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `Connection refused` on port 8081 | Emulator not running or still starting | Check `docker ps`; wait and retry Step 1 |
| `SignatureException` / SSL errors | Stale or missing truststore | Re-run Step 2 after emulator (re)start |
| `Only STRONG and BOUNDED_STALENESS are supported` | Session consistency | Start emulator with `AZURE_COSMOS_EMULATOR_ARGS="/Consistency=Strong"` |
| Emulator container exits with crash dump | Too many CPUs on Linux | Use `--cpus 2 --cpuset-cpus 0-1` (Step 1) |
| `Server-side script execution is not supported` | vNext emulator | Switch to classic `azure-cosmos-emulator:latest` |
| `UnknownHostException: localhost-southcentralus` | Emulator overloaded or crashed | Restart emulator; optionally add `127.0.0.1 localhost-southcentralus` to `/etc/hosts` |
| `Repairing the table failed` / long hangs (~150s) | Emulator under load | Restart emulator; run tests on a fresh container |
| `The namespace has non-ScalarDB tables and cannot be dropped` | Prior run left orphaned containers | Restart emulator, or delete containers in namespace `int_test_pk_version` manually |
| Tests pass but Exp 5B is lenient | Emulator may not enforce 255-char document `id` limit | Expected on emulator; validate Exp 5B against a real Azure account |

---

## Stop the emulator

```bash
docker stop scalardb-cosmos-emulator
docker rm scalardb-cosmos-emulator
```

---

## System properties reference

Wired through [`CosmosEnv`](../core/src/integration-test/java/com/scalar/db/storage/cosmos/CosmosEnv.java):

| Property | Required | Description |
|----------|----------|-------------|
| `scalardb.cosmos.uri` | Yes | Cosmos endpoint (e.g. `https://localhost:8081/`) |
| `scalardb.cosmos.password` | Yes | Account primary key |
| `scalardb.cosmos.create_options` | No | Table creation options; default `ru:10000` |

The test class uses namespace `int_test_pk_version` and an isolated metadata database suffix (`pk_version`).
