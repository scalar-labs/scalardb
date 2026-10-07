# PostgreSQL Frontend: Integration with ScalarDB Cluster

Status: draft, 2026-10-03. Nothing implemented yet. Related: `postgres-frontend/` (the module), `scan-read-your-writes-design.md`, `text-collation-design.md`.

## Problem

The frontend in `postgres-frontend/` speaks the PostgreSQL wire protocol and plans SQL into ScalarDB operations. Today it is a standalone jar that embeds the core library: `PostgresServer.main` builds a `TransactionFactory` from a properties file and serves one thread per connection. It has no authentication, no TLS, and no relation to ScalarDB Cluster.

The question is how it should be deployed in a real system. Two shapes were on the table: integrate it into ScalarDB Cluster, or run it as a sidecar container next to each application.

## Background

### What the frontend needs from ScalarDB

Only the public API. The imports under `postgres-frontend/src/main/java` are `DistributedTransactionManager` (`begin`, `beginReadOnly`, one-shot CRUD, `getScanner`), `DistributedTransactionAdmin` (table metadata for planning and for the `pg_catalog` emulation, DDL), `TransactionFactory`, and the `api`, `io`, and `exception` types. Nothing from Consensus Commit internals or from a storage adapter.

### What the cluster already provides

Facts from `~/git/scalardb-cluster` at b5d85887 (2026-08-04).

- **Client SDK** (`client` module). `ClusterClientTransactionManager` implements `DistributedTransactionManager`, including `beginReadOnly` and `getScanner`. A scanner is `createScanner`, then `fetchFromScanner` pages of `scan_fetch_size`, then `closeScanner`, each a gRPC call. The frontend jar therefore runs against a cluster unchanged, with `scalar.db.transaction_manager=cluster` and contact points.
- **Node** (`node` module). `ClusterNodeServer` starts the gRPC server on `scalar.db.cluster.node.port` (60053) and, when `scalar.db.graphql.enabled`, an in-process HTTP `GraphQlServer`. SQL is enabled by `scalar.db.sql.enabled`; `SqlServerUtils` adds gRPC services backed by `scalardb-sql-direct-mode`, so SQL text is parsed and planned on the node. `ClusterNodeManagerForClusterNode` builds the local manager chain: `TransactionFactory.create(properties)` wrapped in `AuthDistributedTransactionManager` (when `scalar.db.cluster.auth.enabled`) and `ScannerManagedTransactionManager`, plus gate keeping and instrumentation. The local gRPC services call that chain.
- **Auth** (`auth` module). `AuthService.login(username, password)` returns a token. `AuthDistributedTransactionManager` checks the caller on every begin, reading the token from a thread-local in `AuthExecutionUtils`; the gRPC services set it from request metadata. `GraphQlServer` refuses to start when auth is enabled, so the per-connection identity plumbing is the part that integration has skipped before.
- **Routing.** Every gRPC request carries a transaction ID. `ClusterRequestRouter.route(txId)` maps it to a node by consistent hashing over the membership (`ConsistentHashingDistribution`); a request without a transaction goes to `routeNearest()` (round-robin). A node that receives a request it does not own forwards it, see `SqlTransactionGrpcService.perform`, under `scalar.db.cluster.hop_limit` (default 3). Client modes: `indirect` (one endpoint, nodes forward) and `direct-kubernetes` (the SDK reads pod addresses from the Kubernetes API and routes itself).
- **TLS.** `scalar.db.cluster.tls.enabled` and certificate settings cover gRPC.

### Why the cluster forwards

A transaction's state (snapshot, read set, buffered writes, open scanners) is a Java object on the node that began it; nothing is externalized before commit. gRPC calls are independent requests: behind one endpoint an HTTP-aware proxy balances per request, and a layer 4 one spreads reconnects and channels across pods. So the receiving node is often not the owner, and ownership has to be computable from the ID alone. The only things that strictly need ID-based routing are accesses to a transaction from another connection or process: join, resume, and two-phase commit participants. Core 4.0 removed join and resume (#3889) and the two-phase commit interface (#3872).

### Measured cost structure

From `postgres-frontend/bench`. The frontend's CPU per statement is 80 to 120 µs with the parse and plan caches warm; round trips dominate. Per ScalarDB operation against a JDBC backend: Get 1 round trip, scan 3 to 4 (begin and execute, end-of-cursor fetch, commit), update 2 (pre-read plus conditional write). At an effective 0.5 ms RTT, the frontend co-located with the application is within 1.1 to 1.4x of native PostgreSQL on single-operation statements and transactions; a frontend one hop away adds one RTT per statement. TPC-C at 0.68 ms RTT: native 327 tps, frontend co-located 144, frontend one hop away 101.

## Options

Hops are counted as application to frontend, frontend to cluster node, and node to database.

| | A. Sidecar, core library | B. Sidecar, cluster client | C. In-node listener |
|---|---|---|---|
| Where the frontend runs | a process in the app pod | a process in the app pod | a thread in the cluster node |
| Code needed | none (today's jar) | none (properties) | listener wiring, auth, TLS, module placement |
| Network hops per SQL statement | 0 (localhost) | 0 (localhost) | 1 (app to node) |
| Network hops per ScalarDB operation | 1 (to the database) | 2 (to the node, then to the database); a scanner is at least 3 calls to the node | 1 (to the database) |
| Database credentials and connection pools | in every app pod | in the cluster nodes | in the cluster nodes |
| Auth, ABAC, encryption, metering | bypassed | through the SDK's single login: one identity per sidecar unless credentials are mapped per connection | through the node's decorators, per PostgreSQL user |
| JVMs and warm caches | one per app pod | one per app pod | one per node |
| Routing | none | SDK: hash and forward, or direct-kubernetes | none; the connection pins the node |

A fourth shape, a standalone frontend Deployment in front of the cluster, is B with one more hop per statement and no advantage over C except not touching the node.

- **Latency.** C pays one in-cluster RTT per statement and the database RTTs per operation. B pays an in-cluster RTT per operation on top of the database RTTs, and inside a transaction those are sequential, so a join with ten lookups costs twenty hops before the database is counted; a scanner costs several calls per scan. A pays the least, but only because it bypasses the cluster.
- **Operations.** A and B multiply JVMs with application pods and warm the parse and plan caches per pod; A also puts database credentials and a backend connection pool in every pod. C keeps one JVM and one pool per node.
- **Routing.** A PostgreSQL session is one TCP connection with a strictly sequential protocol and no request IDs, so a transaction's statements cannot be spread across nodes. With C the transaction's home is settled by the socket. Hashing, forwarding, and the direct-kubernetes mode are unused on this path.
- **Product.** A gives a PostgreSQL-compatible ScalarDB to anyone with the open-source core library, without cluster features. Whether that should exist is tied to where the module lives (see Module placement).

## Decision

Integrate the frontend as an in-node listener (C). Keep the standalone jar as a deployment variant for development, non-Kubernetes installs, and core-library users (A). Run B once as a contract check before writing node code (Plan, phase 0); it is not part of the integration.

## Design: in-node listener

### Wiring

- Properties, following `scalar.db.graphql.*` and `scalar.db.sql.enabled`: `scalar.db.postgres.enabled` (default false), `scalar.db.postgres.port` (default 5432), `scalar.db.postgres.max_connections`.
- `ClusterNodeServer` starts `PostgresServer` after the gRPC server, passing the node's decorated `DistributedTransactionManager` and `DistributedTransactionAdmin` from `ClusterNodeManagerForClusterNode`, the same objects the local gRPC services use, and the node's `Metrics`.
- Stop order on decommissioning: stop accepting, wait up to `scalar.db.cluster.node.decommissioning_duration_secs` for open transactions, roll back the rest, close sockets. GraphQL overrides its own duration to 0 and relies on the node's; do the same.
- Caches: the parse cache is already shared. The plan cache is per session today (`QueryProcessor.planCache`); make it per node, keyed by namespace and SQL text and cleared on DDL, so it warms once per node rather than once per connection.

### Sessions and routing

- One TCP connection is one session on one thread, as today (`PostgresServer.Connection`). Transactions begin on the node's manager in-process, so nothing is routed or forwarded.
- Exposure: a layer 4 Kubernetes Service over the node pods (ClusterIP inside the cluster, LoadBalancer or NodePort outside). psql, pgjdbc, libpq, psycopg, and Npgsql connect as to PostgreSQL:

  ```
  psql "host=scalardb-cluster-pg port=5432 dbname=demo user=admin sslmode=require"
  ```

- Pinning: the Service picks a node when the connection opens and the kernel flow table keeps every packet of that connection on it. A transaction's statements travel on one connection by protocol, so they reach the same node. This holds for every driver, for a JDBC connection pool (a transaction runs on one physical connection from borrow to commit), and for PgBouncer in session or transaction mode. Session affinity on the Service is not needed.
- Rebalancing: connections never move. Applications should bound connection lifetime in their pools; the node may also close idle connections after a configurable age so a scaled-out node receives traffic.
- Lost connections: the session rolls back its transaction on socket close. If the node never sees the close, core's active transaction management expires the transaction (60 s by default).
- Gate keeper pause: statements fail or wait as the gate-kept manager dictates; map the failure to SQLSTATE 57P03 (cannot_connect_now) at startup and to 25P02 inside a transaction (open question 5).

### Authentication

- Startup: when `scalar.db.cluster.auth.enabled`, send `AuthenticationCleartextPassword`, call `AuthService.login(user, password)`, and keep the `AuthTokenInfo` in the session. On failure reply SQLSTATE 28P01 (invalid_password). When auth is disabled, send `AuthenticationOk` as today (trust).
- Per statement: set the session's token in `AuthExecutionUtils` on the connection thread before calling the manager or admin, clear it afterwards, and `logout` on close. The decorators then do what they do for gRPC callers.
- Why cleartext: the cluster verifies plaintext passwords against its own stored hashes. SCRAM or MD5 would require the server to hold verifiers in the format those mechanisms need (open question 2). Cleartext requires TLS.
- OIDC: the auth module has `OidcAuthConfig` and `OidcJwtValidator`. Passing a JWT as the PostgreSQL password is a common pattern and a natural follow-up.

### TLS

Handle `SSLRequest` (code 80877103): today the server answers `N`. Answer `S`, wrap the socket with an `SSLSocket` built from the node's certificate settings under `scalar.db.cluster.tls.*`, and refuse cleartext passwords on non-TLS connections unless explicitly allowed for development.

### Authorization

- DML and DDL are enforced by `AuthDistributedTransactionManager` and `AuthDistributedTransactionAdmin`; nothing is added in the frontend. Map the auth exceptions to SQLSTATE 42501 (insufficient_privilege).
- The catalog emulation (`Catalog`) is built from admin metadata. With authorization on, `\dt` and `\d` should show only what the user may read; whether the admin decorator already filters or the frontend must is open question 4.
- Database name is the namespace: `\l` lists namespaces, and connecting to a missing one returns 3D000 (invalid_catalog_name).
- Frontend session settings such as `scalardb.max_rows_per_write` stay as they are.

### Resource model

Thread per connection, like PostgreSQL's process per connection. Cap with `scalar.db.postgres.max_connections` and reject beyond it with 53300 (too_many_connections). Report connections, statements, errors, and statement latency through the node's `Metrics`, and keep the existing OpenTelemetry instrumentation of the manager chain.

### Module placement

1. Publish `scalardb-postgres-frontend` from the core repository, the way `scalardb-sql-direct-mode` is consumed from a package repository, and depend on it from `node`. The cluster's pin on core must then track it; the pin has flipped twice before.
2. Move the module into the cluster repository. Keeps it private and removes a cross-repository pin, but the standalone jar (A) becomes a cluster artifact as well.

This is the product question of whether a PostgreSQL-compatible interface over the open-source core library should exist (open question 1).

### Coexistence with ScalarDB SQL

This is a second SQL surface. ScalarDB SQL has its own grammar, gRPC services, JDBC driver, and Spring Data integration. The frontend accepts a PostgreSQL grammar subset (joins, subqueries, aggregates, CTEs, set operations) with in-memory evaluation and works with any PostgreSQL driver or tool. Position it as the PostgreSQL-compatible interface. Converging the two, either the frontend's planner over ScalarDB SQL's catalog and privileges or ScalarDB SQL's engine behind the PostgreSQL protocol, is out of scope here.

## Alternative considered: stream-per-transaction gRPC

Not needed for the frontend. Recorded because the discussion clarified what the ID-based routing is for.

- **Precedent.** ScalarDB Server defined the transaction RPC as a bidirectional stream: `rpc Transaction(stream TransactionRequest) returns (stream TransactionResponse)` in `rpc/src/main/proto/scalardb.proto` at v3.9.7, line 308.
- **Effect.** If begin opens a stream and every operation and the commit travel on it, the node that answered begin owns the transaction by construction. A layer 4 Service pins by connection; an HTTP-aware proxy pins by stream, since a stream cannot be split. No hashing, no membership knowledge in the client, no hop limit, no direct-kubernetes mode.
- **Load spreading.** A single channel with the pick-first policy behind a layer 4 Service does put all of a client's transactions on one node. The standard answers all work with a stream per transaction: a channel pool (the documented gRPC practice, because each HTTP/2 connection caps concurrent streams, commonly at 100, and tops out in throughput), a headless Service with the round-robin policy (one subchannel per pod, each stream stays on its subchannel, only DNS needed), or an HTTP-aware proxy balancing per stream. Rebalancing comes from a server-side max connection age with a grace period, which lets in-flight streams finish. The stream cap bounds concurrent transactions per channel, so pool size follows from the concurrency target.
- **What it gives up.** Reaching a transaction by ID from another connection or process (join, resume, two-phase commit participants), mostly removed in core 4.0; per-operation deadlines and multi-language client ergonomics are harder with streams. One-shot operations stay unary either way.
- **Status.** A possible simplification of the gRPC path, independent of this design (open question 6). The next section places it among the other round-trip reductions.

## Client-to-cluster communication: where the round trips go

Not part of the frontend integration. Recorded here because the analysis of the cluster's routing came out of it (2026-10-03) and the frontend numbers set the scale: one extra hop per statement took TPC-C from 144 to 101 tps at a 0.68 ms RTT, and the gRPC path pays extra hops per operation.

### What the SDK already avoids

- Begin is piggybacked on the first operation (`ClusterClientTransaction.begun`), so no call is spent on BEGIN alone.
- Writes are buffered on the client (`bufferedWrites`) and flushed with the next read or with the commit, so a put costs no round trip of its own.
- Token validation on the node is served from `AuthService.authTokenCache`, so auth adds no storage read per call.

### Where the round trips go today

1. **Forwarding in indirect mode.** The owner is the hash of the transaction ID, so about (N-1)/N of calls travel client to Envoy to receiving node to owner: two hops more than necessary, the proxy and the forward, on nearly every call.
2. **Direct-kubernetes mode** removes the forward but requires the client to run inside Kubernetes with API access to the endpoint list, which many deployments cannot do.
3. **Scanners.** `CreateScannerResponse` carries only the scanner ID. A scan is create, one fetch per page of the client's `scan_fetch_size` (default 10), one more fetch to learn that the scan has ended, and close. A small scan costs three or four calls where a one-shot `scan()` costs one.
4. **One channel per address** (`RemoteClusterNode`). In indirect mode that is one TCP connection for the whole client, which caps concurrency and throughput and cannot spread across nodes behind a layer 4 Service.

### Hybrid ownership rule

Forward only where two parties must reach one transaction.

- **Ordinary transactions:** the owner is the node that received BEGIN, and the SDK keeps the transaction on the connection that carried it. The transaction, SQL, and two-phase commit gRPC services call the local transaction service directly and answer transaction not found for an ID they do not hold.
- **Participant transactions keep hashing and forwarding.** In the transaction coordinator flow the client issues record-level operations to participant nodes while the coordinator process prepares and commits the same transaction (doc comment on `ClusterTwoPhaseCommitCoordinator`). Two connections, one node-local transaction, so an ID-to-node rule is inherent there. `TransactionParticipantGrpcService` keeps routing by hash, including its scanners. The coordinator keeps hashing among its own nodes.
- **The rules separate by gRPC service**, so no per-request flag is needed. The coordinator generates routing-compatible IDs while ordinary transactions use random UUIDs, so the ID format can be checked at the service boundary to catch a transaction reached through the wrong service.
- **Deployment requirements differ by path.** The pinned path needs a layer 4 or direct connection between client and nodes; the participant path works behind anything because it forwards. A client that uses global transactions still needs indirect forwarding or direct-kubernetes for its record-level calls.
- **Failure mode on the pinned path:** a connection lost mid-transaction (node restart, idle timeout, GOAWAY) puts the next call on another node, which answers transaction not found, and the application retries. A max connection age on the node must be long compared with transactions, since its grace period lets in-flight calls finish, not transactions. A client-supplied transaction ID is no longer checked for duplicates cluster-wide, only per node.
- The node keeps the membership, hashing, and hop-limit machinery for the participant service; the gain is on the hot path, not in code size.

### Improvements in order of payoff

1. **Adopt the hybrid ownership rule.** Removes the forward on almost every ordinary call and drops the Kubernetes API requirement.
2. **Replace Envoy with a layer 4 Service on the ordinary path.** kube-proxy is in-kernel, so the proxy hop disappears. Valid only together with item 1; the nodes already terminate TLS.
3. **Cut scanner calls.** Return the first page in the CreateScanner response, flag end-of-scan in the fetch response so neither the extra fetch nor the close call is needed, and raise the default fetch size. Under latency the frontend bench saw scan-heavy scripts improve three to six times from the fetch size alone. A server-streaming scan, which ScalarDB Server had (`rpc Scan(stream ScanRequest) returns (stream ScanResponse)`), is the fuller version.
4. **Pool channels, or use a headless Service with the round-robin policy.** Needed for throughput regardless, and it is what spreads pinned transactions across nodes after item 1.
5. **A stream per transaction** (previous section). Makes item 1 safe behind any proxy and removes per-call header and token overhead. A protocol change, so last.

Items 1 and 2 together turn a four-hop call into a one-hop call in indirect mode; item 3 turns a small scan from four calls into one.

## Plan

0. **Contract check.** Run the current jar with the cluster client against a local cluster; run `postgres-frontend/difftest` and the `bench` scripts. Watch scanner semantics (page fetches, close), one-shot operations, the auth token, and error mapping. Half a day; findings feed phase 1.
1. **Listener in the node** behind `scalar.db.postgres.enabled`, auth disabled only (as GraphQL today), node-level plan cache, drain on decommissioning, a Service manifest in the Helm chart.
2. **Auth and TLS.** Cleartext password over TLS, token lifetime handling, catalog filtering, SQLSTATE mapping for auth errors.
3. **Hardening.** Connection cap and max connection age, metrics, documentation.

## Tests

- Unit: the startup and auth state machine with a stubbed `AuthService`; the thread-local token set and cleared around each statement; `SSLRequest` negotiation.
- Integration, in the cluster's `integration-test`: psql and pgjdbc against a three-node standalone cluster behind a Service. Assert through node metrics that a transaction's statements run on one node; auth success and failure; a denied operation maps to 42501; decommissioning during an open transaction; a scaled-out node receives new connections.
- Rerun `postgres-frontend/difftest` and `bench` through the in-node listener and compare with the standalone numbers. Expected: one more in-cluster RTT per statement, the same RTTs per operation.

## Open questions

1. Module placement: core repository artifact or cluster repository. A product decision.
2. Password mechanism: cleartext over TLS, or SCRAM with verifiers stored in the auth tables.
3. Token lifetime versus session lifetime: re-login silently, which means keeping the password in memory for the session, or terminate the session when the token expires.
4. Catalog visibility under authorization: does the admin decorator filter, or must the frontend.
5. SQLSTATE mapping while the gate keeper is paused.
6. Whether the gRPC path should adopt the hybrid ownership rule and, later, a stream per transaction (see the communication section). A separate decision from this design.
