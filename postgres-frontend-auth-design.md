# PostgreSQL Frontend: Authentication with ScalarDB Cluster

Status: draft, 2026-10-07. Nothing implemented yet. Based on reading `postgres-frontend` and the `scalardb-cluster` client and node code. Points marked **(verify)** need a test against a running Cluster.

## Problem

The frontend accepts every connection. It answers the StartupMessage with `AuthenticationOk` without asking for a password (`PostgresServer.java:208`), has no TLS, and runs every statement with the single identity from its `scalardb.properties`. Anyone who can reach the port can read and write every namespace. `current_user` returns the connection's user name as given, unchecked.

We want PostgreSQL clients (psql, pgjdbc and anything built on them) to log in as ScalarDB users. Their ScalarDB privileges should then apply to everything they run through the frontend, without inventing a second user store.

## What ScalarDB Cluster provides

- **Users, roles and privileges.** `AuthAdmin` (core API, implemented by Cluster) has `createUser`, `grant`/`revoke`, roles, `getCurrentUser()` and `hasPrivilege`. Cluster nodes enforce privileges on every operation (`AuthDistributedTransaction`, `AuthDistributedTransactionAdmin` in `scalardb-cluster/auth`).
- **Two login methods:** `USERPASS` and `OIDC` (`AuthAdmin.AuthenticationMethod`). A login yields a token.
- **Per-call credentials in the client.** Reserved attribute keys (`AuthOperationAttributes`) can be passed:
  - in the attributes of `begin(...)`, `beginReadOnly(...)` and `start(...)` on a transaction manager;
  - on each CRUD operation when it is executed directly on the manager (one-shot);
  - in the `WITH` clause of Cluster SQL.

  | Key | Value |
  |---|---|
  | `auth-type` | `userpass` or `oidc_jwt` |
  | `auth-userpass-username`, `auth-userpass-password` | user and password |
  | `auth-oidc-jwt-access-token` | a JWT access token |

  The client strips these keys before forwarding the operation. It logs in once per credential pair and caches the token (`AuthTokenManagerFactory`, `UserpassTokenCache`), so repeated transactions by the same user don't log in again.
- **Admin calls with a thread-local identity.** Admin methods take no attributes. `UserpassHolder.executeWithUserpass(user, password, supplier)` sets the credentials for the current thread for the duration of one call.
- **Error codes:**
  - `DB-AUTH-10005` "Invalid username or password";
  - `DB-AUTH-10006` "Access denied: Invalid auth token" (e.g. an expired token);
  - "Access denied: You need the %s privilege on the table %s ..." and similar for privilege failures.

Embedded ScalarDB core (no Cluster) has no user store: `AuthAdmin`'s methods throw "unsupported". Per-user authentication therefore requires `scalar.db.transaction_manager=cluster`. Without Cluster, the frontend can still require TLS and a shared password (below), but it cannot enforce per-user privileges.

## Design

### Configuration

New frontend settings, read from the same properties file (names are proposals):

| Property | Values | Meaning |
|---|---|---|
| `scalar.db.postgres.auth` | `none` (default), `userpass`, `oidc_jwt` | How a connection authenticates |
| `scalar.db.postgres.ssl.cert`, `scalar.db.postgres.ssl.key` | PEM paths | Server certificate and key; enables TLS |
| `scalar.db.postgres.ssl.require` | `true` (default when auth is not `none`) | Refuse connections that don't use TLS |

`none` keeps today's behavior for local development. With `userpass` or `oidc_jwt`, the frontend refuses to start without TLS unless `ssl.require=false` is set explicitly.

### Connection startup

```
client                                   frontend                              Cluster
  |-- SSLRequest ------------------------>|
  |<-- 'S' -------------------------------|  (TLS handshake)
  |-- StartupMessage(user, database) ---->|
  |<-- AuthenticationCleartextPassword ---|  R, code 3
  |-- PasswordMessage(secret) ----------->|
  |                                       |-- admin.getCurrentUser() as (user, secret) -->|
  |                                       |<-- User, or DB-AUTH-10005 -------------------|
  |<-- AuthenticationOk, ParameterStatus, BackendKeyData, ReadyForQuery
     or ErrorResponse 28P01 invalid_password
```

1. **TLS.** Handle `SSLRequest` (code 80877103) by answering `S` and wrapping the socket in an `SSLSocket` built from the configured certificate. Answer `N` when TLS is not configured. PostgreSQL 17 clients can also open TLS directly (`sslnegotiation=direct`); supporting that means detecting a TLS ClientHello as the first bytes. This is optional.
2. **Password request.** After the StartupMessage, send `AuthenticationCleartextPassword` and read the `PasswordMessage`.
   - Cleartext is required, not chosen for convenience. SCRAM-SHA-256 never reveals the password to the server, but the frontend must present the password to Cluster. Cleartext over TLS is what PostgreSQL itself does for LDAP, PAM and RADIUS authentication.
3. **Verification.** Call `admin.getCurrentUser()` inside `UserpassHolder.executeWithUserpass(user, secret, ...)` (`userpass`), or with the token (`oidc_jwt`). On success the result gives the canonical user name and the superuser flag. On `DB-AUTH-10005`/`10006`, send `28P01` and close.
   - **(verify)** whether `getCurrentUser` is the cheapest call that forces a login, or whether `beginReadOnly(attributes)` followed by a rollback is cheaper.
4. **Session state.** The session keeps the user name and the credential (password or token) in memory for its lifetime, because it must attach them to every call (below). They are never logged, never included in error messages or EXPLAIN output, and cleared when the session ends.

### Statements

All ScalarDB access from a session goes through two places, which become the only places that add credentials:

- **Transactions:** `QueryProcessor` calls `manager.begin()` / `beginReadOnly()` (`QueryProcessor.java:338`, and `:477` for the transaction a read-then-write statement opens itself). These become `begin(attributes)` / `beginReadOnly(attributes)` with the session's auth attributes. Operations inside the transaction carry no credentials of their own.
- **Autocommit one-shot operations:** `plan.open(manager)` (`QueryProcessor.java:419`, `:498`) runs Gets, Scans and mutations directly on the manager. Two options:
  - (a) **Wrap the manager** in a `CrudOperable` that copies every operation with the auth attributes added. `QueryParser.reader(crud)` (`QueryParser.java:611`) is the single entry for reads, and `crud.mutate(...)` for writes. Builders support `Operation.newBuilder(op).attribute(k, v)`.
  - (b) **Run autocommit statements in an explicit transaction** begun with the attributes. Simpler, but it loses the one-operation fast path that the benchmarks rely on.
  
  Proposal: (a).
  - **(verify)** that attribute-based credentials on one-shot operations reach every path the frontend uses: `getScanner`, `mutate` of a list, and the parallel lookups that `Operators.LookupJoin` issues from a thread pool in autocommit. Attributes travel with the operation rather than the thread, so the pool should be fine.
- **DDL:** admin calls (`CREATE TABLE`, `CREATE INDEX`, `TRUNCATE`, ...) run inside `UserpassHolder.executeWithUserpass(...)`. To avoid a compile-time dependency on the Cluster client SDK in the OSS module, the alternative is a per-user admin created lazily from properties (`scalar.db.username`/`password` set to the session's user). DDL is rare, so caching one admin per user is acceptable. Proposal: the per-user admin, unless depending on the SDK is acceptable.
- **The frontend's own catalog** (`pg_catalog` emulation) reads table metadata through the frontend's service identity. It may show tables the user cannot read. Reading the data still fails with a privilege error, but filtering the catalog by `hasPrivilege` would match PostgreSQL more closely. That is a later step.

### Identity in SQL

`current_user`, `session_user` and `current_role` return the authenticated user, as Cluster reports it in `getCurrentUser()` (`Evaluator.java:981`). psql shows it in its prompt and `\conninfo`, and tools read it.

### Errors

Add to `PostgresServer.sqlState`:

| ScalarDB error | SQLSTATE | PostgreSQL name |
|---|---|---|
| `DB-AUTH-10005` invalid username or password | 28P01 | invalid_password |
| `DB-AUTH-10006` invalid auth token (e.g. expired mid-session) | 28000 | invalid_authorization_specification; the session then closes |
| `Access denied: ...` (missing privilege, superuser required) | 42501 | insufficient_privilege |

**(verify)** how these arrive through the client API, as `CrudException` or `ExecutionException` messages or a dedicated exception type. The mapping should key on the `DB-AUTH-` code, not the English text.

**Token expiry mid-session.** With `userpass`, the Cluster client re-logs in from the cached credentials ahead of expiry (`userpassCacheExpirationMarginMillis`), so sessions should not notice. With `oidc_jwt`, the frontend only has the token the client sent at login, so a session outlives its token. When that happens, the frontend reports 28000 and closes the session. Clients reconnect with a fresh token, and pools do this automatically when a connection fails.

## Clients

Neither psql nor pgjdbc needs changes. Both implement the SSLRequest and cleartext-password steps; they only need connection settings.

**psql (libpq)**

```sh
psql "host=fe.example.com port=5432 dbname=sample user=alice sslmode=verify-full sslrootcert=ca.pem"
```

- **Password:** psql prompts for it, or takes it from `PGPASSWORD`, `~/.pgpass` or `-W`.
- **`require_auth`:** don't set `require_auth=scram-sha-256` (libpq 16+), which refuses a cleartext request. `require_auth=password` is the matching value.
- **OIDC:** `PGPASSWORD=$(get-oidc-token) psql ...` with the frontend in `oidc_jwt` mode.
- **Native OAuth, later:** PostgreSQL 18's libpq supports OAuth natively (SASL `OAUTHBEARER`, with a device-flow login in psql). If the frontend offered that SASL mechanism, psql 18 could log in through the identity provider directly, and the frontend would pass the bearer token to Cluster as `oidc_jwt`.

**pgjdbc**

```java
Properties p = new Properties();
p.setProperty("user", "alice");
p.setProperty("password", secret);
DriverManager.getConnection(
    "jdbc:postgresql://fe.example.com:5432/sample?sslmode=verify-full&sslrootcert=/path/ca.pem", p);
```

- **Pools and frameworks** (HikariCP, Spring, Hibernate) need nothing beyond `user`, `password` and the TLS parameters. BenchBase adds the parameters to its JDBC URL.
- **OIDC:** set the token as the password. Because tokens expire, new pooled connections need a fresh one. pgjdbc's `authenticationPluginClassName` lets a class supply the password per connection, the mechanism used for AWS IAM tokens. A small plugin that fetches an OIDC token keeps application code unchanged.

## Security notes

- TLS is required whenever credentials are sent. The frontend refuses plaintext password connections unless explicitly configured otherwise for local testing.
- Credentials live only in session memory and are cleared when the session closes. Logging, EXPLAIN output and error messages must not include them. The Cluster client already strips the `auth-*` attributes before forwarding operations.
- A failed login closes the connection after a fixed short delay, to slow down password guessing. The frontend does not track lockouts; Cluster's own policies apply.
- The frontend's service identity (in `scalardb.properties`) is used only for metadata lookups. It should be a user with read access to metadata only, not a superuser.
- The frontend currently ignores `CancelRequest`, so the cancellation key is unused today. It must be validated before cancellation is ever implemented, so one user cannot cancel another's query.

## Plan

1. **TLS:** `SSLRequest`, certificate configuration, and refusing plaintext when auth is on.
2. **Startup handshake:** the cleartext password request, verification through `getCurrentUser`, `28P01`, and session identity in `current_user`.
3. **Credentials on every ScalarDB call:** begin with attributes, the one-shot operation wrapper, and per-user admin for DDL.
4. **Error mapping:** `28P01`, `28000` and `42501`.
5. **OIDC mode** (the token as the password).
6. **Later:**
   - `CREATE USER`, `ALTER USER`, `GRANT` and `REVOKE` in SQL, mapped onto `AuthAdmin`;
   - a privilege-filtered catalog;
   - native `OAUTHBEARER` for psql 18.

**Rough effort for steps 1–5:** 2–3 days, plus setting up a local Cluster node for integration tests. That may need a license key.

## Tests

- **Unit:** the startup state machine (TLS on and off, cleartext request, a wrong password giving `28P01`), credential attachment on begin and one-shot operations (mocked manager), and the SQLSTATE mapping.
- **Integration against a Cluster node:**
  - psql and pgjdbc with `sslmode=require` and `verify-full`;
  - a wrong password;
  - a user without a privilege on a table: `42501` on SELECT and on INSERT;
  - a superuser-only DDL attempted by a normal user;
  - two concurrent sessions with different users, each seeing only its own privileges;
  - the parallel lookup path in autocommit;
  - OIDC token expiry mid-session.
- **Regression:** with `auth=none`, the existing difftest, concurrency test and benchmarks run unchanged.

## Open questions

1. Can the OSS `postgres-frontend` module depend on the Cluster client SDK (for `UserpassHolder`)? Or should DDL use a per-user admin created from properties?
2. Is cleartext-over-TLS acceptable for the first version, or is a frontend-side SCRAM verifier store wanted? SCRAM would remove password transport, but then Cluster could not check the password, which defeats the goal. Cleartext over TLS is recommended.
3. Without Cluster (embedded core), should the frontend offer a simple shared-password or `pg_hba`-like mode, or remain `none`-only there?
