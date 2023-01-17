# A Guide on How to Backup and Restore Databases Used Through ScalarDB

Since ScalarDB provides transaction capability on top of non-transactional (possibly transactional) databases non-invasively, you need to take special care of backing up and restoring the databases in a transactionally-consistent way.
This document sets out some guidelines for backing up and restoring the databases that ScalarDB supports.

## Create Backup

The way to create a backup varies on what database you use and whether or not you use multiple databases.


The following is the decision tree to decide what approach you should take.

- :question: Do you use a single DB under ScalarDB?
  - :question: Does the DB have transaction support?
    - :arrow_right: use [backup without explicit pausing](#backup-without-explicit-pausing)
- Otherwise
  - :arrow_right: use [backup with explicit pausing](#backup-with-explicit-pausing)

### Backup without explicit pausing

If you use ScalarDB on a single DB with transaction support, you can create backups for the DB even while ScalarDB continues to accept transactions.

One requirement for backup in ScalarDB is that backups for all the ScalarDB managed tables (including the coordinator tables) need to be transactionally-consistent or automatically recoverable to a transactionally-consistent state.
That means that you need to create a consistent snapshot by dumping all tables in a single transaction.

The ways to create transactional backups are different among each database product.
We show examples for some database products, but you are requested to find safe ways for a transactional backup from your databases at your own risk.

#### MySQL (backup)

Use the `mysqldump` command with `--single-transaction` option.

#### PostgreSQL (backup)

Use the `pg_dump` command.

#### Amazon RDS or Azure Database for MySQL/PostgreSQL (backup)

You can restore to any point within the backup retention period with the automated backup feature.

### Backup with explicit pausing

One way to create a transactionally-consistent backup is to take a backup while ScalarDB cluster does not have outstanding transactions.
If an underlying database supports a point-in-time snapshot/backup mechanism, you can take a snapshot during the period.
If an underlying database supports a point-in-time restore/recovery mechanism, you can set a restore point to a time (preferably the mid-time) in the period.

To easily make ScalarDB drain outstanding requests and stop accepting new requests to create a pause duration, it is recommended to use [ScalarDB Server](https://github.com/scalar-labs/scalardb/tree/master/server) (which implements `scalar-admin` interface) or implement the [scalar-admin](https://github.com/scalar-labs/scalar-admin) interface properly in your ScalarDB applications.
With [scalar-admin client tool](https://github.com/scalar-labs/scalar-admin/tree/main/java#scalar-admin-client-tool), you can pause nodes/servers/applications that implement the scalar-admin interface without losing ongoing transactions.

Note that when you use a point-in-time-restore/recovery mechanism, it is recommended to minimize the clock drifts between clients and servers by using clock synchronization such as NTP.
Otherwise, the time you get as a paused duration might be too different from the time in which the pause was actually conducted, which could restore to a point where ongoing transactions exist.
Also, it is recommended to pause a long enough time (e.g., 10 seconds) and use the mid-time of the paused duration as a restore point since clock synchronization cannot perfectly synchronize clocks between nodes.

#### Database-specific ways to create a transactionally-consistent backup

**Cassandra**

Cassandra has a built-in replication mechanism, so you do not always have to create a transactionally-consistent backup.
For example, if the replication factor is set to 3 and only the data of one of the nodes in a cluster is lost, you do not need a transactionally-consistent backup because the node can be recovered with a normal (transactionally-inconsistent) snapshot and the repair mechanism.
However, if the quorum of nodes of a cluster loses their data, we need a transactionally-consistent backup to restore the cluster to a certain transactionally-consistent point.

If you want to create a transactionally-consistent cluster-wide backup, pause the ScalarDB application (or ScalarDB Server) and take the snapshots of nodes as described [here](#transactionally-consistent-backups-during-pause-duration), or stop the Cassandra cluster and take the copies of all the nodes' data, and start the cluster.

To avoid mistakes, it is recommended to use [Cassy](https://github.com/scalar-labs/cassy).
Cassy is also integrated with `scalar-admin` so it can issue a pause request to the ScalarDB application (or ScalarDB Server) of a Cassandra cluster.
Please see [the doc](https://github.com/scalar-labs/cassy/blob/master/docs/getting-started.md#take-cluster-wide-consistent-backups) for more details.

**Cosmos DB**

You must create a Cosmos DB account with a continuous backup policy enabled to use point-in-time restore (PITR) feature. Backups are created continuously after it is enabled.
To specify a transactionally-consistent restore point, please pause the ScalarDB application of a Cosmos DB as described [here](#transactionally-consistent-backups-during-pause-duration).

**DynamoDB**

You must enable the point-in-time recovery (PITR) feature for DynamoDB tables. If you use [ScalarDB Schema Loader](https://github.com/scalar-labs/scalardb/tree/master/schema-loader) for creating schema, it enables PITR feature for tables by default.
To specify a transactionally-consistent restore point, please pause the ScalarDB application of a DynamoDB as described [here](#transactionally-consistent-backups-during-pause-duration).

## Restore Backup

### MySQL (restore)

If you use `mysqldump` to create a backup file, use `mysql` command to restore the file as specified [in the MySQL doc](https://dev.mysql.com/doc/mysql-backup-excerpt/8.0/en/reloading-sql-format-dumps.html).

### PostgreSQL (restore)

If you use `pg_dump` to create a backup file, use `psql` command to restore the file as specified in [the PostgreSQL doc](https://www.postgresql.org/docs/current/backup-dump.html#BACKUP-DUMP-RESTORE).

### Amazon RDS or Azure Database for MySQL/PostgreSQL (restore)

You can restore to any point within the backup retention period with the automated backup feature.

### Cassandra (restore)

You first need to stop all the nodes of a Cassandra cluster. Clean the directories (`data`, `commitlogs`, and `hints`) and place backups (snapshots) in each node. Then, start all the nodes.

To avoid mistakes, it is recommended to use [Cassy](https://github.com/scalar-labs/cassy).
Please see [the doc](https://github.com/scalar-labs/cassy/blob/master/docs/getting-started.md#take-cluster-wide-consistent-backups) for more details.

### Cosmos DB (restore)

You can follow the [azure official guide](https://docs.microsoft.com/en-us/azure/cosmos-db/restore-account-continuous-backup#restore-account-portal). After restoring backups. change the default consistencies of the restored databases to `STRONG`
It is recommended to use the mid-time of paused duration as a restore point as we explained earlier.

### DynamoDB (restore)

You can basically follow [the official doc](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/PointInTimeRecovery.Tutorial.html). However, a table can only be restored with an alias, so you need to restore a table with an alias and delete the original table and rename the alias to the original name to restore the same named tables.

Specifically, please follow the steps below.

1. Create a backup
   * Select the mid-time of paused duration as the restore point.
   * Restore with PITR of table A to another table B
   * Take a backup of the restored table B (assume the backup is named backup B)
   * Remove table B 
2. Restore from the backup 
   * Remove table A
   * Create a table named A with backup B

Note:

* You need to do the above steps for each table because tables can only be restored one by one.
* Configurations such as PITR and auto-scaling policies are reset to the default values for restored tables, so please manually configure required settings. See [the doc](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CreateBackup.html#CreateBackup_HowItWorks-restore) for more detail.
