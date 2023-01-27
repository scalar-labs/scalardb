# Configurations for Consensus Commit

Consensus Commit transaction manager is the default transaction manager type in ScalarDB.
If you don't specify the `scalar.db.transaction_manager` property, or you specify `consensus-commit` for the property as follows, the Consensus Commit transaction manager is used.

```properties
scalar.db.transaction_manager=consensus-commit
```

The following sections describes the configurations for Consensus Commit.

## Basic configurations

The basic configurations for Consensus Commit are as follows:

| name                                             | description                                                                                                                                                                                                                          | default     |
|--------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| scalar.db.consensus_commit.isolation_level       | Isolation level used for ConsensusCommit. Either `SNAPSHOT` or `SERIALIZABLE` can be specified.                                                                                                                                      | SNAPSHOT    |
| scalar.db.consensus_commit.serializable_strategy | Serializable strategy used for ConsensusCommit transaction manager. Either `EXTRA_READ` or `EXTRA_WRITE` can be specified. If `SNAPSHOT` is specified in the property `scalar.db.consensus_commit.isolation_level`, this is ignored. | EXTRA_READ  |
| scalar.db.consensus_commit.coordinator.namespace | Namespace name of coordinator tables.                                                                                                                                                                                                | coordinator |

## Performance related configurations

The Performance related configurations for Consensus Commit are as follows:

| name                                                    | description                                                                    | default                                                           |
|---------------------------------------------------------|--------------------------------------------------------------------------------|-------------------------------------------------------------------|
| scalar.db.consensus_commit.parallel_executor_count      | The number of the executors (threads) for the parallel execution.              | 128                                                               |
| scalar.db.consensus_commit.parallel_preparation.enabled | Whether or not the preparation phase is executed in parallel.                  | true                                                              |
| scalar.db.consensus_commit.parallel_validation.enabled  | Whether or not the validation phase (in `EXTRA_READ`) is executed in parallel. | The value of `scalar.db.consensus_commit.parallel_commit.enabled` |
| scalar.db.consensus_commit.parallel_commit.enabled      | Whether or not the commit phase is executed in parallel.                       | true                                                              |
| scalar.db.consensus_commit.parallel_rollback.enabled    | Whether or not the rollback phase is executed in parallel.                     | The value of `scalar.db.consensus_commit.parallel_commit.enabled` |
| scalar.db.consensus_commit.async_commit.enabled         | Whether or not the commit phase is executed asynchronously.                    | false                                                             |
| scalar.db.consensus_commit.async_rollback.enabled       | Whether or not the rollback phase is executed asynchronously.                  | The value of `scalar.db.consensus_commit.async_commit.enabled`    |
