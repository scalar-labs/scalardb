# Configurations for Consensus Commit

Consensus Commit transaction manager is the default transaction manager type in Scalar DB.
If you don't specify the `scalar.db.transaction_manager` property, or you specify `consensus-commit` for the property as follows, the Consensus Commit transaction manager is used.

```properties
scalar.db.transaction_manager=consensus-commit
```

The following sections describes the configurations for Consensus Commit.

## Basic configurations

The basic configurations for Consensus Commit is as follows:

```properties
# Default isolation level for ConsensusCommit. Either SNAPSHOT or SERIALIZABLE can be specified. SNAPSHOT is used by default.
scalar.db.consensus_commit.isolation_level=SNAPSHOT

# Default serializable strategy for ConsensusCommit transaction manager.
# Either EXTRA_READ or EXTRA_WRITE can be specified. EXTRA_READ is used by default.
# If SNAPSHOT is specified, this is ignored.
scalar.db.consensus_commit.serializable_strategy=EXTRA_READ

# Namespace name of coordinator tables. The default is "coordinator"
scalar.db.consensus_commit.coordinator.namespace=
```

### Performance related configurations

The Performance related configurations for Consensus Commit is as follows:

```properties
# The number of the executors (threads) for the parallel execution
scalar.db.consensus_commit.parallel_executor_count=30

# Whether or not the preparation phase is executed in parallel
scalar.db.consensus_commit.parallel_preparation.enabled=false

# Whether or not the validation phase (in EXTRA_READ) is executed in parallel. Use the value of "scalar.db.consensus_commit.parallel_commit.enabled" as default value
scalar.db.consensus_commit.parallel_validation.enabled=

# Whether or not the commit phase is executed in parallel
scalar.db.consensus_commit.parallel_commit.enabled=false

# Whether or not the rollback phase is executed in parallel.Use the value of "scalar.db.consensus_commit.parallel_commit.enabled" as default value
scalar.db.consensus_commit.parallel_rollback.enabled=

# Whether or not the commit phase is executed asynchronously
scalar.db.consensus_commit.async_commit.enabled=false

# Whether or not the rollback phase is executed asynchronously. Use the value of "scalar.db.consensus_commit.async_commit.enabled" as default value
scalar.db.consensus_commit.async_rollback.enabled=
```
