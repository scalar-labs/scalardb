package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.ParallelExecutor.ParallelExecutorTask;
import com.scalar.db.transaction.consensuscommit.replication.LogRecorder;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.client.DefaultLogRecorder;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.client.PrepareMutationComposerForReplication;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit.GroupCommitCascadeException;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit.GroupCommitException;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit.GroupCommitter2;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CommitHandler {
  private static final String ENV_VAR_COORDINATOR_GROUP_COMMIT_ENABLED =
      "LOG_RECORDER_COORDINATOR_GROUP_COMMIT_ENABLED";
  private static final String ENV_VAR_COORDINATOR_GROUP_COMMIT_NUM_OF_RETENTION_VALUES =
      "LOG_RECORDER_COORDINATOR_GROUP_COMMIT_NUM_OF_RETENTION_VALUES";

  private static final Logger logger = LoggerFactory.getLogger(CommitHandler.class);
  private final DistributedStorage storage;
  private final Coordinator coordinator;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final ParallelExecutor parallelExecutor;

  // FIXME
  private final LogRecorder logRecorder;
  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
  private final GroupCommitter2<String, Snapshot> groupCommitter;
  private final ExecutorService executorService =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("commit-handler-commit-records-%d")
              .build());

  private Optional<LogRecorder> prepareLogRecorder() {
    String replicationDbConfigPath = System.getenv("LOG_RECORDER_REPLICATION_CONFIG");
    if (replicationDbConfigPath == null) {
      return Optional.empty();
    }
    ReplicationTransactionRepository replicationTransactionRepository;
    try {
      replicationTransactionRepository =
          new ReplicationTransactionRepository(
              StorageFactory.create(replicationDbConfigPath).getStorage(),
              objectMapper,
              "replication",
              "transactions");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return Optional.of(
        new DefaultLogRecorder(tableMetadataManager, replicationTransactionRepository));
  }

  private Optional<GroupCommitter2<String, Snapshot>> prepareGroupCommitter() {
    // TODO: Make this configurable
    // TODO: Take care of lazy recovery
    if (!"true".equalsIgnoreCase(System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_ENABLED))) {
      return Optional.empty();
    }

    int groupCommitNumOfRetentionValues = 32;
    if (System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_NUM_OF_RETENTION_VALUES) != null) {
      groupCommitNumOfRetentionValues =
          Integer.parseInt(System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_NUM_OF_RETENTION_VALUES));
    }

    return Optional.of(
        new GroupCommitter2<>(
            "coordinator-writer",
            500,
            groupCommitNumOfRetentionValues,
            5,
            32,
            snapshots -> {
              try {
                long startCommitState = System.currentTimeMillis();
                commitStateWithParentTxId(snapshots);
                logger.info(
                    "CommitState-ed(thread_id:{}, num_of_values:{}): {} ms",
                    Thread.currentThread().getId(),
                    snapshots.size(),
                    System.currentTimeMillis() - startCommitState);

                long startCommitRecords = System.currentTimeMillis();
                List<Future<Void>> futures = new ArrayList<>(snapshots.size());
                for (Snapshot snapshot : snapshots) {
                  Future<Void> f =
                      executorService.submit(
                          () -> {
                            commitRecords(snapshot);
                            return null;
                          });
                  futures.add(f);
                }
                for (Future<Void> f : futures) {
                  try {
                    f.get();
                  } catch (Throwable e) {
                    logger.warn(
                        "Committing the records failed. But the state is already committed. So the record state will be recovered later",
                        e);
                  }
                }
                logger.info(
                    "CommitRecords-ed(thread_id:{}, num_of_values:{}): {} ms",
                    Thread.currentThread().getId(),
                    snapshots.size(),
                    System.currentTimeMillis() - startCommitRecords);
              } catch (TransactionException e) {
                throw new TransactionGroupCommitException(e);
              } catch (Throwable e) {
                logger.error("Failed to group-commit", e);
                throw e;
              }
            }));
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CommitHandler(
      DistributedStorage storage,
      Coordinator coordinator,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor) {
    this.storage = checkNotNull(storage);
    this.coordinator = checkNotNull(coordinator);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
    this.parallelExecutor = checkNotNull(parallelExecutor);

    // FIXME: These are only for PoC.
    logRecorder = prepareLogRecorder().orElse(null);
    groupCommitter = prepareGroupCommitter().orElse(null);
  }

  static class TransactionGroupCommitException extends RuntimeException {
    public TransactionGroupCommitException(TransactionException cause) {
      super(cause);
    }

    public TransactionException getTransactionException() {
      return (TransactionException) getCause();
    }
  }

  public void commit(Snapshot snapshot) throws CommitException, UnknownTransactionStatusException {
    if (groupCommitter == null) {
      normalCommit(snapshot);
    } else {
      groupCommit(snapshot);
    }
  }

  private void groupCommit(Snapshot snapshot)
      throws CommitException, UnknownTransactionStatusException {
    String transactionId = snapshot.getId();
    try {
      groupCommitter.addValue(
          transactionId,
          parentId -> {
            // TODO?: Immutable
            snapshot.setParentId(parentId);
            try {
              prepareAndValidate(snapshot);
            } catch (TransactionException e) {
              throw new TransactionGroupCommitException(e);
            }
            return snapshot;
          });
    } catch (GroupCommitCascadeException e) {
      throw new CommitConflictException(e.getMessage(), e, transactionId);
    } catch (GroupCommitException e) {
      Throwable cause = e.getCause();
      TransactionException transactionEx;
      if (cause instanceof TransactionGroupCommitException) {
        TransactionGroupCommitException gce = (TransactionGroupCommitException) cause;
        transactionEx = gce.getTransactionException();
      } else if (cause instanceof TransactionException) {
        transactionEx = (TransactionException) cause;
      } else {
        throw new CommitException("Group-commit failed", cause, transactionId);
      }
      if (transactionEx instanceof PreparationConflictException) {
        PreparationConflictException ce = (PreparationConflictException) transactionEx;
        throw new CommitConflictException(
            "A conflict happened in the group-commit", ce, transactionId);
      }
      if (transactionEx instanceof ValidationConflictException) {
        ValidationConflictException ce = (ValidationConflictException) transactionEx;
        throw new CommitConflictException(
            "A conflict happened in the group-commit", ce, transactionId);
      }
      if (transactionEx instanceof CommitConflictException) {
        CommitConflictException ce = (CommitConflictException) transactionEx;
        throw new CommitConflictException(
            "A conflict happened in the group-commit", ce, transactionId);
      }
      if (transactionEx instanceof UnknownTransactionStatusException) {
        throw (UnknownTransactionStatusException) transactionEx;
      }
      throw new CommitException("Group-commit failed", transactionEx, transactionId);
    } catch (Throwable e) {
      throw new CommitException("Group-commit failed", e, transactionId);
    }
  }

  public void prepareAndValidate(Snapshot snapshot) throws TransactionException {
    Optional<Future<Void>> optLogRecordFuture;
    try {
      optLogRecordFuture = prepare(snapshot);
    } catch (PreparationException e) {
      abortState(snapshot.getId());
      rollbackRecords(snapshot);
      throw e;
    } catch (Throwable e) {
      logger.error("Failed to prepare", e);
      throw e;
    }

    try {
      validate(snapshot);
    } catch (ValidationException e) {
      abortState(snapshot.getId());
      rollbackRecords(snapshot);
      throw e;
    } catch (Throwable e) {
      logger.error("Failed to validate", e);
      throw e;
    }

    // FIXME: Move this before validate
    if (optLogRecordFuture.isPresent()) {
      Future<Void> logRecordFuture = optLogRecordFuture.get();
      try {
        logRecordFuture.get();
      } catch (InterruptedException e) {
        throw new RuntimeException(
            String.format(
                "Log recording failed due to an interruption. transactionId:%s", snapshot.getId()),
            e);
      } catch (java.util.concurrent.ExecutionException e) {
        throw new TransactionException("Log recording failed", e, snapshot.getId());
      } catch (Throwable e) {
        logger.error("Failed to replicate data", e);
        throw e;
      }
    }
  }

  private void normalCommit(Snapshot snapshot)
      throws CommitException, UnknownTransactionStatusException {
    Optional<Future<Void>> logRecordFuture;
    try {
      logRecordFuture = prepare(snapshot);
    } catch (PreparationException e) {
      abortState(snapshot.getId());
      rollbackRecords(snapshot);
      if (e instanceof PreparationConflictException) {
        throw new CommitConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
      }
      throw new CommitException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }

    try {
      validate(snapshot);
    } catch (ValidationException e) {
      abortState(snapshot.getId());
      rollbackRecords(snapshot);
      if (e instanceof ValidationConflictException) {
        throw new CommitConflictException(e.getMessage(), e, e.getTransactionId().orElse(null));
      }
      throw new CommitException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }

    logRecordFuture.ifPresent(
        logRecord -> {
          try {
            logRecord.get();
          } catch (InterruptedException e) {
            throw new RuntimeException(
                String.format(
                    "Log recording failed due to an interruption. transactionId:%s",
                    snapshot.getId()),
                e);
          } catch (java.util.concurrent.ExecutionException e) {
            throw new RuntimeException(
                String.format("Log recording failed. transactionId:%s", snapshot.getId()), e);
          }
        });

    commitState(snapshot);
    commitRecords(snapshot);
  }

  public Optional<Future<Void>> prepare(Snapshot snapshot) throws PreparationException {
    try {
      return prepareRecords(snapshot);
    } catch (NoMutationException e) {
      throw new PreparationConflictException("Preparing record exists", e, snapshot.getId());
    } catch (RetriableExecutionException e) {
      throw new PreparationConflictException(
          "Conflict happened when preparing records", e, snapshot.getId());
    } catch (ExecutionException e) {
      throw new PreparationException("Preparing records failed", e, snapshot.getId());
    }
  }

  private Optional<Future<Void>> prepareRecords(Snapshot snapshot)
      throws ExecutionException, PreparationConflictException {
    PrepareMutationComposer composer;
    Optional<Future<Void>> logRecordFuture = Optional.empty();
    if (logRecorder != null) {
      composer = new PrepareMutationComposerForReplication(snapshot.getId(), tableMetadataManager);
      snapshot.to(composer);
      logRecordFuture =
          Optional.of(logRecorder.record((PrepareMutationComposerForReplication) composer));
    } else {
      composer = new PrepareMutationComposer(snapshot.getId(), tableMetadataManager);
      snapshot.to(composer);
    }
    PartitionedMutations mutations = new PartitionedMutations(composer.get());

    ImmutableList<PartitionedMutations.Key> orderedKeys = mutations.getOrderedKeys();
    List<ParallelExecutorTask> tasks = new ArrayList<>(orderedKeys.size());
    for (PartitionedMutations.Key key : orderedKeys) {
      tasks.add(() -> storage.mutate(mutations.get(key)));
    }
    parallelExecutor.prepare(tasks, snapshot.getId());

    return logRecordFuture;
  }

  public void validate(Snapshot snapshot) throws ValidationException {
    try {
      // validation is executed when SERIALIZABLE with EXTRA_READ strategy is chosen.
      snapshot.toSerializableWithExtraRead(storage);
    } catch (ExecutionException e) {
      throw new ValidationException("Validation failed", e, snapshot.getId());
    }
  }

  public void commitState(Snapshot snapshot)
      throws CommitException, UnknownTransactionStatusException {
    String id = snapshot.getId();
    try {
      Coordinator.State state = new Coordinator.State(id, TransactionState.COMMITTED);
      coordinator.putState(state);
      logger.debug(
          "Transaction {} is committed successfully at {}", id, System.currentTimeMillis());
    } catch (CoordinatorConflictException e) {
      try {
        Optional<Coordinator.State> s = coordinator.getState(id);
        if (s.isPresent()) {
          TransactionState state = s.get().getState();
          if (state.equals(TransactionState.ABORTED)) {
            rollbackRecords(snapshot);
            throw new CommitException(
                "Committing state in coordinator failed. the transaction is aborted", e, id);
          }
        } else {
          throw new UnknownTransactionStatusException(
              "Committing state failed with NoMutationException but the coordinator status doesn't exist",
              e,
              id);
        }
      } catch (CoordinatorException e1) {
        throw new UnknownTransactionStatusException("Can't get the state", e1, id);
      }
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException("Coordinator status is unknown", e, id);
    }
  }

  public void commitStateWithParentTxId(List<Snapshot> snapshots)
      throws CommitException, UnknownTransactionStatusException {
    // Validate parent transaction IDs
    String id = null;
    for (Snapshot snapshot : snapshots) {
      if (id == null) {
        id = snapshot.getParentId();
      } else {
        if (!id.equals(snapshot.getParentId())) {
          throw new AssertionError(
              String.format(
                  "Found different parent transaction IDs. prev=%s, new=%s",
                  id, snapshot.getParentId()));
        }
      }
    }
    if (id == null) {
      throw new AssertionError("No parent transaction ID is found. snapshots:" + snapshots);
    }

    try {
      Coordinator.State state = new Coordinator.State(id, TransactionState.COMMITTED);
      coordinator.putState(state);
      logger.debug(
          "Transaction {} is committed successfully at {}", id, System.currentTimeMillis());
    } catch (CoordinatorConflictException e) {
      try {
        Optional<Coordinator.State> s = coordinator.getState(id);
        if (s.isPresent()) {
          TransactionState state = s.get().getState();
          if (state.equals(TransactionState.ABORTED)) {
            for (Snapshot snapshot : snapshots) {
              rollbackRecords(snapshot);
            }
            throw new CommitException(
                "Committing state in coordinator failed. the transaction is aborted", e, id);
          }
        } else {
          throw new UnknownTransactionStatusException(
              "Committing state failed with NoMutationException but the coordinator status doesn't exist",
              e,
              id);
        }
      } catch (CoordinatorException e1) {
        throw new UnknownTransactionStatusException("Can't get the state", e1, id);
      }
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException("Coordinator status is unknown", e, id);
    }
  }

  public void commitRecords(Snapshot snapshot) {
    try {
      CommitMutationComposer composer = new CommitMutationComposer(snapshot.getId());
      snapshot.to(composer);
      PartitionedMutations mutations = new PartitionedMutations(composer.get());

      ImmutableList<PartitionedMutations.Key> orderedKeys = mutations.getOrderedKeys();
      List<ParallelExecutorTask> tasks = new ArrayList<>(orderedKeys.size());
      for (PartitionedMutations.Key key : orderedKeys) {
        tasks.add(() -> storage.mutate(mutations.get(key)));
      }
      parallelExecutor.commitRecords(tasks, snapshot.getId());
    } catch (Exception e) {
      logger.warn("Committing records failed. transaction ID: {}", snapshot.getId(), e);
      // ignore since records are recovered lazily
    }
  }

  public TransactionState abortState(String id) throws UnknownTransactionStatusException {
    try {
      Coordinator.State state = new Coordinator.State(id, TransactionState.ABORTED);
      coordinator.putState(state);
      return TransactionState.ABORTED;
    } catch (CoordinatorConflictException e) {
      try {
        Optional<Coordinator.State> state = coordinator.getState(id);
        if (state.isPresent()) {
          // successfully COMMITTED or ABORTED
          return state.get().getState();
        }
        throw new UnknownTransactionStatusException(
            "Aborting state failed with NoMutationException but the coordinator status doesn't exist",
            e,
            id);
      } catch (CoordinatorException e1) {
        throw new UnknownTransactionStatusException("Can't get the state", e1, id);
      }
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException("Coordinator status is unknown", e, id);
    }
  }

  public void rollbackRecords(Snapshot snapshot) {
    logger.debug("Rollback from snapshot for {}", snapshot.getId());
    try {
      RollbackMutationComposer composer =
          new RollbackMutationComposer(snapshot.getId(), storage, tableMetadataManager);
      snapshot.to(composer);
      PartitionedMutations mutations = new PartitionedMutations(composer.get());

      ImmutableList<PartitionedMutations.Key> orderedKeys = mutations.getOrderedKeys();
      List<ParallelExecutorTask> tasks = new ArrayList<>(orderedKeys.size());
      for (PartitionedMutations.Key key : orderedKeys) {
        tasks.add(() -> storage.mutate(mutations.get(key)));
      }
      parallelExecutor.rollbackRecords(tasks, snapshot.getId());
    } catch (Exception e) {
      logger.warn("Rolling back records failed. transaction ID: {}", snapshot.getId(), e);
      // ignore since records are recovered lazily
    }
  }
}
