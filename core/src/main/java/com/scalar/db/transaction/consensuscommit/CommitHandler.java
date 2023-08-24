package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.ParallelExecutor.ParallelExecutorTask;
import com.scalar.db.transaction.consensuscommit.replication.LogRecorder;
import com.scalar.db.transaction.consensuscommit.replication.repository.ReplicationTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisync.DefaultLogRecorder;
import com.scalar.db.transaction.consensuscommit.replication.semisync.PrepareMutationComposerForReplication;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CommitHandler {
  private static final Logger logger = LoggerFactory.getLogger(CommitHandler.class);
  private final DistributedStorage storage;
  private final Coordinator coordinator;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final ParallelExecutor parallelExecutor;

  private final LogRecorder logRecorder;

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

    // FIXME: This is only for PoC.
    Properties replicationDbProps = new Properties();
    replicationDbProps.put("scalar.db.storage", "jdbc");
    replicationDbProps.put(
        "scalar.db.contact_points", "jdbc:postgresql://localhost:5433/replication");
    replicationDbProps.put("scalar.db.username", "postgres");
    replicationDbProps.put("scalar.db.password", "postgres");
    replicationDbProps.put("scalar.db.jdbc.connection_pool.max_total", "50");
    ReplicationTransactionRepository replicationTransactionRepository =
        new ReplicationTransactionRepository(
            StorageFactory.create(replicationDbProps).getStorage(),
            new ObjectMapper(),
            "replication",
            "transactions");
    this.logRecorder =
        new DefaultLogRecorder(tableMetadataManager, replicationTransactionRepository);
  }

  public void commit(Snapshot snapshot) throws CommitException, UnknownTransactionStatusException {
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
    // FIXME: This should be configured
    boolean logRecorderEnabled = true;
    if (logRecorderEnabled) {
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
