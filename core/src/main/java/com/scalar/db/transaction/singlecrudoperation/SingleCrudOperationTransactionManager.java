package com.scalar.db.transaction.singlecrudoperation;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.SerializableStrategy;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.Update;
import com.scalar.db.api.UpdateIf;
import com.scalar.db.api.UpdateIfExists;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.AbstractDistributedTransactionManager;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.ScalarDbUtils;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class SingleCrudOperationTransactionManager extends AbstractDistributedTransactionManager {

  private final DistributedStorage storage;

  public SingleCrudOperationTransactionManager(DatabaseConfig databaseConfig) {
    super(databaseConfig);
    StorageFactory storageFactory = StorageFactory.create(databaseConfig.getProperties());
    storage = storageFactory.getStorage();
  }

  @VisibleForTesting
  SingleCrudOperationTransactionManager(DatabaseConfig databaseConfig, DistributedStorage storage) {
    super(databaseConfig);
    this.storage = storage;
  }

  @Override
  public DistributedTransaction begin() throws TransactionException {
    throw new UnsupportedOperationException(
        CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_BEGINNING_TRANSACTION_NOT_ALLOWED
            .buildMessage());
  }

  @Override
  public DistributedTransaction begin(String txId) throws TransactionException {
    throw new UnsupportedOperationException(
        CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_BEGINNING_TRANSACTION_NOT_ALLOWED
            .buildMessage());
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation) throws TransactionException {
    throw new UnsupportedOperationException(
        CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_BEGINNING_TRANSACTION_NOT_ALLOWED
            .buildMessage());
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, Isolation isolation)
      throws TransactionException {
    throw new UnsupportedOperationException(
        CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_BEGINNING_TRANSACTION_NOT_ALLOWED
            .buildMessage());
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    throw new UnsupportedOperationException(
        CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_BEGINNING_TRANSACTION_NOT_ALLOWED
            .buildMessage());
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(SerializableStrategy strategy) throws TransactionException {
    throw new UnsupportedOperationException(
        CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_BEGINNING_TRANSACTION_NOT_ALLOWED
            .buildMessage());
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException {
    throw new UnsupportedOperationException(
        CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_BEGINNING_TRANSACTION_NOT_ALLOWED
            .buildMessage());
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @Deprecated
  @Override
  public DistributedTransaction start(
      String txId, Isolation isolation, SerializableStrategy strategy) throws TransactionException {
    throw new UnsupportedOperationException(
        CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_BEGINNING_TRANSACTION_NOT_ALLOWED
            .buildMessage());
  }

  @Override
  public DistributedTransaction resume(String txId) throws TransactionNotFoundException {
    throw new UnsupportedOperationException(
        CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_RESUMING_TRANSACTION_NOT_ALLOWED
            .buildMessage());
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    get = copyAndSetTargetToIfNot(get);

    try {
      return storage.get(get.withConsistency(Consistency.LINEARIZABLE));
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, null);
    }
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    scan = copyAndSetTargetToIfNot(scan);

    try (com.scalar.db.api.Scanner scanner =
        storage.scan(scan.withConsistency(Consistency.LINEARIZABLE))) {
      return scanner.all();
    } catch (ExecutionException | IOException e) {
      throw new CrudException(e.getMessage(), e, null);
    }
  }

  @Override
  public Scanner getScanner(Scan scan) throws CrudException {
    throw new UnsupportedOperationException("Implement later");
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    put = copyAndSetTargetToIfNot(put);

    try {
      storage.put(put.withConsistency(Consistency.LINEARIZABLE));
    } catch (NoMutationException e) {
      throwUnsatisfiedConditionException(put);
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, null);
    }
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException {
    mutate(puts);
  }

  @Override
  public void insert(Insert insert) throws CrudException {
    insert = copyAndSetTargetToIfNot(insert);

    PutBuilder.Buildable buildable =
        Put.newBuilder()
            .namespace(insert.forNamespace().orElse(null))
            .table(insert.forTable().orElse(null))
            .partitionKey(insert.getPartitionKey());
    insert.getClusteringKey().ifPresent(buildable::clusteringKey);
    insert.getColumns().values().forEach(buildable::value);
    buildable.condition(ConditionBuilder.putIfNotExists());
    Put put = buildable.consistency(Consistency.LINEARIZABLE).build();

    try {
      storage.put(put);
    } catch (NoMutationException e) {
      throw new CrudConflictException(
          CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_CONFLICT_OCCURRED_IN_INSERT.buildMessage(),
          e,
          null);
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, null);
    }
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException {
    upsert = copyAndSetTargetToIfNot(upsert);

    PutBuilder.Buildable buildable =
        Put.newBuilder()
            .namespace(upsert.forNamespace().orElse(null))
            .table(upsert.forTable().orElse(null))
            .partitionKey(upsert.getPartitionKey());
    upsert.getClusteringKey().ifPresent(buildable::clusteringKey);
    upsert.getColumns().values().forEach(buildable::value);
    Put put = buildable.consistency(Consistency.LINEARIZABLE).build();

    try {
      storage.put(put);
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, null);
    }
  }

  @Override
  public void update(Update update) throws CrudException {
    update = copyAndSetTargetToIfNot(update);

    ScalarDbUtils.checkUpdate(update);
    PutBuilder.Buildable buildable =
        Put.newBuilder()
            .namespace(update.forNamespace().orElse(null))
            .table(update.forTable().orElse(null))
            .partitionKey(update.getPartitionKey());
    update.getClusteringKey().ifPresent(buildable::clusteringKey);
    update.getColumns().values().forEach(buildable::value);
    if (update.getCondition().isPresent()) {
      if (update.getCondition().get() instanceof UpdateIf) {
        buildable.condition(ConditionBuilder.putIf(update.getCondition().get().getExpressions()));
      } else {
        assert update.getCondition().get() instanceof UpdateIfExists;
        buildable.condition(ConditionBuilder.putIfExists());
      }
    } else {
      buildable.condition(ConditionBuilder.putIfExists());
    }
    Put put = buildable.consistency(Consistency.LINEARIZABLE).build();

    try {
      storage.put(put);
    } catch (NoMutationException e) {
      if (update.getCondition().isPresent()) {
        throwUnsatisfiedConditionException(update);
      }

      // If the condition is not specified, it means that the record does not exist. In this case,
      // we do nothing
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, null);
    }
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    delete = copyAndSetTargetToIfNot(delete);

    try {
      storage.delete(delete.withConsistency(Consistency.LINEARIZABLE));
    } catch (NoMutationException e) {
      throwUnsatisfiedConditionException(delete);
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, null);
    }
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    checkArgument(!mutations.isEmpty(), CoreError.EMPTY_MUTATIONS_SPECIFIED.buildMessage());
    if (mutations.size() > 1) {
      throw new UnsupportedOperationException(
          CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_MULTIPLE_MUTATIONS_NOT_SUPPORTED
              .buildMessage());
    }

    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        put((Put) mutation);
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
      } else if (mutation instanceof Insert) {
        insert((Insert) mutation);
      } else if (mutation instanceof Upsert) {
        upsert((Upsert) mutation);
      } else {
        assert mutation instanceof Update;
        update((Update) mutation);
      }
    }
  }

  private void throwUnsatisfiedConditionException(Mutation mutation)
      throws UnsatisfiedConditionException {
    assert mutation instanceof Put || mutation instanceof Delete || mutation instanceof Update;
    assert mutation.getCondition().isPresent();

    // Build the exception message
    MutationCondition condition = mutation.getCondition().get();
    String conditionColumns = null;
    // For PutIf, DeleteIf, and UpdateIf, aggregate the condition columns to the message
    if (condition instanceof PutIf
        || condition instanceof DeleteIf
        || condition instanceof UpdateIf) {
      List<ConditionalExpression> expressions = condition.getExpressions();
      conditionColumns =
          expressions.stream()
              .map(expr -> expr.getColumn().getName())
              .collect(Collectors.joining(", "));
    }

    throw new UnsatisfiedConditionException(
        CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_CONDITION_NOT_SATISFIED.buildMessage(
            condition.getClass().getSimpleName(),
            mutation.getClass().getSimpleName(),
            conditionColumns == null ? "null" : "[" + conditionColumns + "]"),
        null);
  }

  @Override
  public TransactionState getState(String txId) {
    throw new UnsupportedOperationException(
        CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_GETTING_TRANSACTION_STATE_NOT_SUPPORTED
            .buildMessage());
  }

  @Override
  public TransactionState rollback(String txId) {
    throw new UnsupportedOperationException(
        CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_ROLLING_BACK_TRANSACTION_NOT_SUPPORTED
            .buildMessage());
  }

  @Override
  public void close() {
    storage.close();
  }
}
