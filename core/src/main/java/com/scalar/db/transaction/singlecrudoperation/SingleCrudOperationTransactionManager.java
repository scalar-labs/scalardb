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
import com.scalar.db.api.Operation;
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
import com.scalar.db.common.AbstractTransactionManagerCrudOperableScanner;
import com.scalar.db.common.BatchResultImpl;
import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.ScalarDbUtils;
import java.io.IOException;
import java.util.Collections;
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

  @Override
  public DistributedTransaction beginReadOnly() throws TransactionException {
    throw new UnsupportedOperationException(
        CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_BEGINNING_TRANSACTION_NOT_ALLOWED
            .buildMessage());
  }

  @Override
  public DistributedTransaction beginReadOnly(String txId) throws TransactionException {
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
      return storage.get(Get.newBuilder(get).consistency(Consistency.LINEARIZABLE).build());
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, null);
    }
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    scan = copyAndSetTargetToIfNot(scan);

    try (com.scalar.db.api.Scanner scanner =
        storage.scan(Scan.newBuilder(scan).consistency(Consistency.LINEARIZABLE).build())) {
      return scanner.all();
    } catch (ExecutionException | IOException e) {
      throw new CrudException(e.getMessage(), e, null);
    }
  }

  @Override
  public Scanner getScanner(Scan scan) throws CrudException {
    scan = copyAndSetTargetToIfNot(scan);

    com.scalar.db.api.Scanner scanner;
    try {
      scanner = storage.scan(scan);
    } catch (ExecutionException e) {
      throw new CrudException(e.getMessage(), e, null);
    }

    return new AbstractTransactionManagerCrudOperableScanner() {
      @Override
      public Optional<Result> one() throws CrudException {
        try {
          return scanner.one();
        } catch (ExecutionException e) {
          throw new CrudException(e.getMessage(), e, null);
        }
      }

      @Override
      public List<Result> all() throws CrudException {
        try {
          return scanner.all();
        } catch (ExecutionException e) {
          throw new CrudException(e.getMessage(), e, null);
        }
      }

      @Override
      public void close() throws CrudException {
        try {
          scanner.close();
        } catch (IOException e) {
          throw new CrudException(e.getMessage(), e, null);
        }
      }
    };
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    put = copyAndSetTargetToIfNot(put);

    try {
      storage.put(Put.newBuilder(put).consistency(Consistency.LINEARIZABLE).build());
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
      storage.delete(Delete.newBuilder(delete).consistency(Consistency.LINEARIZABLE).build());
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

    Mutation mutation = mutations.get(0);
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

  @Override
  public List<BatchResult> batch(List<? extends Operation> operations)
      throws CrudException, UnknownTransactionStatusException {
    checkArgument(!operations.isEmpty(), CoreError.EMPTY_OPERATIONS_SPECIFIED.buildMessage());
    if (operations.size() > 1) {
      throw new UnsupportedOperationException(
          CoreError.SINGLE_CRUD_OPERATION_TRANSACTION_MULTIPLE_OPERATIONS_NOT_SUPPORTED
              .buildMessage());
    }

    Operation operation = operations.get(0);
    if (operation instanceof Get) {
      Optional<Result> result = get((Get) operation);
      return Collections.singletonList(new BatchResultImpl(result));
    } else if (operation instanceof Scan) {
      List<Result> results = scan((Scan) operation);
      return Collections.singletonList(new BatchResultImpl(results));
    } else if (operation instanceof Put) {
      put((Put) operation);
      return Collections.singletonList(BatchResultImpl.PUT_BATCH_RESULT);
    } else if (operation instanceof Insert) {
      insert((Insert) operation);
      return Collections.singletonList(BatchResultImpl.INSERT_BATCH_RESULT);
    } else if (operation instanceof Upsert) {
      upsert((Upsert) operation);
      return Collections.singletonList(BatchResultImpl.UPSERT_BATCH_RESULT);
    } else if (operation instanceof Update) {
      update((Update) operation);
      return Collections.singletonList(BatchResultImpl.UPDATE_BATCH_RESULT);
    } else if (operation instanceof Delete) {
      delete((Delete) operation);
      return Collections.singletonList(BatchResultImpl.DELETE_BATCH_RESULT);
    } else {
      throw new AssertionError("Unknown operation: " + operation);
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
