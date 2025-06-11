package com.scalar.db.storage.objectstorage;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.common.AbstractDistributedStorage;
import com.scalar.db.common.FilterableScanner;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectStorage extends AbstractDistributedStorage {
  private static final Logger logger = LoggerFactory.getLogger(ObjectStorage.class);

  private final ObjectStorageWrapper wrapper;
  private final SelectStatementHandler selectStatementHandler;
  private final MutateStatementHandler mutateStatementHandler;
  private final OperationChecker operationChecker;

  public ObjectStorage(DatabaseConfig databaseConfig) {
    super(databaseConfig);
    if (databaseConfig.isCrossPartitionScanOrderingEnabled()) {
      throw new IllegalArgumentException(
          CoreError.OBJECT_STORAGE_CROSS_PARTITION_SCAN_WITH_ORDERING_NOT_SUPPORTED.buildMessage());
    }
    ObjectStorageConfig config = new ObjectStorageConfig(databaseConfig);
    wrapper = ObjectStorageUtils.getObjectStorageWrapper(config);
    TableMetadataManager metadataManager =
        new TableMetadataManager(
            new ObjectStorageAdmin(wrapper, config),
            databaseConfig.getMetadataCacheExpirationTimeSecs());
    operationChecker = new ObjectStorageOperationChecker(databaseConfig, metadataManager);
    selectStatementHandler = new SelectStatementHandler(wrapper, metadataManager);
    mutateStatementHandler = new MutateStatementHandler(wrapper, metadataManager);
    logger.info("ObjectStorage object is created properly");
  }

  @VisibleForTesting
  public ObjectStorage(
      DatabaseConfig databaseConfig,
      ObjectStorageWrapper wrapper,
      SelectStatementHandler select,
      MutateStatementHandler mutate,
      OperationChecker operationChecker) {
    super(databaseConfig);
    this.wrapper = wrapper;
    this.selectStatementHandler = select;
    this.mutateStatementHandler = mutate;
    this.operationChecker = operationChecker;
  }

  @Override
  public Optional<Result> get(Get get) throws ExecutionException {
    get = copyAndSetTargetToIfNot(get);
    operationChecker.check(get);
    Scanner scanner = null;
    try {
      if (get.getConjunctions().isEmpty()) {
        scanner = selectStatementHandler.handle(get);
      } else {
        scanner =
            new FilterableScanner(
                get, selectStatementHandler.handle(copyAndPrepareForDynamicFiltering(get)));
      }
      Optional<Result> ret = scanner.one();
      if (!scanner.one().isPresent()) {
        return ret;
      } else {
        throw new IllegalArgumentException(
            CoreError.GET_OPERATION_USED_FOR_NON_EXACT_MATCH_SELECTION.buildMessage(get));
      }
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
          logger.warn("Failed to close the scanner", e);
        }
      }
    }
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    scan = copyAndSetTargetToIfNot(scan);
    operationChecker.check(scan);
    if (scan.getConjunctions().isEmpty()) {
      return selectStatementHandler.handle(scan);
    } else {
      return new FilterableScanner(
          scan, selectStatementHandler.handle(copyAndPrepareForDynamicFiltering(scan)));
    }
  }

  @Override
  public void put(Put put) throws ExecutionException {
    put = copyAndSetTargetToIfNot(put);
    operationChecker.check(put);
    mutateStatementHandler.handle(put);
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    delete = copyAndSetTargetToIfNot(delete);
    operationChecker.check(delete);
    mutateStatementHandler.handle(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws ExecutionException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws ExecutionException {
    mutations = copyAndSetTargetToIfNot(mutations);
    operationChecker.check(mutations);
    mutateStatementHandler.handle(mutations);
  }

  @Override
  public void close() {
    wrapper.close();
  }
}
