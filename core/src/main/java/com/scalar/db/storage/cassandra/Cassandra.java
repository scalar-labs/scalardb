package com.scalar.db.storage.cassandra;

import static com.google.common.base.Preconditions.checkArgument;

import com.datastax.driver.core.Session;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
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
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A storage implementation with Cassandra for {@link DistributedStorage}.
 *
 * @author Hiroyuki Yamada
 */
@ThreadSafe
public class Cassandra extends AbstractDistributedStorage {
  private static final Logger logger = LoggerFactory.getLogger(Cassandra.class);
  private final StatementHandlerManager handlers;
  private final BatchHandler batch;
  private final ClusterManager clusterManager;
  private final TableMetadataManager metadataManager;
  private final OperationChecker operationChecker;

  @Inject
  public Cassandra(DatabaseConfig config) {
    super(config);

    if (config.isCrossPartitionScanOrderingEnabled()) {
      throw new IllegalArgumentException(
          CoreError.CASSANDRA_CROSS_PARTITION_SCAN_WITH_ORDERING_NOT_SUPPORTED.buildMessage());
    }

    clusterManager = new ClusterManager(config);
    Session session = clusterManager.getSession();

    handlers =
        StatementHandlerManager.builder()
            .select(new SelectStatementHandler(session))
            .insert(new InsertStatementHandler(session))
            .update(new UpdateStatementHandler(session))
            .delete(new DeleteStatementHandler(session))
            .build();

    batch = new BatchHandler(session, handlers);
    logger.info("Cassandra object is created properly");

    metadataManager =
        new TableMetadataManager(
            new CassandraAdmin(clusterManager), config.getMetadataCacheExpirationTimeSecs());
    operationChecker = new OperationChecker(config, metadataManager);
  }

  @VisibleForTesting
  Cassandra(
      DatabaseConfig config,
      ClusterManager clusterManager,
      StatementHandlerManager handlers,
      BatchHandler batch,
      TableMetadataManager metadataManager,
      OperationChecker operationChecker) {
    super(config);
    this.clusterManager = clusterManager;
    this.handlers = handlers;
    this.batch = batch;
    this.metadataManager = metadataManager;
    this.operationChecker = operationChecker;
  }

  @Override
  @Nonnull
  public Optional<Result> get(Get get) throws ExecutionException {
    get = copyAndSetTargetToIfNot(get);
    operationChecker.check(get);

    Scanner scanner = null;
    try {
      if (get.getConjunctions().isEmpty()) {
        scanner = getInternal(get);
      } else {
        scanner = new FilterableScanner(get, getInternal(copyAndPrepareForDynamicFiltering(get)));
      }
      Optional<Result> ret = scanner.one();
      if (scanner.one().isPresent()) {
        throw new IllegalArgumentException(
            CoreError.GET_OPERATION_USED_FOR_NON_EXACT_MATCH_SELECTION.buildMessage(get));
      }
      return ret;
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

  private Scanner getInternal(Get get) throws ExecutionException {
    return new ScannerImpl(
        handlers.select().handle(get),
        new ResultInterpreter(get.getProjections(), metadataManager.getTableMetadata(get)));
  }

  @Override
  @Nonnull
  public Scanner scan(Scan scan) throws ExecutionException {
    scan = copyAndSetTargetToIfNot(scan);
    operationChecker.check(scan);

    if (scan.getConjunctions().isEmpty()) {
      return scanInternal(scan);
    } else {
      return new FilterableScanner(scan, scanInternal(copyAndPrepareForDynamicFiltering(scan)));
    }
  }

  private Scanner scanInternal(Scan scan) throws ExecutionException {
    return new ScannerImpl(
        handlers.select().handle(scan),
        new ResultInterpreter(scan.getProjections(), metadataManager.getTableMetadata(scan)));
  }

  @Override
  public void put(Put put) throws ExecutionException {
    put = copyAndSetTargetToIfNot(put);
    operationChecker.check(put);
    handlers.get(put).handle(put);
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    delete = copyAndSetTargetToIfNot(delete);
    operationChecker.check(delete);
    handlers.delete().handle(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws ExecutionException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws ExecutionException {
    checkArgument(!mutations.isEmpty(), CoreError.EMPTY_MUTATIONS_SPECIFIED.buildMessage());
    if (mutations.size() == 1) {
      Mutation mutation = mutations.get(0);
      if (mutation instanceof Put) {
        put((Put) mutation);
        return;
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
        return;
      }
    }

    mutations = copyAndSetTargetToIfNot(mutations);
    operationChecker.check(mutations);
    batch.handle(mutations);
  }

  @Override
  public void close() {
    clusterManager.close();
  }
}
