package com.scalar.db.storage.cassandra;

import static com.google.common.base.Preconditions.checkArgument;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.common.AbstractDistributedStorage;
import com.scalar.db.storage.common.checker.OperationChecker;
import com.scalar.db.util.ScalarDbUtils;
import com.scalar.db.util.TableMetadataManager;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra.class);
  private final StatementHandlerManager handlers;
  private final BatchHandler batch;
  private final ClusterManager clusterManager;
  private final TableMetadataManager metadataManager;
  private final OperationChecker operationChecker;

  @Inject
  public Cassandra(DatabaseConfig config) {
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
    LOGGER.info("Cassandra object is created properly.");

    metadataManager =
        new TableMetadataManager(
            new CassandraAdmin(clusterManager, config),
            config.getTableMetadataCacheExpirationTimeSecs());
    operationChecker = new OperationChecker(metadataManager);
  }

  @Override
  @Nonnull
  public Optional<Result> get(Get get) throws ExecutionException {
    get = copyAndSetTargetToIfNot(get);
    operationChecker.check(get);
    TableMetadata metadata = metadataManager.getTableMetadata(get);
    ScalarDbUtils.addProjectionsForKeys(get, metadata);

    ResultSet resultSet = handlers.select().handle(get);
    Row row = resultSet.one();
    if (row == null) {
      return Optional.empty();
    }
    Row next = resultSet.one();
    if (next != null) {
      throw new IllegalArgumentException("please use scan() for non-exact match selection");
    }
    return Optional.of(new ResultInterpreter(get.getProjections(), metadata).interpret(row));
  }

  @Override
  @Nonnull
  public Scanner scan(Scan scan) throws ExecutionException {
    scan = copyAndSetTargetToIfNot(scan);
    operationChecker.check(scan);
    TableMetadata metadata = metadataManager.getTableMetadata(scan);
    ScalarDbUtils.addProjectionsForKeys(scan, metadata);

    ResultSet results = handlers.select().handle(scan);

    return new ScannerImpl(results, new ResultInterpreter(scan.getProjections(), metadata));
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
    checkArgument(mutations.size() != 0);
    if (mutations.size() == 1) {
      Mutation mutation = mutations.get(0);
      if (mutation instanceof Put) {
        put((Put) mutation);
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
      }
      return;
    }

    mutations = copyAndSetTargetToIfNot(mutations);
    operationChecker.check(mutations);
    for (Mutation mutation : mutations) {
      operationChecker.check(mutation);
    }
    batch.handle(mutations);
  }

  @Override
  public void close() {
    clusterManager.close();
  }
}
