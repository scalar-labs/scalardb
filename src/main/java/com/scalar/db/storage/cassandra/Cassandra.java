package com.scalar.db.storage.cassandra;

import static com.google.common.base.Preconditions.checkArgument;

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
import com.scalar.db.api.Selection;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.InvalidUsageException;
import com.scalar.db.io.Key;
import com.scalar.db.storage.StorageUtility;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
public class Cassandra implements DistributedStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra.class);
  private final StatementHandlerManager handlers;
  private final BatchHandler batch;
  private final ClusterManager clusterManager;
  private final Map<String, CassandraTableMetadata> tableMetadataMap;
  private Optional<String> namespace;
  private Optional<String> tableName;

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

    namespace = Optional.empty();
    tableName = Optional.empty();
    tableMetadataMap = new ConcurrentHashMap<>();
  }

  @Override
  public void with(String namespace, String tableName) {
    this.namespace = Optional.ofNullable(namespace);
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public void withNamespace(String namespace) {
    this.namespace = Optional.ofNullable(namespace);
  }

  @Override
  public Optional<String> getNamespace() {
    return namespace;
  }

  @Override
  public void withTable(String tableName) {
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public Optional<String> getTable() {
    return tableName;
  }

  @Override
  @Nonnull
  public Optional<Result> get(Get get) throws ExecutionException {
    LOGGER.debug("executing get operation with " + get);
    StorageUtility.setTargetToIfNot(get, namespace, tableName);
    addProjectionsForKeys(get);
    CassandraTableMetadata metadata =
        getTableMetadata(get.forNamespace().get(), get.forTable().get());

    List<com.datastax.driver.core.Row> rows = handlers.select().handle(get).all();
    if (rows.size() > 1) {
      throw new InvalidUsageException("please use scan() for non-exact match selection");
    }
    if (rows.isEmpty() || rows.get(0) == null) {
      return Optional.empty();
    }
    return Optional.of(new ResultImpl(rows.get(0), metadata));
  }

  @Override
  @Nonnull
  public Scanner scan(Scan scan) throws ExecutionException {
    LOGGER.debug("executing scan operation with " + scan);
    StorageUtility.setTargetToIfNot(scan, namespace, tableName);
    addProjectionsForKeys(scan);
    CassandraTableMetadata metadata =
        getTableMetadata(scan.forNamespace().get(), scan.forTable().get());

    com.datastax.driver.core.ResultSet results = handlers.select().handle(scan);
    return new ScannerImpl(results, metadata);
  }

  @Override
  public void put(Put put) throws ExecutionException {
    LOGGER.debug("executing put operation with " + put);
    StorageUtility.setTargetToIfNot(put, namespace, tableName);
    checkIfPrimaryKeyExists(put);
    handlers.get(put).handle(put);
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    LOGGER.debug("executing batch-put operation with " + puts);
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    LOGGER.debug("executing delete operation with " + delete);
    StorageUtility.setTargetToIfNot(delete, namespace, tableName);
    handlers.delete().handle(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws ExecutionException {
    LOGGER.debug("executing batch-delete operation with " + deletes);
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws ExecutionException {
    checkArgument(mutations.size() != 0);
    LOGGER.debug("executing batch-mutate operation with " + mutations);
    if (mutations.size() > 1) {
      StorageUtility.setTargetToIfNot(mutations, namespace, tableName);
      batch.handle(mutations);
    } else if (mutations.size() == 1) {
      Mutation mutation = mutations.get(0);
      if (mutation instanceof Put) {
        put((Put) mutation);
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
      }
    }
  }

  @Override
  public void close() {
    clusterManager.close();
  }

  private void addProjectionsForKeys(Selection selection) {
    if (selection.getProjections().size() == 0) { // meaning projecting all
      return;
    }
    selection.getPartitionKey().forEach(v -> selection.withProjection(v.getName()));
    selection
        .getClusteringKey()
        .ifPresent(
            k -> {
              k.forEach(v -> selection.withProjection(v.getName()));
            });
  }

  private synchronized CassandraTableMetadata getTableMetadata(String namespace, String tableName) {
    String fullName = namespace + "." + tableName;
    if (!tableMetadataMap.containsKey(fullName)) {
      tableMetadataMap.put(
          fullName, new CassandraTableMetadata(clusterManager.getMetadata(namespace, tableName)));
    }
    return tableMetadataMap.get(fullName);
  }

  private void checkIfPrimaryKeyExists(Put put) {
    CassandraTableMetadata metadata =
        getTableMetadata(put.forNamespace().get(), put.forTable().get());

    StorageUtility.checkIfPrimaryKeyExists(put, metadata);
  }
}
