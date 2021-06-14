package com.scalar.db.storage.hbase;

import static com.google.common.base.Preconditions.checkArgument;

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
import com.scalar.db.storage.common.checker.OperationChecker;
import com.scalar.db.storage.hbase.query.QueryBuilder;
import com.scalar.db.storage.hbase.query.SelectQuery;
import com.scalar.db.util.Utility;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBase implements DistributedStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(HBase.class);

  private final HBaseConnection hbaseConnection;
  private final HBaseTableMetadataManager tableMetadataManager;
  private final OperationChecker operationChecker;
  private final QueryBuilder queryBuilder;
  private final HBaseMutator hbaseMutator;
  private final Optional<String> namespacePrefix;

  private Optional<String> namespace;
  private Optional<String> tableName;

  @Inject
  public HBase(DatabaseConfig config) {
    String jdbcUrl = config.getContactPoints().get(0);
    hbaseConnection = new HBaseConnection(jdbcUrl);
    namespacePrefix = config.getNamespacePrefix();
    tableMetadataManager = new HBaseTableMetadataManager(hbaseConnection, namespacePrefix);
    operationChecker = new OperationChecker(tableMetadataManager);
    queryBuilder = new QueryBuilder(tableMetadataManager);
    hbaseMutator = new HBaseMutator(hbaseConnection, queryBuilder);
    namespace = Optional.empty();
    tableName = Optional.empty();
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
  public Optional<Result> get(Get get) throws ExecutionException {
    Utility.setTargetToIfNot(get, namespacePrefix, namespace, tableName);
    operationChecker.check(get);
    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(get);
    Utility.addProjectionsForKeys(get, tableMetadata);

    Connection connection = null;
    try {
      connection = hbaseConnection.getConnection();

      SelectQuery selectQuery =
          queryBuilder
              .select(get.getProjections())
              .from(get.forFullNamespace().get(), get.forTable().get())
              .where(get.getPartitionKey(), get.getClusteringKey())
              .build();

      try (PreparedStatement preparedStatement = selectQuery.prepareAndBind(connection);
          ResultSet resultSet = preparedStatement.executeQuery()) {
        if (resultSet.next()) {
          Optional<Result> ret =
              Optional.of(
                  new ResultInterpreter(get.getProjections(), tableMetadata).interpret(resultSet));
          if (resultSet.next()) {
            throw new IllegalArgumentException("please use scan() for non-exact match selection");
          }
          return ret;
        }
        return Optional.empty();
      }
    } catch (SQLException e) {
      throw new ExecutionException("get operation failed", e);
    } finally {
      close(connection);
    }
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    Utility.setTargetToIfNot(scan, namespacePrefix, namespace, tableName);
    operationChecker.check(scan);
    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(scan);
    Utility.addProjectionsForKeys(scan, tableMetadata);

    Connection connection = null;
    try {
      connection = hbaseConnection.getConnection();

      SelectQuery selectQuery =
          queryBuilder
              .select(scan.getProjections())
              .from(scan.forFullNamespace().get(), scan.forTable().get())
              .where(
                  scan.getPartitionKey(),
                  scan.getStartClusteringKey(),
                  scan.getStartInclusive(),
                  scan.getEndClusteringKey(),
                  scan.getEndInclusive())
              .orderBy(scan.getOrderings())
              .limit(scan.getLimit())
              .build();
      PreparedStatement preparedStatement = selectQuery.prepareAndBind(connection);
      ResultSet resultSet = preparedStatement.executeQuery();
      return new ScannerImpl(
          new ResultInterpreter(scan.getProjections(), tableMetadata),
          connection,
          preparedStatement,
          resultSet);
    } catch (SQLException e) {
      close(connection);
      throw new ExecutionException("scan operation failed", e);
    }
  }

  @Override
  public void put(Put put) throws ExecutionException {
    Utility.setTargetToIfNot(put, namespacePrefix, namespace, tableName);
    operationChecker.check(put);

    Connection connection = null;
    try {
      connection = hbaseConnection.getConnection();

      if (!put.getCondition().isPresent()) {
        try (PreparedStatement preparedStatement =
            queryBuilder
                .upsertInto(put.forFullNamespace().get(), put.forTable().get())
                .values(put.getPartitionKey(), put.getClusteringKey(), put.getValues())
                .build()
                .prepareAndBind(connection)) {
          preparedStatement.executeUpdate();
        }
      } else {
        hbaseMutator.mutate(Collections.singletonList(put));
      }
    } catch (SQLException e) {
      throw new ExecutionException("put operation failed", e);
    } finally {
      close(connection);
    }
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    Utility.setTargetToIfNot(delete, namespacePrefix, namespace, tableName);
    operationChecker.check(delete);

    Connection connection = null;
    try {
      connection = hbaseConnection.getConnection();
      if (!delete.getCondition().isPresent()) {
        try (PreparedStatement preparedStatement =
            queryBuilder
                .deleteFrom(delete.forFullNamespace().get(), delete.forTable().get())
                .where(delete.getPartitionKey(), delete.getClusteringKey())
                .build()
                .prepareAndBind(connection)) {
          preparedStatement.executeUpdate();
        }
      } else {
        hbaseMutator.mutate(Collections.singletonList(delete));
      }
    } catch (SQLException e) {
      throw new ExecutionException("delete operation failed", e);
    } finally {
      close(connection);
    }
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

    Utility.setTargetToIfNot(mutations, namespacePrefix, namespace, tableName);
    operationChecker.check(mutations);
    mutations.forEach(operationChecker::check);
    hbaseMutator.mutate(mutations);
  }

  private void close(Connection connection) {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("failed to close the connection", e);
    }
  }

  @Override
  public void close() {}
}
