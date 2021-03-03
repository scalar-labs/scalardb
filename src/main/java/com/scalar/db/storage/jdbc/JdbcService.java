package com.scalar.db.storage.jdbc;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection;
import com.scalar.db.storage.common.checker.OperationChecker;
import com.scalar.db.storage.common.util.Utility;
import com.scalar.db.storage.jdbc.metadata.JdbcTableMetadata;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.SelectQuery;

import javax.annotation.concurrent.ThreadSafe;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A service class to perform get/scan/put/delete/mutate operations.
 *
 * @author Toshihiro Suzuki
 */
@ThreadSafe
public class JdbcService {

  private final TableMetadataManager tableMetadataManager;
  private final QueryBuilder queryBuilder;
  private final Optional<String> namespacePrefix;

  public JdbcService(
      TableMetadataManager tableMetadataManager,
      QueryBuilder queryBuilder,
      Optional<String> namespacePrefix) {
    this.tableMetadataManager = Objects.requireNonNull(tableMetadataManager);
    this.queryBuilder = Objects.requireNonNull(queryBuilder);
    this.namespacePrefix = Objects.requireNonNull(namespacePrefix);
  }

  public Optional<Result> get(
      Get get, Connection connection, Optional<String> namespace, Optional<String> tableName)
      throws SQLException {
    Utility.setTargetToIfNot(get, namespacePrefix, namespace, tableName);
    JdbcTableMetadata metadata = getTableMetadata(get);
    OperationChecker.check(get, metadata);
    addProjectionsForKeys(get);

    SelectQuery selectQuery =
        queryBuilder
            .select(get.getProjections())
            .from(get.forFullNamespace().get(), get.forTable().get())
            .where(get.getPartitionKey(), get.getClusteringKey())
            .build();

    try (PreparedStatement preparedStatement = selectQuery.prepareAndBind(connection);
        ResultSet resultSet = preparedStatement.executeQuery()) {
      if (resultSet.next()) {
        Optional<Result> ret = Optional.of(selectQuery.getResult(resultSet));
        if (resultSet.next()) {
          throw new IllegalArgumentException("please use scan() for non-exact match selection");
        }
        return ret;
      }
      return Optional.empty();
    }
  }

  public Scanner scan(
      Scan scan, Connection connection, Optional<String> namespace, Optional<String> tableName)
      throws SQLException {
    Utility.setTargetToIfNot(scan, namespacePrefix, namespace, tableName);
    JdbcTableMetadata metadata = getTableMetadata(scan);
    OperationChecker.check(scan, metadata);
    addProjectionsForKeys(scan);

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
    return new ScannerImpl(selectQuery, connection, preparedStatement, resultSet);
  }

  private JdbcTableMetadata getTableMetadata(Operation operation) throws SQLException {
    return tableMetadataManager.getTableMetadata(operation.forFullTableName().get());
  }

  private void addProjectionsForKeys(Selection selection) {
    if (selection.getProjections().size() == 0) { // meaning projecting all
      return;
    }
    selection.getPartitionKey().forEach(v -> selection.withProjection(v.getName()));
    selection
        .getClusteringKey()
        .ifPresent(k -> k.forEach(v -> selection.withProjection(v.getName())));
  }

  public boolean put(
      Put put, Connection connection, Optional<String> namespace, Optional<String> tableName)
      throws SQLException {
    Utility.setTargetToIfNot(put, namespacePrefix, namespace, tableName);
    JdbcTableMetadata metadata = getTableMetadata(put);
    OperationChecker.check(put, metadata);

    if (!put.getCondition().isPresent()) {
      try (PreparedStatement preparedStatement =
          queryBuilder
              .upsertInto(put.forFullNamespace().get(), put.forTable().get())
              .values(put.getPartitionKey(), put.getClusteringKey(), put.getValues())
              .build()
              .prepareAndBind(connection)) {
        preparedStatement.executeUpdate();
        return true;
      }
    } else {
      return new ConditionalMutator(put, connection, queryBuilder).mutate();
    }
  }

  public boolean delete(
      Delete delete, Connection connection, Optional<String> namespace, Optional<String> tableName)
      throws SQLException {
    Utility.setTargetToIfNot(delete, namespacePrefix, namespace, tableName);
    JdbcTableMetadata metadata = getTableMetadata(delete);
    OperationChecker.check(delete, metadata);

    if (!delete.getCondition().isPresent()) {
      try (PreparedStatement preparedStatement =
          queryBuilder
              .deleteFrom(delete.forFullNamespace().get(), delete.forTable().get())
              .where(delete.getPartitionKey(), delete.getClusteringKey())
              .build()
              .prepareAndBind(connection)) {
        preparedStatement.executeUpdate();
        return true;
      }
    } else {
      return new ConditionalMutator(delete, connection, queryBuilder).mutate();
    }
  }

  public boolean mutate(
      List<? extends Mutation> mutations,
      Connection connection,
      Optional<String> namespace,
      Optional<String> tableName)
      throws SQLException {
    Utility.setTargetToIfNot(mutations, namespacePrefix, namespace, tableName);
    OperationChecker.check(mutations);

    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        if (!put((Put) mutation, connection, namespace, tableName)) {
          return false;
        }
      } else {
        if (!delete((Delete) mutation, connection, namespace, tableName)) {
          return false;
        }
      }
    }
    return true;
  }
}
