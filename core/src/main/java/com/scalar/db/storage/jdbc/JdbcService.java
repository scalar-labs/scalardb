package com.scalar.db.storage.jdbc;

import static com.google.common.base.Preconditions.checkArgument;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.storage.common.checker.OperationChecker;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.util.Utility;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A service class to perform get/scan/put/delete/mutate operations.
 *
 * @author Toshihiro Suzuki
 */
@ThreadSafe
public class JdbcService {

  private final JdbcTableMetadataManager tableMetadataManager;
  private final OperationChecker operationChecker;
  private final QueryBuilder queryBuilder;
  private final Optional<String> namespacePrefix;

  public JdbcService(
      JdbcTableMetadataManager tableMetadataManager,
      OperationChecker operationChecker,
      QueryBuilder queryBuilder,
      Optional<String> namespacePrefix) {
    this.tableMetadataManager = Objects.requireNonNull(tableMetadataManager);
    this.operationChecker = Objects.requireNonNull(operationChecker);
    this.queryBuilder = Objects.requireNonNull(queryBuilder);
    this.namespacePrefix = Objects.requireNonNull(namespacePrefix);
  }

  public Optional<Result> get(
      Get get, Connection connection, Optional<String> namespace, Optional<String> tableName)
      throws SQLException {
    Utility.setTargetToIfNot(get, namespacePrefix, namespace, tableName);
    operationChecker.check(get);
    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(get);
    Utility.addProjectionsForKeys(get, tableMetadata);

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
  }

  public Scanner getScanner(
      Scan scan, Connection connection, Optional<String> namespace, Optional<String> tableName)
      throws SQLException {
    Utility.setTargetToIfNot(scan, namespacePrefix, namespace, tableName);
    operationChecker.check(scan);
    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(scan);
    Utility.addProjectionsForKeys(scan, tableMetadata);

    SelectQuery selectQuery = buildSelectQueryForScan(scan);
    PreparedStatement preparedStatement = selectQuery.prepareAndBind(connection);
    ResultSet resultSet = preparedStatement.executeQuery();
    return new ScannerImpl(
        new ResultInterpreter(scan.getProjections(), tableMetadata),
        connection,
        preparedStatement,
        resultSet);
  }

  public List<Result> scan(
      Scan scan, Connection connection, Optional<String> namespace, Optional<String> tableName)
      throws SQLException {
    Utility.setTargetToIfNot(scan, namespacePrefix, namespace, tableName);
    operationChecker.check(scan);
    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(scan);
    Utility.addProjectionsForKeys(scan, tableMetadata);

    SelectQuery selectQuery = buildSelectQueryForScan(scan);
    try (PreparedStatement preparedStatement = selectQuery.prepareAndBind(connection);
        ResultSet resultSet = preparedStatement.executeQuery()) {
      List<Result> ret = new ArrayList<>();
      ResultInterpreter resultInterpreter =
          new ResultInterpreter(scan.getProjections(), tableMetadata);
      while (resultSet.next()) {
        ret.add(resultInterpreter.interpret(resultSet));
      }
      return ret;
    }
  }

  private SelectQuery buildSelectQueryForScan(Scan scan) {
    return queryBuilder
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
  }

  public boolean put(
      Put put, Connection connection, Optional<String> namespace, Optional<String> tableName)
      throws SQLException {
    Utility.setTargetToIfNot(put, namespacePrefix, namespace, tableName);
    operationChecker.check(put);

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
    operationChecker.check(delete);

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
    checkArgument(mutations.size() != 0);
    Utility.setTargetToIfNot(mutations, namespacePrefix, namespace, tableName);
    operationChecker.check(mutations);

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
