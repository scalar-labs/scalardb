package com.scalar.db.storage.jdbc;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection;
import com.scalar.db.storage.Utility;
import com.scalar.db.storage.jdbc.checker.OperationChecker;
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
 * A service class to perform get/scan/put/delete/mutate operations
 *
 * @author Toshihiro Suzuki
 */
@ThreadSafe
public class JdbcService {

  private final OperationChecker operationChecker;
  private final QueryBuilder queryBuilder;
  private final Optional<String> schemaPrefix;

  public JdbcService(
      OperationChecker operationChecker, QueryBuilder queryBuilder, Optional<String> schemaPrefix) {
    this.operationChecker = Objects.requireNonNull(operationChecker);
    this.queryBuilder = Objects.requireNonNull(queryBuilder);
    this.schemaPrefix = Objects.requireNonNull(schemaPrefix);
  }

  public Optional<Result> get(
      Get get, Connection connection, Optional<String> schema, Optional<String> tableName)
      throws SQLException {
    addProjectionsForKeys(get);
    Utility.setTargetToIfNot(get, schemaPrefix, schema, tableName);

    operationChecker.check(get);

    SelectQuery selectQuery =
        queryBuilder
            .select(get.getProjections())
            .from(get.forFullTableName().get())
            .where(get.getPartitionKey(), get.getClusteringKey())
            .build();

    try (PreparedStatement preparedStatement = selectQuery.prepareAndBind(connection);
        ResultSet resultSet = preparedStatement.executeQuery()) {
      if (resultSet.next()) {
        return Optional.of(selectQuery.getResult(resultSet));
      }
      return Optional.empty();
    }
  }

  public Scanner scan(
      Scan scan, Connection connection, Optional<String> schema, Optional<String> tableName)
      throws SQLException {
    addProjectionsForKeys(scan);
    Utility.setTargetToIfNot(scan, schemaPrefix, schema, tableName);

    operationChecker.check(scan);

    SelectQuery selectQuery =
        queryBuilder
            .select(scan.getProjections())
            .from(scan.forFullTableName().get())
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
      Put put, Connection connection, Optional<String> schema, Optional<String> tableName)
      throws SQLException {
    Utility.setTargetToIfNot(put, schemaPrefix, schema, tableName);

    operationChecker.check(put);

    if (!put.getCondition().isPresent()) {
      try (PreparedStatement preparedStatement =
          queryBuilder
              .upsertInto(put.forFullTableName().get())
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
      Delete delete, Connection connection, Optional<String> schema, Optional<String> tableName)
      throws SQLException {
    Utility.setTargetToIfNot(delete, schemaPrefix, schema, tableName);

    operationChecker.check(delete);

    if (!delete.getCondition().isPresent()) {
      try (PreparedStatement preparedStatement =
          queryBuilder
              .deleteFrom(delete.forFullTableName().get())
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
      Optional<String> schema,
      Optional<String> tableName)
      throws SQLException {
    mutations.forEach(m -> Utility.setTargetToIfNot(m, schemaPrefix, schema, tableName));
    operationChecker.checkMutate(mutations);

    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        if (!put((Put) mutation, connection, schema, tableName)) {
          return false;
        }
      } else {
        if (!delete((Delete) mutation, connection, schema, tableName)) {
          return false;
        }
      }
    }
    return true;
  }
}
