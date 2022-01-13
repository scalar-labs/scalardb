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
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.common.checker.OperationChecker;
import com.scalar.db.storage.jdbc.query.DeleteQuery;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import com.scalar.db.util.ScalarDbUtils;
import com.scalar.db.util.TableMetadataManager;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A service class to perform get/scan/put/delete/mutate operations.
 *
 * @author Toshihiro Suzuki
 */
@SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
@ThreadSafe
public class JdbcService {

  private final TableMetadataManager tableMetadataManager;
  private final OperationChecker operationChecker;
  private final QueryBuilder queryBuilder;

  public JdbcService(
      TableMetadataManager tableMetadataManager,
      OperationChecker operationChecker,
      QueryBuilder queryBuilder) {
    this.tableMetadataManager = Objects.requireNonNull(tableMetadataManager);
    this.operationChecker = Objects.requireNonNull(operationChecker);
    this.queryBuilder = Objects.requireNonNull(queryBuilder);
  }

  public Optional<Result> get(
      Get get, Connection connection, Optional<String> namespace, Optional<String> tableName)
      throws SQLException, ExecutionException {
    ScalarDbUtils.setTargetToIfNot(get, namespace, tableName);
    operationChecker.check(get);
    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(get);
    ScalarDbUtils.addProjectionsForKeys(get, tableMetadata);

    SelectQuery selectQuery =
        queryBuilder
            .select(get.getProjections())
            .from(get.forNamespace().get(), get.forTable().get(), tableMetadata)
            .where(get.getPartitionKey(), get.getClusteringKey())
            .build();

    try (PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.sql())) {
      selectQuery.bind(preparedStatement);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
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
  }

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE")
  public Scanner getScanner(
      Scan scan, Connection connection, Optional<String> namespace, Optional<String> tableName)
      throws SQLException, ExecutionException {
    ScalarDbUtils.setTargetToIfNot(scan, namespace, tableName);
    operationChecker.check(scan);
    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(scan);
    ScalarDbUtils.addProjectionsForKeys(scan, tableMetadata);

    SelectQuery selectQuery = buildSelectQueryForScan(scan, tableMetadata);
    PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.sql());
    selectQuery.bind(preparedStatement);
    ResultSet resultSet = preparedStatement.executeQuery();
    return new ScannerImpl(
        new ResultInterpreter(scan.getProjections(), tableMetadata),
        connection,
        preparedStatement,
        resultSet);
  }

  public List<Result> scan(
      Scan scan, Connection connection, Optional<String> namespace, Optional<String> tableName)
      throws SQLException, ExecutionException {
    ScalarDbUtils.setTargetToIfNot(scan, namespace, tableName);
    operationChecker.check(scan);
    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(scan);
    ScalarDbUtils.addProjectionsForKeys(scan, tableMetadata);

    SelectQuery selectQuery = buildSelectQueryForScan(scan, tableMetadata);
    try (PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.sql())) {
      selectQuery.bind(preparedStatement);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        List<Result> ret = new ArrayList<>();
        ResultInterpreter resultInterpreter =
            new ResultInterpreter(scan.getProjections(), tableMetadata);
        while (resultSet.next()) {
          ret.add(resultInterpreter.interpret(resultSet));
        }
        return ret;
      }
    }
  }

  private SelectQuery buildSelectQueryForScan(Scan scan, TableMetadata tableMetadata) {
    return queryBuilder
        .select(scan.getProjections())
        .from(scan.forNamespace().get(), scan.forTable().get(), tableMetadata)
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
      throws SQLException, ExecutionException {
    ScalarDbUtils.setTargetToIfNot(put, namespace, tableName);
    operationChecker.check(put);

    if (!put.getCondition().isPresent()) {
      UpsertQuery upsertQuery =
          queryBuilder
              .upsertInto(put.forNamespace().get(), put.forTable().get())
              .values(put.getPartitionKey(), put.getClusteringKey(), put.getValues())
              .build();
      try (PreparedStatement preparedStatement = connection.prepareStatement(upsertQuery.sql())) {
        upsertQuery.bind(preparedStatement);
        preparedStatement.executeUpdate();
        return true;
      }
    } else {
      return new ConditionalMutator(put, connection, queryBuilder).mutate();
    }
  }

  public boolean delete(
      Delete delete, Connection connection, Optional<String> namespace, Optional<String> tableName)
      throws SQLException, ExecutionException {
    ScalarDbUtils.setTargetToIfNot(delete, namespace, tableName);
    operationChecker.check(delete);

    if (!delete.getCondition().isPresent()) {
      DeleteQuery deleteQuery =
          queryBuilder
              .deleteFrom(delete.forNamespace().get(), delete.forTable().get())
              .where(delete.getPartitionKey(), delete.getClusteringKey())
              .build();
      try (PreparedStatement preparedStatement = connection.prepareStatement(deleteQuery.sql())) {
        deleteQuery.bind(preparedStatement);
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
      throws SQLException, ExecutionException {
    assert !connection.getAutoCommit();
    checkArgument(mutations.size() != 0);
    ScalarDbUtils.setTargetToIfNot(mutations, namespace, tableName);
    operationChecker.check(mutations);

    Map<String, PreparedStatement> preparedStatementMap = new HashMap<>();
    try {
      for (Mutation mutation : mutations) {
        if (mutation instanceof Put) {
          if (!batchPut((Put) mutation, connection, preparedStatementMap)) {
            return false;
          }
        } else {
          if (!batchDelete((Delete) mutation, connection, preparedStatementMap)) {
            return false;
          }
        }
      }
      for (PreparedStatement preparedStatement : preparedStatementMap.values()) {
        preparedStatement.executeBatch();
      }
      return true;
    } finally {
      for (PreparedStatement preparedStatement : preparedStatementMap.values()) {
        preparedStatement.close();
      }
    }
  }

  private boolean batchPut(
      Put put, Connection connection, Map<String, PreparedStatement> preparedStatementMap)
      throws ExecutionException, SQLException {
    operationChecker.check(put);
    if (!put.getCondition().isPresent()) {
      UpsertQuery upsertQuery =
          queryBuilder
              .upsertInto(put.forNamespace().get(), put.forTable().get())
              .values(put.getPartitionKey(), put.getClusteringKey(), put.getValues())
              .build();
      String sql = upsertQuery.sql();
      PreparedStatement preparedStatement = preparedStatementMap.get(sql);
      if (preparedStatement == null) {
        preparedStatement = connection.prepareStatement(sql);
        preparedStatementMap.put(sql, preparedStatement);
      }
      upsertQuery.bind(preparedStatement);
      preparedStatement.addBatch();
      return true;
    } else {
      return new ConditionalMutator(put, connection, queryBuilder).mutate();
    }
  }

  private boolean batchDelete(
      Delete delete, Connection connection, Map<String, PreparedStatement> preparedStatementMap)
      throws ExecutionException, SQLException {
    operationChecker.check(delete);
    if (!delete.getCondition().isPresent()) {
      DeleteQuery deleteQuery =
          queryBuilder
              .deleteFrom(delete.forNamespace().get(), delete.forTable().get())
              .where(delete.getPartitionKey(), delete.getClusteringKey())
              .build();
      String sql = deleteQuery.sql();
      PreparedStatement preparedStatement = preparedStatementMap.get(sql);
      if (preparedStatement == null) {
        preparedStatement = connection.prepareStatement(sql);
        preparedStatementMap.put(sql, preparedStatement);
      }
      deleteQuery.bind(preparedStatement);
      preparedStatement.addBatch();
      return true;
    } else {
      return new ConditionalMutator(delete, connection, queryBuilder).mutate();
    }
  }
}
