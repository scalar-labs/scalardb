package com.scalar.db.storage.jdbc;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.jdbc.query.Query;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A service class to perform CRUD operations.
 *
 * @author Toshihiro Suzuki
 */
@SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
@ThreadSafe
public class JdbcCrudService {

  private final TableMetadataManager tableMetadataManager;
  private final OperationChecker operationChecker;
  private final RdbEngineStrategy rdbEngine;
  private final QueryBuilder queryBuilder;
  private final int scanFetchSize;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public JdbcCrudService(
      TableMetadataManager tableMetadataManager,
      OperationChecker operationChecker,
      RdbEngineStrategy rdbEngine,
      int scanFetchSize) {
    this(
        tableMetadataManager,
        operationChecker,
        rdbEngine,
        new QueryBuilder(rdbEngine),
        scanFetchSize);
  }

  @VisibleForTesting
  JdbcCrudService(
      TableMetadataManager tableMetadataManager,
      OperationChecker operationChecker,
      RdbEngineStrategy rdbEngine,
      QueryBuilder queryBuilder,
      int scanFetchSize) {
    this.tableMetadataManager = Objects.requireNonNull(tableMetadataManager);
    this.operationChecker = Objects.requireNonNull(operationChecker);
    this.rdbEngine = Objects.requireNonNull(rdbEngine);
    this.queryBuilder = Objects.requireNonNull(queryBuilder);
    this.scanFetchSize = scanFetchSize;
  }

  public Optional<Result> get(Get get, Connection connection)
      throws SQLException, ExecutionException {
    operationChecker.check(get);
    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(get);

    SelectQuery selectQuery =
        queryBuilder
            .select(get.getProjections())
            .from(get.forNamespace().get(), get.forTable().get(), tableMetadata)
            .where(get.getPartitionKey(), get.getClusteringKey(), get.getConjunctions())
            .build();

    try (PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.sql())) {
      selectQuery.bind(preparedStatement);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        if (resultSet.next()) {
          Optional<Result> ret =
              Optional.of(
                  new ResultInterpreter(get.getProjections(), tableMetadata, rdbEngine)
                      .interpret(resultSet));
          if (resultSet.next()) {
            throw new IllegalArgumentException(
                CoreError.GET_OPERATION_USED_FOR_NON_EXACT_MATCH_SELECTION.buildMessage(get));
          }
          return ret;
        }
        return Optional.empty();
      }
    }
  }

  public Scanner getScanner(Scan scan, Connection connection)
      throws SQLException, ExecutionException {
    return getScanner(scan, connection, true);
  }

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE")
  public Scanner getScanner(
      Scan scan, Connection connection, boolean commitAndCloseConnectionOnScannerClose)
      throws SQLException, ExecutionException {
    operationChecker.check(scan);

    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(scan);

    SelectQuery selectQuery = buildSelectQuery(scan, tableMetadata);
    PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.sql());
    selectQuery.bind(preparedStatement);
    preparedStatement.setFetchSize(scanFetchSize);
    ResultSet resultSet = preparedStatement.executeQuery();
    return new ScannerImpl(
        new ResultInterpreter(scan.getProjections(), tableMetadata, rdbEngine),
        connection,
        preparedStatement,
        resultSet,
        commitAndCloseConnectionOnScannerClose);
  }

  public List<Result> scan(Scan scan, Connection connection)
      throws SQLException, ExecutionException {
    operationChecker.check(scan);

    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(scan);

    SelectQuery selectQuery = buildSelectQuery(scan, tableMetadata);
    try (PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.sql())) {
      selectQuery.bind(preparedStatement);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        List<Result> ret = new ArrayList<>();
        ResultInterpreter resultInterpreter =
            new ResultInterpreter(scan.getProjections(), tableMetadata, rdbEngine);
        while (resultSet.next()) {
          ret.add(resultInterpreter.interpret(resultSet));
        }
        return ret;
      }
    }
  }

  private SelectQuery buildSelectQuery(Scan scan, TableMetadata tableMetadata) {
    if (scan instanceof ScanAll) {
      return buildSelectQuery((ScanAll) scan, tableMetadata);
    }

    return queryBuilder
        .select(scan.getProjections())
        .from(scan.forNamespace().get(), scan.forTable().get(), tableMetadata)
        .where(
            scan.getPartitionKey(),
            scan.getStartClusteringKey(),
            scan.getStartInclusive(),
            scan.getEndClusteringKey(),
            scan.getEndInclusive(),
            scan.getConjunctions())
        .orderBy(scan.getOrderings())
        .limit(scan.getLimit())
        .build();
  }

  private SelectQuery buildSelectQuery(ScanAll scan, TableMetadata tableMetadata) {
    return queryBuilder
        .select(scan.getProjections())
        .from(scan.forNamespace().get(), scan.forTable().get(), tableMetadata)
        .where(scan.getConjunctions())
        .orderBy(scan.getOrderings())
        .limit(scan.getLimit())
        .build();
  }

  public boolean put(Put put, Connection connection) throws SQLException, ExecutionException {
    operationChecker.check(put);
    return executeSingleMutation(put, connection);
  }

  public boolean delete(Delete delete, Connection connection)
      throws SQLException, ExecutionException {
    operationChecker.check(delete);
    return executeSingleMutation(delete, connection);
  }

  private boolean executeSingleMutation(Mutation mutation, Connection connection)
      throws SQLException, ExecutionException {
    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(mutation);
    Query mutationQuery = buildMutationQuery(mutation, tableMetadata);
    return executeSingleMutationQuery(mutationQuery, connection);
  }

  public boolean mutate(List<? extends Mutation> mutations, Connection connection)
      throws SQLException, ExecutionException {
    checkArgument(!mutations.isEmpty(), CoreError.EMPTY_MUTATIONS_SPECIFIED.buildMessage());
    operationChecker.check(mutations);

    if (mutations.size() == 1) {
      return executeSingleMutation(mutations.get(0), connection);
    }

    // Group queries by SQL for batching
    Map<String, List<Query>> batchableGroups = new LinkedHashMap<>();
    List<Query> nonBatchable = new ArrayList<>();
    for (Mutation mutation : mutations) {
      TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(mutation);
      Query query = buildMutationQuery(mutation, tableMetadata);
      if (isBatchable(query)) {
        batchableGroups.computeIfAbsent(query.sql(), k -> new ArrayList<>()).add(query);
      } else {
        nonBatchable.add(query);
      }
    }

    // Execute batchable groups
    for (Map.Entry<String, List<Query>> entry : batchableGroups.entrySet()) {
      if (!executeBatch(entry.getKey(), entry.getValue(), connection)) {
        return false;
      }
    }

    // Execute non-batchable queries individually
    for (Query query : nonBatchable) {
      if (!executeSingleMutationQuery(query, connection)) {
        return false;
      }
    }

    return true;
  }

  private boolean isBatchable(Query query) {
    if (query instanceof ConditionalMutationQuery) {
      return ((ConditionalMutationQuery) query).isBatchable();
    }

    return true;
  }

  private boolean executeBatch(String sql, List<Query> queries, Connection connection)
      throws SQLException {
    if (queries.size() == 1) {
      return executeSingleMutationQuery(queries.get(0), connection);
    }

    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      for (Query query : queries) {
        query.bind(preparedStatement);
        preparedStatement.addBatch();
      }

      int[] results = preparedStatement.executeBatch();

      for (int i = 0; i < results.length; i++) {
        Query query = queries.get(i);
        boolean isConditional = query instanceof ConditionalMutationQuery;
        if (isConditional && results[i] <= 0) {
          return false;
        }
      }
    }

    return true;
  }

  private boolean executeSingleMutationQuery(Query mutationQuery, Connection connection)
      throws SQLException {
    try (PreparedStatement preparedStatement = connection.prepareStatement(mutationQuery.sql())) {
      mutationQuery.bind(preparedStatement);
      int updateCount = preparedStatement.executeUpdate();

      // For conditional mutations, we need to check whether the mutation was applied
      if (mutationQuery instanceof ConditionalMutationQuery) {
        return updateCount > 0;
      }

      return true;
    } catch (SQLException e) {
      // For conditional mutations, we need to handle the exception
      if (mutationQuery instanceof ConditionalMutationQuery) {
        return ((ConditionalMutationQuery) mutationQuery).handleSQLException(e);
      }

      throw e;
    }
  }

  private Query buildMutationQuery(Mutation mutation, TableMetadata tableMetadata) {
    assert mutation instanceof Put || mutation instanceof Delete;

    if (mutation.getCondition().isPresent()) {
      return new ConditionalMutationQuery(mutation, tableMetadata, rdbEngine, queryBuilder);
    }

    if (mutation instanceof Put) {
      Put put = (Put) mutation;
      return queryBuilder
          .upsertInto(put.forNamespace().get(), put.forTable().get(), tableMetadata)
          .values(put.getPartitionKey(), put.getClusteringKey(), put.getColumns())
          .build();
    } else {
      Delete delete = (Delete) mutation;
      return queryBuilder
          .deleteFrom(delete.forNamespace().get(), delete.forTable().get(), tableMetadata)
          .where(delete.getPartitionKey(), delete.getClusteringKey())
          .build();
    }
  }
}
