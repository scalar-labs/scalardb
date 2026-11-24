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
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.TextColumn;
import com.scalar.db.storage.jdbc.query.Query;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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

  /**
   * The largest IN list of a multi-get statement: within Oracle's limit of 1,000 expressions in a
   * list, SQL Server's 2,100 parameters per statement, and SQLite's default of 999 parameters
   * before 3.32.0.
   */
  @VisibleForTesting static final int MAX_IN_LIST_SIZE = 512;

  /**
   * Retrieves the results of the Gets, in the order of the Gets, each as {@link #get(Get,
   * Connection)} would. The Gets on one table that have no projections, no conditions and no index,
   * and that share every primary key column but the last, are read with one {@code SELECT ... WHERE
   * k1=? AND ... AND kn IN (?, ...)} per up to {@link #MAX_IN_LIST_SIZE} Gets. The list is padded
   * to a power of two by repeating its last key, so that few distinct statements exist per table.
   * The other Gets, and a group with a single key, run one by one.
   *
   * <p>A record is matched to its Get by the value of the last primary key column. For column types
   * whose SQL equality can differ from the value's (TEXT under a case-insensitive or pad-space
   * collation, FLOAT and DOUBLE, BLOB, and the date and time types), the Gets that match no record
   * are checked with one more IN list of their values alone, and read again one by one only if that
   * finds records.
   *
   * @param gets the Gets, whose targets are set
   * @param connection the connection
   * @return the results, in the order of the Gets
   */
  public List<Optional<Result>> multiGet(List<Get> gets, Connection connection)
      throws SQLException, ExecutionException {
    @SuppressWarnings("unchecked")
    Optional<Result>[] results = new Optional[gets.size()];
    Map<List<Object>, List<Integer>> groups = new LinkedHashMap<>();
    for (int i = 0; i < gets.size(); i++) {
      Get get = gets.get(i);
      operationChecker.check(get);
      TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(get);
      if (!get.getProjections().isEmpty()
          || !get.getConjunctions().isEmpty()
          || ScalarDbUtils.isSecondaryIndexSpecified(get, tableMetadata)) {
        results[i] = get(get, connection);
        continue;
      }
      List<Column<?>> keyColumns = keyColumns(get);
      List<Object> group = new ArrayList<>(keyColumns.subList(0, keyColumns.size() - 1));
      group.add(0, get.forFullTableName().get());
      groups.computeIfAbsent(group, g -> new ArrayList<>()).add(i);
    }

    for (List<Integer> group : groups.values()) {
      for (int from = 0; from < group.size(); from += MAX_IN_LIST_SIZE) {
        getWithInList(
            gets,
            group.subList(from, Math.min(group.size(), from + MAX_IN_LIST_SIZE)),
            results,
            connection);
      }
    }
    return Arrays.asList(results);
  }

  private static List<Column<?>> keyColumns(Get get) {
    List<Column<?>> keyColumns = new ArrayList<>(get.getPartitionKey().getColumns());
    get.getClusteringKey().ifPresent(k -> keyColumns.addAll(k.getColumns()));
    return keyColumns;
  }

  private void getWithInList(
      List<Get> gets, List<Integer> indexes, Optional<Result>[] results, Connection connection)
      throws SQLException, ExecutionException {
    // The Gets by their value of the last primary key column; duplicate Gets share a value
    Map<Column<?>, List<Integer>> indexesByLastKey = new LinkedHashMap<>();
    for (int i : indexes) {
      List<Column<?>> keyColumns = keyColumns(gets.get(i));
      indexesByLastKey
          .computeIfAbsent(keyColumns.get(keyColumns.size() - 1), k -> new ArrayList<>())
          .add(i);
    }
    if (indexesByLastKey.size() == 1) {
      Optional<Result> result = get(gets.get(indexes.get(0)), connection);
      for (int i : indexes) {
        results[i] = result;
      }
      return;
    }

    Get first = gets.get(indexes.get(0));
    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(first);
    List<Column<?>> leadingKeyColumns = keyColumns(first);
    Column<?> lastKeyColumn = leadingKeyColumns.remove(leadingKeyColumns.size() - 1);
    InListRows rows =
        selectWithInList(
            first,
            tableMetadata,
            leadingKeyColumns,
            new ArrayList<>(indexesByLastKey.keySet()),
            connection);
    Map<Column<?>, List<Integer>> indexesByMatchKey = indexesByLastKey;
    if (rows.blankPadded) {
      // A CHAR column holds and returns its values padded with spaces to its length, and SQL
      // compares them ignoring trailing spaces, so match them with trailing spaces removed: two
      // records cannot differ only in trailing spaces
      indexesByMatchKey = new HashMap<>();
      for (Map.Entry<Column<?>, List<Integer>> entry : indexesByLastKey.entrySet()) {
        indexesByMatchKey
            .computeIfAbsent(withoutTrailingSpaces(entry.getKey()), k -> new ArrayList<>())
            .addAll(entry.getValue());
      }
    }
    for (Result result : rows.results) {
      Column<?> value = result.getColumns().get(lastKeyColumn.getName());
      List<Integer> matched =
          indexesByMatchKey.get(rows.blankPadded ? withoutTrailingSpaces(value) : value);
      if (matched == null) {
        // Equal in SQL but not in value; handled with the Gets that match no record below
        continue;
      }
      for (int i : matched) {
        if (results[i] != null) {
          throw new IllegalArgumentException(
              CoreError.GET_OPERATION_USED_FOR_NON_EXACT_MATCH_SELECTION.buildMessage(gets.get(i)));
        }
        results[i] = Optional.of(result);
      }
    }

    List<Column<?>> missed = new ArrayList<>();
    for (Map.Entry<Column<?>, List<Integer>> entry : indexesByLastKey.entrySet()) {
      if (results[entry.getValue().get(0)] == null) {
        missed.add(entry.getKey());
        for (int i : entry.getValue()) {
          results[i] = Optional.empty();
        }
      }
    }
    // For a type whose SQL equality can differ from the value's, a Get that matches no record may
    // still match one in SQL (e.g., "A" a record "a" under a case-insensitive collation). Any such
    // record is in the result of an IN list of the missed values alone: when that is empty, the
    // missed Gets have no record; otherwise they are read again one by one
    if (missed.isEmpty()
        || isExactMatchType(lastKeyColumn.getDataType())
        || (missed.size() > 1
            && selectWithInList(first, tableMetadata, leadingKeyColumns, missed, connection)
                .results
                .isEmpty())) {
      return;
    }
    for (Column<?> value : missed) {
      List<Integer> matched = indexesByLastKey.get(value);
      Optional<Result> result = get(gets.get(matched.get(0)), connection);
      for (int i : matched) {
        results[i] = result;
      }
    }
  }

  /**
   * Selects the records whose primary key columns but the last equal {@code leadingKeyColumns} and
   * whose last primary key column is one of {@code values}, with the IN list padded to a power of
   * two by repeating its last value (IN returns each matching record once).
   */
  private InListRows selectWithInList(
      Get get,
      TableMetadata tableMetadata,
      List<Column<?>> leadingKeyColumns,
      List<Column<?>> values,
      Connection connection)
      throws SQLException {
    List<Column<?>> padded = new ArrayList<>(values);
    int size = Integer.highestOneBit(values.size() * 2 - 1);
    padded.addAll(Collections.nCopies(size - values.size(), values.get(values.size() - 1)));
    SelectQuery selectQuery =
        queryBuilder
            .select(Collections.emptyList())
            .from(get.forNamespace().get(), get.forTable().get(), tableMetadata)
            .where(leadingKeyColumns, padded)
            .build();
    ResultInterpreter interpreter =
        new ResultInterpreter(Collections.emptyList(), tableMetadata, rdbEngine);
    List<Result> results = new ArrayList<>();
    boolean blankPadded = false;
    try (PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.sql())) {
      selectQuery.bind(preparedStatement);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          if (results.isEmpty()) {
            blankPadded = isBlankPadded(values.get(0), resultSet);
          }
          results.add(interpreter.interpret(resultSet));
        }
      }
    }
    return new InListRows(results, blankPadded);
  }

  /** The records an IN-list query selects. */
  private static class InListRows {
    final List<Result> results;
    /** Whether the last key column is a CHAR column, whose values are padded with spaces. */
    final boolean blankPadded;

    InListRows(List<Result> results, boolean blankPadded) {
      this.results = results;
      this.blankPadded = blankPadded;
    }
  }

  private static boolean isBlankPadded(Column<?> lastKeyColumn, ResultSet resultSet)
      throws SQLException {
    if (lastKeyColumn.getDataType() != DataType.TEXT) {
      return false;
    }
    int type = resultSet.getMetaData().getColumnType(resultSet.findColumn(lastKeyColumn.getName()));
    return type == Types.CHAR || type == Types.NCHAR;
  }

  private static Column<?> withoutTrailingSpaces(Column<?> column) {
    String text = column.getTextValue();
    if (text == null) {
      return column;
    }
    int end = text.length();
    while (end > 0 && text.charAt(end - 1) == ' ') {
      end--;
    }
    return end == text.length() ? column : TextColumn.of(column.getName(), text.substring(0, end));
  }

  /** Whether SQL equality on a column of the type is equality of the values. */
  private static boolean isExactMatchType(DataType dataType) {
    return dataType == DataType.BOOLEAN || dataType == DataType.INT || dataType == DataType.BIGINT;
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
