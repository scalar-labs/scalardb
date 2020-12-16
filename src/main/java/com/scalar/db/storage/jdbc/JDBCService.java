package com.scalar.db.storage.jdbc;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection;
import com.scalar.db.io.Key;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.SelectQuery;

import javax.annotation.Nullable;
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
public class JDBCService {

  private final OperationChecker operationChecker;
  private final QueryBuilder queryBuilder;
  private final String schemaPrefix;

  public JDBCService(
      OperationChecker operationChecker, QueryBuilder queryBuilder, String schemaPrefix) {
    this.operationChecker = Objects.requireNonNull(operationChecker);
    this.queryBuilder = Objects.requireNonNull(queryBuilder);
    this.schemaPrefix = Objects.requireNonNull(schemaPrefix);
  }

  private Table getTable(
      Operation operation, @Nullable String defaultSchema, @Nullable String defaultTable) {
    String schema = operation.forNamespace().orElse(defaultSchema);
    String table = operation.forTable().orElse(defaultTable);
    return new Table(schemaPrefix, schema, table);
  }

  public Optional<Result> get(
      Get get, Connection connection, @Nullable String defaultSchema, @Nullable String defaultTable)
      throws SQLException {
    addProjectionsForKeys(get);
    Table table = getTable(get, defaultSchema, defaultTable);
    @Nullable Key clusteringKey = get.getClusteringKey().orElse(null);

    operationChecker.checkGet(table, get.getProjections(), get.getPartitionKey(), clusteringKey);

    SelectQuery selectQuery =
        queryBuilder
            .select(get.getProjections())
            .from(table)
            .where(get.getPartitionKey(), clusteringKey)
            .build();

    PreparedStatement preparedStatement = selectQuery.prepareAndBind(connection);
    try (ResultSet resultSet = preparedStatement.executeQuery()) {
      if (resultSet.next()) {
        return Optional.of(selectQuery.getResult(resultSet));
      }
      return Optional.empty();
    }
  }

  public Scanner scan(
      Scan scan,
      Connection connection,
      @Nullable String defaultSchema,
      @Nullable String defaultTable)
      throws SQLException {
    addProjectionsForKeys(scan);
    Table table = getTable(scan, defaultSchema, defaultTable);

    @Nullable Key startClusteringKey = scan.getStartClusteringKey().orElse(null);
    @Nullable Key endClusteringKey = scan.getEndClusteringKey().orElse(null);

    operationChecker.checkScan(
        table,
        scan.getProjections(),
        scan.getPartitionKey(),
        startClusteringKey,
        endClusteringKey,
        scan.getLimit(),
        scan.getOrderings());

    SelectQuery selectQuery =
        queryBuilder
            .select(scan.getProjections())
            .from(table)
            .where(
                scan.getPartitionKey(),
                startClusteringKey,
                scan.getStartInclusive(),
                endClusteringKey,
                scan.getEndInclusive())
            .orderBy(scan.getOrderings())
            .limit(scan.getLimit())
            .build();

    PreparedStatement preparedStatement = selectQuery.prepareAndBind(connection);
    ResultSet resultSet = preparedStatement.executeQuery();
    return new ScannerImpl(selectQuery, connection, resultSet);
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
      Put put, Connection connection, @Nullable String defaultSchema, @Nullable String defaultTable)
      throws SQLException {
    Table table = getTable(put, defaultSchema, defaultTable);
    @Nullable Key clusteringKey = put.getClusteringKey().orElse(null);
    @Nullable MutationCondition condition = put.getCondition().orElse(null);

    operationChecker.checkPut(
        table, put.getPartitionKey(), clusteringKey, put.getValues(), condition);

    if (condition == null) {
      queryBuilder
          .upsertInto(table)
          .values(put.getPartitionKey(), clusteringKey, put.getValues())
          .build()
          .prepareAndBind(connection)
          .executeUpdate();
      return true;
    } else {
      return new ConditionalUpdater(
              connection,
              queryBuilder,
              table,
              put.getPartitionKey(),
              clusteringKey,
              condition,
              put.getValues())
          .update();
    }
  }

  public boolean delete(
      Delete delete,
      Connection connection,
      @Nullable String defaultSchema,
      @Nullable String defaultTable)
      throws SQLException {
    Table table = getTable(delete, defaultSchema, defaultTable);
    @Nullable Key clusteringKey = delete.getClusteringKey().orElse(null);
    @Nullable MutationCondition condition = delete.getCondition().orElse(null);

    operationChecker.checkDelete(table, delete.getPartitionKey(), clusteringKey, condition);

    if (condition == null) {
      queryBuilder
          .deleteFrom(table)
          .where(delete.getPartitionKey(), clusteringKey)
          .build()
          .prepareAndBind(connection)
          .executeUpdate();
      return true;
    } else {
      return new ConditionalUpdater(
              connection,
              queryBuilder,
              table,
              delete.getPartitionKey(),
              clusteringKey,
              condition,
              null)
          .update();
    }
  }

  public boolean mutate(
      List<? extends Mutation> mutations,
      Connection connection,
      @Nullable String defaultSchema,
      @Nullable String defaultTable)
      throws SQLException {
    operationChecker.checkMutate(mutations);

    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        if (!put((Put) mutation, connection, defaultSchema, defaultTable)) {
          return false;
        }
      } else {
        if (!delete((Delete) mutation, connection, defaultSchema, defaultTable)) {
          return false;
        }
      }
    }
    return true;
  }
}
