package com.scalar.db.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteBuilder;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.VirtualTableInfo;
import com.scalar.db.api.VirtualTableJoinType;
import com.scalar.db.common.AbstractDistributedStorage;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.VirtualTableInfoManager;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.util.ScalarDbUtils;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A storage implementation with JDBC for {@link DistributedStorage}.
 *
 * <p>Note that the consistency in an operation is always LINEARIZABLE in this implementation. Even
 * if consistency is specified in an operation, it will be ignored.
 *
 * @author Toshihiro Suzuki
 */
@ThreadSafe
public class JdbcDatabase extends AbstractDistributedStorage {
  private static final Logger logger = LoggerFactory.getLogger(JdbcDatabase.class);

  private final RdbEngineStrategy rdbEngine;
  private final BasicDataSource dataSource;
  private final BasicDataSource tableMetadataDataSource;
  private final TableMetadataManager tableMetadataManager;
  private final VirtualTableInfoManager virtualTableInfoManager;
  private final JdbcService jdbcService;
  private final boolean requiresExplicitCommit;

  @Inject
  public JdbcDatabase(DatabaseConfig databaseConfig) {
    super(databaseConfig);
    JdbcConfig config = new JdbcConfig(databaseConfig);

    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSource(config, rdbEngine);
    requiresExplicitCommit = JdbcUtils.requiresExplicitCommit(dataSource, rdbEngine);

    tableMetadataDataSource = JdbcUtils.initDataSourceForTableMetadata(config, rdbEngine);
    JdbcAdmin jdbcAdmin = new JdbcAdmin(tableMetadataDataSource, config, requiresExplicitCommit);
    tableMetadataManager =
        new TableMetadataManager(jdbcAdmin, databaseConfig.getMetadataCacheExpirationTimeSecs());
    OperationChecker operationChecker =
        new JdbcOperationChecker(
            databaseConfig, tableMetadataManager, new StorageInfoProvider(jdbcAdmin), rdbEngine);

    jdbcService =
        new JdbcService(
            tableMetadataManager, operationChecker, rdbEngine, databaseConfig.getScanFetchSize());

    virtualTableInfoManager =
        new VirtualTableInfoManager(jdbcAdmin, databaseConfig.getMetadataCacheExpirationTimeSecs());
  }

  @VisibleForTesting
  JdbcDatabase(
      DatabaseConfig databaseConfig,
      RdbEngineStrategy rdbEngine,
      BasicDataSource dataSource,
      BasicDataSource tableMetadataDataSource,
      TableMetadataManager tableMetadataManager,
      VirtualTableInfoManager virtualTableInfoManager,
      JdbcService jdbcService,
      boolean requiresExplicitCommit) {
    super(databaseConfig);
    this.dataSource = dataSource;
    this.tableMetadataDataSource = tableMetadataDataSource;
    this.jdbcService = jdbcService;
    this.rdbEngine = rdbEngine;
    this.tableMetadataManager = tableMetadataManager;
    this.virtualTableInfoManager = virtualTableInfoManager;
    this.requiresExplicitCommit = requiresExplicitCommit;
  }

  @Override
  public Optional<Result> get(Get get) throws ExecutionException {
    get = copyAndSetTargetToIfNot(get);
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      if (requiresExplicitCommit) {
        connection.setAutoCommit(false);
      }
      rdbEngine.setConnectionToReadOnly(connection, true);
      Optional<Result> result = jdbcService.get(get, connection);
      if (requiresExplicitCommit) {
        connection.commit();
      }
      return result;
    } catch (SQLException e) {
      try {
        if (connection != null && requiresExplicitCommit) {
          connection.rollback();
        }
      } catch (SQLException ex) {
        e.addSuppressed(ex);
      }
      throw new ExecutionException(
          CoreError.JDBC_ERROR_OCCURRED_IN_SELECTION.buildMessage(e.getMessage()), e);
    } finally {
      close(connection);
    }
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    scan = copyAndSetTargetToIfNot(scan);
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      connection.setAutoCommit(false);
      rdbEngine.setConnectionToReadOnly(connection, true);
      return jdbcService.getScanner(scan, connection);
    } catch (SQLException e) {
      try {
        if (connection != null) {
          connection.rollback();
        }
      } catch (SQLException ex) {
        e.addSuppressed(ex);
      }

      close(connection);
      throw new ExecutionException(
          CoreError.JDBC_ERROR_OCCURRED_IN_SELECTION.buildMessage(e.getMessage()), e);
    } catch (Exception e) {
      try {
        if (connection != null) {
          connection.rollback();
        }
      } catch (SQLException ex) {
        e.addSuppressed(ex);
      }

      close(connection);
      throw e;
    }
  }

  @Override
  public void put(Put put) throws ExecutionException {
    put = copyAndSetTargetToIfNot(put);

    VirtualTableInfo virtualTableInfo = getVirtualTableInfo(put);
    if (virtualTableInfo != null) {
      // For a virtual table
      List<Put> dividedPuts = dividePutForSourceTables(put, virtualTableInfo);
      mutateInternal(dividedPuts);
      return;
    }

    // For a regular table
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      if (requiresExplicitCommit) {
        connection.setAutoCommit(false);
      }
      if (!jdbcService.put(put, connection)) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(put));
      }
      if (requiresExplicitCommit) {
        connection.commit();
      }
    } catch (SQLException e) {
      try {
        if (connection != null && requiresExplicitCommit) {
          connection.rollback();
        }
      } catch (SQLException ex) {
        e.addSuppressed(ex);
      }
      throw new ExecutionException(
          CoreError.JDBC_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
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
    delete = copyAndSetTargetToIfNot(delete);

    VirtualTableInfo virtualTableInfo = getVirtualTableInfo(delete);
    if (virtualTableInfo != null) {
      // For a virtual table
      List<Delete> dividedDeletes = divideDeleteForSourceTables(delete, virtualTableInfo);
      mutateInternal(dividedDeletes);
      return;
    }

    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      if (requiresExplicitCommit) {
        connection.setAutoCommit(false);
      }
      if (!jdbcService.delete(delete, connection)) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(delete));
      }
      if (requiresExplicitCommit) {
        connection.commit();
      }
    } catch (SQLException e) {
      try {
        if (connection != null && requiresExplicitCommit) {
          connection.rollback();
        }
      } catch (SQLException ex) {
        e.addSuppressed(ex);
      }
      throw new ExecutionException(
          CoreError.JDBC_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
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
    if (mutations.size() == 1) {
      Mutation mutation = mutations.get(0);
      if (mutation instanceof Put) {
        put((Put) mutation);
        return;
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
        return;
      }
    }

    mutations = copyAndSetTargetToIfNot(mutations);

    List<Mutation> convertedMutations = new ArrayList<>();
    for (Mutation mutation : mutations) {
      VirtualTableInfo virtualTableInfo = getVirtualTableInfo(mutation);
      if (virtualTableInfo != null) {
        // For a virtual table
        if (mutation instanceof Put) {
          Put put = (Put) mutation;
          List<Put> dividedPuts = dividePutForSourceTables(put, virtualTableInfo);
          convertedMutations.addAll(dividedPuts);
        } else {
          assert mutation instanceof Delete;
          Delete delete = (Delete) mutation;
          List<Delete> dividedDeletes = divideDeleteForSourceTables(delete, virtualTableInfo);
          convertedMutations.addAll(dividedDeletes);
        }
      } else {
        // For a regular table
        convertedMutations.add(mutation);
      }
    }

    mutateInternal(convertedMutations);
  }

  private void mutateInternal(List<? extends Mutation> mutations) throws ExecutionException {
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      close(connection);
      throw new ExecutionException(
          CoreError.JDBC_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    } catch (Exception e) {
      close(connection);
      throw e;
    }

    try {
      if (!jdbcService.mutate(mutations, connection)) {
        try {
          connection.rollback();
        } catch (SQLException e) {
          throw new ExecutionException(
              CoreError.JDBC_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
        }
        throw new NoMutationException(CoreError.NO_MUTATION_APPLIED.buildMessage(), mutations);
      } else {
        connection.commit();
      }
    } catch (SQLException e) {
      try {
        connection.rollback();
      } catch (SQLException sqlException) {
        throw new ExecutionException(
            CoreError.JDBC_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
      }
      if (rdbEngine.isConflict(e)) {
        // Since a mutate operation executes multiple put/delete operations in a transaction,
        // conflicts can occur. Throw RetriableExecutionException in that case.
        throw new RetriableExecutionException(
            CoreError.JDBC_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()),
            e);
      }
      throw new ExecutionException(
          CoreError.JDBC_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    } finally {
      close(connection);
    }
  }

  @Nullable
  private VirtualTableInfo getVirtualTableInfo(Operation operation) throws ExecutionException {
    assert operation.forNamespace().isPresent() && operation.forTable().isPresent();
    return virtualTableInfoManager.getVirtualTableInfo(
        operation.forNamespace().get(), operation.forTable().get());
  }

  private List<Put> dividePutForSourceTables(Put put, VirtualTableInfo virtualTableInfo)
      throws ExecutionException {
    TableMetadata leftSourceTableMetadata =
        tableMetadataManager.getTableMetadata(
            virtualTableInfo.getLeftSourceNamespaceName(),
            virtualTableInfo.getLeftSourceTableName());
    TableMetadata rightSourceTableMetadata =
        tableMetadataManager.getTableMetadata(
            virtualTableInfo.getRightSourceNamespaceName(),
            virtualTableInfo.getRightSourceTableName());
    assert leftSourceTableMetadata != null && rightSourceTableMetadata != null;

    Map<String, Column<?>> columns = put.getColumns();

    // Build Put for the left source table
    PutBuilder.BuildableFromExisting putBuilderForLeftSourceTable =
        Put.newBuilder(put)
            .namespace(virtualTableInfo.getLeftSourceNamespaceName())
            .table(virtualTableInfo.getLeftSourceTableName())
            .clearValues()
            .clearCondition();

    // Add columns that belong to the left source table
    for (String columnName : leftSourceTableMetadata.getColumnNames()) {
      if (columns.containsKey(columnName)) {
        putBuilderForLeftSourceTable.value(columns.get(columnName));
      }
    }

    // Build Put for the right source table
    PutBuilder.BuildableFromExisting putBuilderForRightSourceTable =
        Put.newBuilder(put)
            .namespace(virtualTableInfo.getRightSourceNamespaceName())
            .table(virtualTableInfo.getRightSourceTableName())
            .clearValues()
            .clearCondition();

    // Add columns that belong to the right source table
    for (String columnName : rightSourceTableMetadata.getColumnNames()) {
      if (columns.containsKey(columnName)) {
        putBuilderForRightSourceTable.value(columns.get(columnName));
      }
    }

    // Handle conditions
    if (put.getCondition().isPresent()) {
      MutationCondition condition = put.getCondition().get();
      if (condition instanceof PutIfExists || condition instanceof PutIfNotExists) {
        // For PutIfExists/PutIfNotExists, apply based on the join type
        if (virtualTableInfo.getJoinType() == VirtualTableJoinType.INNER) {
          // For INNER join, apply the condition to both source tables
          putBuilderForLeftSourceTable.condition(condition);
          putBuilderForRightSourceTable.condition(condition);
        } else {
          // For LEFT_OUTER join, apply the condition only to the left source table
          putBuilderForLeftSourceTable.condition(condition);
        }
      } else if (condition instanceof PutIf) {
        // For PutIf, divide the conditional expressions based on which table the columns belong to
        PutIf putIf = (PutIf) condition;
        List<ConditionalExpression> leftExpressions = new ArrayList<>();
        List<ConditionalExpression> rightExpressions = new ArrayList<>();

        for (ConditionalExpression expression : putIf.getExpressions()) {
          String columnName = expression.getColumn().getName();
          if (leftSourceTableMetadata.getColumnNames().contains(columnName)) {
            leftExpressions.add(expression);
          } else if (rightSourceTableMetadata.getColumnNames().contains(columnName)) {
            rightExpressions.add(expression);
          }
        }

        if (!leftExpressions.isEmpty()) {
          putBuilderForLeftSourceTable.condition(ConditionBuilder.putIf(leftExpressions));
        }
        if (!rightExpressions.isEmpty()) {
          if (isAllIsNullOnRightColumnsInLeftOuterJoin(virtualTableInfo, rightExpressions)
              && JdbcOperationAttributes
                  .isLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled(put)) {
            // In a LEFT_OUTER join, when all conditions on the right source table columns are
            // IS_NULL, we cannot distinguish whether we should check for the existence of a
            // right-side record with NULL values or for the case where the right-side record does
            // not exist at all. Therefore, this behavior is controlled by the operation attribute.
            // By default, we convert the condition to PutIfNotExists, assuming that the more common
            // use case is to check that the right-side record does not exist.
            putBuilderForRightSourceTable.condition(ConditionBuilder.putIfNotExists());
          } else {
            putBuilderForRightSourceTable.condition(ConditionBuilder.putIf(rightExpressions));
          }
        }
      }
    }

    Put putForLeftSourceTable = putBuilderForLeftSourceTable.build();
    Put putForRightSourceTable = putBuilderForRightSourceTable.build();
    return Arrays.asList(putForLeftSourceTable, putForRightSourceTable);
  }

  private List<Delete> divideDeleteForSourceTables(Delete delete, VirtualTableInfo virtualTableInfo)
      throws ExecutionException {
    TableMetadata leftSourceTableMetadata =
        tableMetadataManager.getTableMetadata(
            virtualTableInfo.getLeftSourceNamespaceName(),
            virtualTableInfo.getLeftSourceTableName());
    TableMetadata rightSourceTableMetadata =
        tableMetadataManager.getTableMetadata(
            virtualTableInfo.getRightSourceNamespaceName(),
            virtualTableInfo.getRightSourceTableName());
    assert leftSourceTableMetadata != null && rightSourceTableMetadata != null;

    // Build Delete for the left source table
    DeleteBuilder.BuildableFromExisting deleteBuilderForLeftSourceTable =
        Delete.newBuilder(delete)
            .namespace(virtualTableInfo.getLeftSourceNamespaceName())
            .table(virtualTableInfo.getLeftSourceTableName())
            .clearCondition();

    // Build Delete for the right source table
    DeleteBuilder.BuildableFromExisting deleteBuilderForRightSourceTable =
        Delete.newBuilder(delete)
            .namespace(virtualTableInfo.getRightSourceNamespaceName())
            .table(virtualTableInfo.getRightSourceTableName())
            .clearCondition();

    // Handle conditions
    if (delete.getCondition().isPresent()) {
      MutationCondition condition = delete.getCondition().get();
      if (condition instanceof DeleteIfExists) {
        // For DeleteIfExists, apply based on the join type
        if (virtualTableInfo.getJoinType() == VirtualTableJoinType.INNER) {
          // For INNER join, apply the condition to both source tables
          deleteBuilderForLeftSourceTable.condition(condition);
          deleteBuilderForRightSourceTable.condition(condition);
        } else {
          // For LEFT_OUTER join, apply the condition only to the left source table
          deleteBuilderForLeftSourceTable.condition(condition);
        }
      } else if (condition instanceof DeleteIf) {
        // For DeleteIf, divide the conditional expressions based on which table the columns belong
        // to
        DeleteIf deleteIf = (DeleteIf) condition;
        List<ConditionalExpression> leftExpressions = new ArrayList<>();
        List<ConditionalExpression> rightExpressions = new ArrayList<>();

        for (ConditionalExpression expression : deleteIf.getExpressions()) {
          String columnName = expression.getColumn().getName();
          if (leftSourceTableMetadata.getColumnNames().contains(columnName)) {
            leftExpressions.add(expression);
          } else if (rightSourceTableMetadata.getColumnNames().contains(columnName)) {
            rightExpressions.add(expression);
          }
        }

        if (!leftExpressions.isEmpty()) {
          deleteBuilderForLeftSourceTable.condition(ConditionBuilder.deleteIf(leftExpressions));
        }
        if (!rightExpressions.isEmpty()) {
          if (isAllIsNullOnRightColumnsInLeftOuterJoin(virtualTableInfo, rightExpressions)
              && !JdbcOperationAttributes
                  .isLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed(delete)) {
            // In a LEFT_OUTER join, when all conditions on the right source table columns are
            // IS_NULL, we cannot distinguish whether we should check for the existence of a
            // right-side record with NULL values or for the case where the right-side record does
            // not exist at all. This makes the delete operation semantically ambiguous. Therefore,
            // this behavior is controlled by the operation attribute. By default, we disallow this
            // operation to prevent unintended behavior.
            assert delete.forNamespace().isPresent() && delete.forTable().isPresent();
            throw new IllegalArgumentException(
                CoreError
                    .DELETE_IF_IS_NULL_FOR_RIGHT_SOURCE_TABLE_NOT_ALLOWED_FOR_LEFT_OUTER_VIRTUAL_TABLES
                    .buildMessage(
                        ScalarDbUtils.getFullTableName(
                            delete.forNamespace().get(), delete.forTable().get())));
          } else {
            deleteBuilderForRightSourceTable.condition(ConditionBuilder.deleteIf(rightExpressions));
          }
        }
      }
    }

    Delete deleteForLeftSourceTable = deleteBuilderForLeftSourceTable.build();
    Delete deleteForRightSourceTable = deleteBuilderForRightSourceTable.build();
    return Arrays.asList(deleteForLeftSourceTable, deleteForRightSourceTable);
  }

  private boolean isAllIsNullOnRightColumnsInLeftOuterJoin(
      VirtualTableInfo virtualTableInfo, List<ConditionalExpression> rightExpressions) {
    return virtualTableInfo.getJoinType() == VirtualTableJoinType.LEFT_OUTER
        && rightExpressions.stream()
            .allMatch(e -> e.getOperator() == ConditionalExpression.Operator.IS_NULL);
  }

  private void close(Connection connection) {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      logger.warn("Failed to close the connection", e);
    }
  }

  @Override
  public void close() {
    try {
      dataSource.close();
    } catch (SQLException e) {
      logger.warn("Failed to close the dataSource", e);
    }
    try {
      tableMetadataDataSource.close();
    } catch (SQLException e) {
      logger.warn("Failed to close the table metadata dataSource", e);
    }
  }
}
