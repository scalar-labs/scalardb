package com.scalar.db.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.common.AbstractDistributedStorage;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
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

  private final BasicDataSource dataSource;
  private final BasicDataSource tableMetadataDataSource;
  private final RdbEngineStrategy rdbEngine;
  private final JdbcService jdbcService;

  @Inject
  public JdbcDatabase(DatabaseConfig databaseConfig) {
    super(databaseConfig);
    JdbcConfig config = new JdbcConfig(databaseConfig);

    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSource(config, rdbEngine);

    tableMetadataDataSource = JdbcUtils.initDataSourceForTableMetadata(config, rdbEngine);
    TableMetadataManager tableMetadataManager =
        new TableMetadataManager(
            new JdbcAdmin(tableMetadataDataSource, config),
            databaseConfig.getMetadataCacheExpirationTimeSecs());

    OperationChecker operationChecker = new OperationChecker(databaseConfig, tableMetadataManager);
    jdbcService = new JdbcService(tableMetadataManager, operationChecker, rdbEngine);
  }

  @VisibleForTesting
  JdbcDatabase(
      DatabaseConfig databaseConfig,
      BasicDataSource dataSource,
      BasicDataSource tableMetadataDataSource,
      RdbEngineStrategy rdbEngine,
      JdbcService jdbcService) {
    super(databaseConfig);
    this.dataSource = dataSource;
    this.tableMetadataDataSource = tableMetadataDataSource;
    this.jdbcService = jdbcService;
    this.rdbEngine = rdbEngine;
  }

  @Override
  public Optional<Result> get(Get get) throws ExecutionException {
    get = copyAndSetTargetToIfNot(get);
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      return jdbcService.get(get, connection);
    } catch (SQLException e) {
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
      return jdbcService.getScanner(scan, connection);
    } catch (SQLException e) {
      close(connection);
      throw new ExecutionException(
          CoreError.JDBC_ERROR_OCCURRED_IN_SELECTION.buildMessage(e.getMessage()), e);
    }
  }

  @Override
  public void put(Put put) throws ExecutionException {
    put = copyAndSetTargetToIfNot(put);
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      if (!jdbcService.put(put, connection)) {
        throw new NoMutationException(CoreError.NO_MUTATION_APPLIED.buildMessage());
      }
    } catch (SQLException e) {
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
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      if (!jdbcService.delete(delete, connection)) {
        throw new NoMutationException(CoreError.NO_MUTATION_APPLIED.buildMessage());
      }
    } catch (SQLException e) {
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
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      close(connection);
      throw new ExecutionException(
          CoreError.JDBC_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    }

    try {
      if (!jdbcService.mutate(mutations, connection)) {
        try {
          connection.rollback();
        } catch (SQLException e) {
          throw new ExecutionException(
              CoreError.JDBC_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
        }
        throw new NoMutationException(CoreError.NO_MUTATION_APPLIED.buildMessage());
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
