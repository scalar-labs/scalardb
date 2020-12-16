package com.scalar.db.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

/**
 * A storage implementation with JDBC for {@link DistributedStorage}.
 *
 * <p>Note that the consistency in an operation is always LINEARIZABLE in this implementation. Even
 * if consistency is specified in an operation, it will be ignored.
 *
 * @author Toshihiro Suzuki
 */
@ThreadSafe
public class JDBC implements DistributedStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(JDBC.class);

  private final BasicDataSource dataSource;
  private final JDBCService jdbcService;
  @Nullable private String defaultSchema;
  @Nullable private String defaultTable;

  @Inject
  public JDBC(DatabaseConfig config) {
    dataSource = JDBCUtils.initDataSource(config);

    String schemaPrefix = config.getNamespacePrefix().orElse("");

    TableMetadataManager tableMetadataManager = new TableMetadataManager(dataSource, schemaPrefix);
    OperationChecker operationChecker = new OperationChecker(tableMetadataManager);
    QueryBuilder queryBuilder =
        new QueryBuilder(
            tableMetadataManager, JDBCUtils.getRDBType(config.getContactPoints().get(0)));
    jdbcService = new JDBCService(operationChecker, queryBuilder, schemaPrefix);
  }

  @VisibleForTesting
  JDBC(BasicDataSource dataSource, JDBCService jdbcService) {
    this.dataSource = dataSource;
    this.jdbcService = jdbcService;
  }

  @Override
  public void with(String namespace, String tableName) {
    defaultSchema = namespace;
    defaultTable = tableName;
  }

  @Override
  public void withNamespace(String namespace) {
    defaultSchema = namespace;
  }

  @Override
  public Optional<String> getNamespace() {
    return Optional.ofNullable(defaultSchema);
  }

  @Override
  public void withTable(String tableName) {
    defaultTable = tableName;
  }

  @Override
  public Optional<String> getTable() {
    return Optional.ofNullable(defaultTable);
  }

  @Override
  public Optional<Result> get(Get get) throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      return jdbcService.get(get, connection, defaultSchema, defaultTable);
    } catch (SQLException e) {
      throw new ExecutionException("An error occurred", e);
    }
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      return jdbcService.scan(scan, connection, defaultSchema, defaultTable);
    } catch (SQLException e) {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException sqlException) {
        throw new ExecutionException("An error occurred", sqlException);
      }
      throw new ExecutionException("An error occurred", e);
    }
  }

  @Override
  public void put(Put put) throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      if (!jdbcService.put(put, connection, defaultSchema, defaultTable)) {
        throw new NoMutationException("no mutation was applied");
      }
    } catch (SQLException e) {
      throw new ExecutionException("An error occurred", e);
    }
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    try (Connection connection = dataSource.getConnection()) {
      if (!jdbcService.delete(delete, connection, defaultSchema, defaultTable)) {
        throw new NoMutationException("no mutation was applied");
      }
    } catch (SQLException e) {
      throw new ExecutionException("An error occurred", e);
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws ExecutionException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws ExecutionException {
    Connection connection = null;
    try {
      try {
        connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        if (!jdbcService.mutate(mutations, connection, defaultSchema, defaultTable)) {
          connection.rollback();
          throw new NoMutationException("no mutation was applied");
        } else {
          connection.commit();
        }
      } catch (SQLException e) {
        if (connection != null) {
          connection.rollback();
        }
        throw new ExecutionException("An error occurred", e);
      } finally {
        if (connection != null) {
          connection.close();
        }
      }
    } catch (SQLException e) {
      throw new ExecutionException("An error occurred", e);
    }
  }

  @Override
  public void close() {
    try {
      dataSource.close();
    } catch (SQLException e) {
      LOGGER.error("Failed to close the dataSource", e);
    }
  }
}
