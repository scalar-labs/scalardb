package com.scalar.db.transaction.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.SerializableStrategy;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.storage.common.checker.OperationChecker;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcService;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.transaction.common.AbstractDistributedTransactionManager;
import java.sql.SQLException;
import java.util.UUID;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class JdbcTransactionManager extends AbstractDistributedTransactionManager {
  private static final Logger logger = LoggerFactory.getLogger(JdbcTransactionManager.class);

  private final BasicDataSource dataSource;
  private final BasicDataSource tableMetadataDataSource;
  private final RdbEngine rdbEngine;
  private final JdbcService jdbcService;

  @Inject
  public JdbcTransactionManager(DatabaseConfig databaseConfig) {
    super(databaseConfig);
    JdbcConfig config = new JdbcConfig(databaseConfig);

    dataSource = JdbcUtils.initDataSource(config, true);
    rdbEngine = JdbcUtils.getRdbEngine(config.getJdbcUrl());

    tableMetadataDataSource = JdbcUtils.initDataSourceForTableMetadata(config);
    TableMetadataManager tableMetadataManager =
        new TableMetadataManager(
            new JdbcAdmin(tableMetadataDataSource, config),
            databaseConfig.getMetadataCacheExpirationTimeSecs());

    OperationChecker operationChecker = new OperationChecker(tableMetadataManager);
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);
    jdbcService = new JdbcService(tableMetadataManager, operationChecker, queryBuilder);
  }

  @VisibleForTesting
  JdbcTransactionManager(
      DatabaseConfig databaseConfig,
      BasicDataSource dataSource,
      BasicDataSource tableMetadataDataSource,
      RdbEngine rdbEngine,
      JdbcService jdbcService) {
    super(databaseConfig);
    this.dataSource = dataSource;
    this.tableMetadataDataSource = tableMetadataDataSource;
    this.rdbEngine = rdbEngine;
    this.jdbcService = jdbcService;
  }

  @Override
  public DistributedTransaction begin() throws TransactionException {
    String txId = UUID.randomUUID().toString();
    return begin(txId);
  }

  @Override
  public DistributedTransaction begin(String txId) throws TransactionException {
    try {
      JdbcTransaction transaction =
          new JdbcTransaction(txId, jdbcService, dataSource.getConnection(), rdbEngine);
      getNamespace().ifPresent(transaction::withNamespace);
      getTable().ifPresent(transaction::withTable);
      return new ActiveTransaction(transaction);
    } catch (SQLException e) {
      throw new TransactionException("failed to start the transaction", e);
    }
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation) throws TransactionException {
    return begin();
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, Isolation isolation)
      throws TransactionException {
    return begin(txId);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    return begin();
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(SerializableStrategy strategy) throws TransactionException {
    return begin();
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException {
    return begin(txId);
  }

  /** @deprecated As of release 2.4.0. Will be removed in release 4.0.0. */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Override
  public DistributedTransaction start(
      String txId, Isolation isolation, SerializableStrategy strategy) throws TransactionException {
    return begin(txId);
  }

  @Override
  public TransactionState getState(String txId) {
    throw new UnsupportedOperationException("this method is not supported in JDBC transaction");
  }

  @Override
  public TransactionState rollback(String txId) {
    throw new UnsupportedOperationException("this method is not supported in JDBC transaction");
  }

  @Override
  public void close() {
    try {
      dataSource.close();
    } catch (SQLException e) {
      logger.warn("failed to close the dataSource", e);
    }
    try {
      tableMetadataDataSource.close();
    } catch (SQLException e) {
      logger.warn("failed to close the table metadata dataSource", e);
    }
  }
}
