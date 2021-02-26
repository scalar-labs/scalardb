package com.scalar.db.transaction.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.SerializableStrategy;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.storage.jdbc.JdbcDatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcService;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.storage.jdbc.checker.OperationChecker;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.sql.SQLException;
import java.util.Optional;

@ThreadSafe
public class JdbcTransactionManager implements DistributedTransactionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcTransactionManager.class);

  private final BasicDataSource dataSource;
  private final JdbcService jdbcService;
  private Optional<String> namespace;
  private Optional<String> tableName;

  @Inject
  public JdbcTransactionManager(JdbcDatabaseConfig config) {
    dataSource = JdbcUtils.initDataSource(config, true);
    Optional<String> namespacePrefix = config.getNamespacePrefix();
    RdbEngine rdbEngine = JdbcUtils.getRdbEngine(config.getContactPoints().get(0));
    TableMetadataManager tableMetadataManager =
        new TableMetadataManager(dataSource, namespacePrefix, rdbEngine);
    OperationChecker operationChecker = new OperationChecker(tableMetadataManager);
    QueryBuilder queryBuilder = new QueryBuilder(tableMetadataManager, rdbEngine);
    jdbcService = new JdbcService(operationChecker, queryBuilder, namespacePrefix);
    namespace = Optional.empty();
    tableName = Optional.empty();
  }

  @VisibleForTesting
  JdbcTransactionManager(BasicDataSource dataSource, JdbcService jdbcService) {
    this.dataSource = dataSource;
    this.jdbcService = jdbcService;
  }

  @Override
  public void with(String namespace, String tableName) {
    this.namespace = Optional.ofNullable(namespace);
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public void withNamespace(String namespace) {
    this.namespace = Optional.ofNullable(namespace);
  }

  @Override
  public Optional<String> getNamespace() {
    return namespace;
  }

  @Override
  public void withTable(String tableName) {
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public Optional<String> getTable() {
    return tableName;
  }

  @Override
  public JdbcTransaction start() throws TransactionException {
    try {
      return new JdbcTransaction(jdbcService, dataSource.getConnection(), namespace, tableName);
    } catch (SQLException e) {
      throw new TransactionException("failed to start the transaction", e);
    }
  }

  @Override
  public JdbcTransaction start(String txId) throws TransactionException {
    throw new UnsupportedOperationException("doesn't support starting transaction with txId");
  }

  @Deprecated
  @Override
  public JdbcTransaction start(Isolation isolation) throws TransactionException {
    return start();
  }

  @Deprecated
  @Override
  public JdbcTransaction start(String txId, Isolation isolation) throws TransactionException {
    throw new UnsupportedOperationException("doesn't support starting transaction with txId");
  }

  @Deprecated
  @Override
  public JdbcTransaction start(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    return start();
  }

  @Deprecated
  @Override
  public JdbcTransaction start(SerializableStrategy strategy) throws TransactionException {
    return start();
  }

  @Deprecated
  @Override
  public JdbcTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException {
    throw new UnsupportedOperationException("doesn't support starting transaction with txId");
  }

  @Deprecated
  @Override
  public JdbcTransaction start(String txId, Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    throw new UnsupportedOperationException("doesn't support starting transaction with txId");
  }

  @Override
  public TransactionState getState(String txId) {
    throw new UnsupportedOperationException("doesn't support this operation");
  }

  @Override
  public void close() {
    try {
      dataSource.close();
    } catch (SQLException e) {
      LOGGER.warn("failed to close the dataSource", e);
    }
  }
}
