package com.scalar.db.transaction.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.SerializableStrategy;
import com.scalar.db.api.TransactionState;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.storage.jdbc.JDBCService;
import com.scalar.db.storage.jdbc.JDBCUtils;
import com.scalar.db.storage.jdbc.OperationChecker;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.sql.SQLException;
import java.util.Optional;

/**
 * JDBC transaction manager
 *
 * <p>Note that the condition of a mutation is ignored in this implementation. We can use the
 * transaction feature instead of the conditional update
 *
 * @author Toshihiro Suzuki
 */
@ThreadSafe
public class JDBCTransactionManager implements DistributedTransactionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(JDBCTransactionManager.class);

  private final BasicDataSource dataSource;
  private final JDBCService jdbcService;
  @Nullable private String defaultSchema;
  @Nullable private String defaultTable;

  @Inject
  public JDBCTransactionManager(DatabaseConfig config) {
    dataSource = JDBCUtils.initDataSource(config, false);

    TableMetadataManager tableMetadataManager = new TableMetadataManager(dataSource);
    OperationChecker operationChecker = new OperationChecker(tableMetadataManager);
    QueryBuilder queryBuilder =
        new QueryBuilder(
            tableMetadataManager, JDBCUtils.getRDBType(config.getContactPoints().get(0)));
    String schemaPrefix = config.getNamespacePrefix().orElse("");
    jdbcService = new JDBCService(operationChecker, queryBuilder, schemaPrefix);
  }

  @VisibleForTesting
  JDBCTransactionManager(BasicDataSource dataSource, JDBCService jdbcService) {
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
  public JDBCTransaction start() throws TransactionException {
    try {
      return new JDBCTransaction(
          jdbcService, dataSource.getConnection(), defaultSchema, defaultTable);
    } catch (SQLException e) {
      throw new TransactionException("Failed to start the transaction", e);
    }
  }

  @Override
  public JDBCTransaction start(String txId) throws TransactionException {
    throw new UnsupportedOperationException("Doesn't support starting transaction with txId");
  }

  @Deprecated
  @Override
  public JDBCTransaction start(Isolation isolation) throws TransactionException {
    return start();
  }

  @Deprecated
  @Override
  public JDBCTransaction start(String txId, Isolation isolation) throws TransactionException {
    throw new UnsupportedOperationException("Doesn't support starting transaction with txId");
  }

  @Deprecated
  @Override
  public JDBCTransaction start(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    return start();
  }

  @Deprecated
  @Override
  public JDBCTransaction start(SerializableStrategy strategy) throws TransactionException {
    return start();
  }

  @Deprecated
  @Override
  public JDBCTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException {
    throw new UnsupportedOperationException("Doesn't support starting transaction with txId");
  }

  @Deprecated
  @Override
  public JDBCTransaction start(String txId, Isolation isolation, SerializableStrategy strategy)
      throws TransactionException {
    throw new UnsupportedOperationException("Doesn't support starting transaction with txId");
  }

  @Override
  public TransactionState getState(String txId) {
    throw new UnsupportedOperationException("Doesn't support this operation");
  }

  @Override
  public void close() {
    try {
      dataSource.close();
    } catch (SQLException e) {
      LOGGER.warn("Failed to close the dataSource", e);
    }
  }
}
