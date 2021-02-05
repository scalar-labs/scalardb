package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.storage.jdbc.JdbcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

/**
 * This indicates a transaction session of JDBC
 *
 * <p>Note that the condition of a mutation is ignored in this implementation. We can use the
 * transaction feature instead of the conditional update
 *
 * @author Toshihiro Suzuki
 */
@NotThreadSafe
public class JdbcTransaction implements DistributedTransaction {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcTransaction.class);

  private final JdbcService jdbcService;
  private final Connection connection;
  private Optional<String> namespace;
  private Optional<String> tableName;
  private boolean isCommitCalled;

  JdbcTransaction(
      JdbcService jdbcService,
      Connection connection,
      Optional<String> namespace,
      Optional<String> tableName) {
    this.jdbcService = jdbcService;
    this.connection = connection;
    this.namespace = namespace;
    this.tableName = tableName;
  }

  @Override
  public String getId() {
    throw new UnsupportedOperationException("doesn't support this operation");
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
  public Optional<Result> get(Get get) throws CrudException {
    try {
      return jdbcService.get(get, connection, namespace, tableName);
    } catch (SQLException e) {
      throw new CrudException("get operation failed", e);
    }
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    try {
      return jdbcService.scan(scan, connection, namespace, tableName).all();
    } catch (SQLException | ExecutionException e) {
      throw new CrudException("scan operation failed", e);
    }
  }

  @Override
  public void put(Put put) throws CrudException {
    // Ignore the condition in the put
    if (put.getCondition().isPresent()) {
      LOGGER.warn("ignoring the condition of the mutation: " + put);
      put.withCondition(null);
    }

    try {
      jdbcService.put(put, connection, namespace, tableName);
    } catch (SQLException e) {
      throw new CrudException("put operation failed", e);
    }
  }

  @Override
  public void put(List<Put> puts) throws CrudException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    // Ignore the condition in the delete
    if (delete.getCondition().isPresent()) {
      LOGGER.warn("ignoring the condition of the mutation: " + delete);
      delete.withCondition(null);
    }

    try {
      jdbcService.delete(delete, connection, namespace, tableName);
    } catch (SQLException e) {
      throw new CrudException("delete operation failed", e);
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    // Ignore the conditions in the mutations
    mutations.forEach(
        m ->
            m.getCondition()
                .ifPresent(
                    c -> {
                      LOGGER.warn("ignoring the condition of the mutation: " + m);
                      m.withCondition(null);
                    }));

    try {
      jdbcService.mutate(mutations, connection, namespace, tableName, true);
    } catch (SQLException e) {
      throw new CrudException("mutate operation failed", e);
    }
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    try {
      connection.commit();
    } catch (SQLException e) {
      try {
        connection.rollback();
      } catch (SQLException sqlException) {
        throw new UnknownTransactionStatusException("failed to rollback", sqlException);
      }
      throw new CommitException("failed to commit", e);
    } finally {
      try {
        connection.close();
      } catch (SQLException e) {
        LOGGER.warn("failed to close the connection", e);
      }
      isCommitCalled = true;
    }
  }

  @Override
  public void abort() throws AbortException {
    if (isCommitCalled) {
      // If the commit method is already called, do nothing here
      return;
    }

    try {
      connection.rollback();
    } catch (SQLException e) {
      throw new AbortException("failed to rollback", e);
    } finally {
      try {
        connection.close();
      } catch (SQLException e) {
        LOGGER.warn("failed to close the connection", e);
      }
    }
  }
}
