package com.scalar.db.transaction.jdbc;

import static com.google.common.base.Preconditions.checkArgument;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.storage.jdbc.JdbcService;
import com.scalar.db.storage.jdbc.RdbEngine;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This indicates a transaction session of JDBC.
 *
 * <p>Note that the isolation level in a transaction is always SERIALIZABLE in this implementation.
 * Even if the isolation level is specified in the configuration, it will be ignored.
 *
 * @author Toshihiro Suzuki
 */
@NotThreadSafe
public class JdbcTransaction implements DistributedTransaction {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcTransaction.class);

  private final JdbcService jdbcService;
  private final Connection connection;
  private final RdbEngine rdbEngine;
  private Optional<String> namespace;
  private Optional<String> tableName;

  JdbcTransaction(
      JdbcService jdbcService,
      Connection connection,
      RdbEngine rdbEngine,
      Optional<String> namespace,
      Optional<String> tableName) {
    this.jdbcService = jdbcService;
    this.connection = connection;
    this.rdbEngine = rdbEngine;
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
      throw createCrudException(e, "put operation failed");
    }
  }

  @Override
  public void put(List<Put> puts) throws CrudException {
    checkArgument(puts.size() != 0);
    for (Put put : puts) {
      put(put);
    }
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
      throw createCrudException(e, "delete operation failed");
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    checkArgument(deletes.size() != 0);
    for (Delete delete : deletes) {
      delete(delete);
    }
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    checkArgument(mutations.size() != 0);
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        put((Put) mutation);
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
      }
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
      throw createCommitException(e);
    } finally {
      try {
        connection.close();
      } catch (SQLException e) {
        LOGGER.warn("failed to close the connection", e);
      }
    }
  }

  @Override
  public void abort() throws AbortException {
    try {
      if (connection.isClosed()) {
        // If the connection is already closed, do nothing here
        return;
      }

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

  private CrudException createCrudException(SQLException e, String message) {
    if (isConflictError(e)) {
      return new CrudConflictException("conflict happened; try restarting transaction", e);
    }
    return new CrudException(message, e);
  }

  private CommitException createCommitException(SQLException e) {
    if (isConflictError(e)) {
      return new CommitConflictException("conflict happened; try restarting transaction", e);
    }
    return new CommitException("failed to commit", e);
  }

  private boolean isConflictError(SQLException e) {
    switch (rdbEngine) {
      case MYSQL:
        if (e.getErrorCode() == 1213 || e.getErrorCode() == 1205) {
          // Deadlock found when trying to get lock or Lock wait timeout exceeded
          return true;
        }
        break;
      case POSTGRESQL:
        if (e.getSQLState().equals("40001") || e.getSQLState().equals("40P01")) {
          // Serialization error happened or Dead lock found
          return true;
        }
        break;
      case ORACLE:
        if (e.getErrorCode() == 8177 || e.getErrorCode() == 60) {
          // ORA-08177: can't serialize access for this transaction
          // ORA-00060: deadlock detected while waiting for resource
          return true;
        }
        break;
      default:
        break;
    }
    return false;
  }
}
