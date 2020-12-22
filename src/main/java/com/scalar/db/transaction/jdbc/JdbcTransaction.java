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
    throw new UnsupportedOperationException("Doesn't support this operation");
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
      throw new CrudException("An error occurred", e);
    }
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    try {
      return jdbcService.scan(scan, connection, namespace, tableName).all();
    } catch (SQLException | ExecutionException e) {
      throw new CrudException("An error occurred", e);
    }
  }

  @Override
  public void put(Put put) throws CrudException {
    // Ignore the condition
    if (put.getCondition().isPresent()) {
      LOGGER.warn("Ignoring the condition of the mutation: " + put);
      put.withCondition(null);
    }

    try {
      jdbcService.put(put, connection, namespace, tableName);
    } catch (SQLException e) {
      throw new CrudException("An error occurred", e);
    }
  }

  @Override
  public void put(List<Put> puts) throws CrudException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    // Ignore the condition
    if (delete.getCondition().isPresent()) {
      LOGGER.warn("Ignoring the condition of the mutation: " + delete);
      delete.withCondition(null);
    }

    try {
      jdbcService.delete(delete, connection, namespace, tableName);
    } catch (SQLException e) {
      throw new CrudException("An error occurred", e);
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    // Ignore conditions for now
    for (Mutation mutation : mutations) {
      if (mutation.getCondition().isPresent()) {
        LOGGER.warn("Ignoring the condition of the mutation: " + mutation);
        mutation.withCondition(null);
      }
    }

    try {
      jdbcService.mutate(mutations, connection, namespace, tableName);
    } catch (SQLException e) {
      throw new CrudException("An error occurred", e);
    }
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    try {
      try {
        connection.commit();
      } catch (SQLException e) {
        try {
          connection.rollback();
        } catch (SQLException sqlException) {
          throw new UnknownTransactionStatusException("Failed to rollback", sqlException);
        }
        throw new CommitException("Failed to commit", e);
      } finally {
        connection.close();
      }
    } catch (SQLException e) {
      throw new CommitException("An error occurred", e);
    }
  }

  @Override
  public void abort() throws AbortException {
    try {
      try {
        connection.rollback();
      } catch (SQLException e) {
        throw new AbortException("Failed to rollback", e);
      } finally {
        connection.close();
      }
    } catch (SQLException e) {
      throw new AbortException("An error occurred", e);
    }
  }
}
