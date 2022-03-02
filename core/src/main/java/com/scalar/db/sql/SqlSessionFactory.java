package com.scalar.db.sql;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.sql.exception.SqlException;
import com.scalar.db.util.TableMetadataManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public final class SqlSessionFactory implements Closeable {

  private final TableMetadataManager tableMetadataManager;

  private final DistributedStorage storage;
  private final DistributedStorageAdmin admin;
  private final DistributedTransactionManager transactionManager;
  private final TwoPhaseCommitTransactionManager twoPhaseCommitTransactionManager;

  private SqlSessionFactory(DatabaseConfig config) {
    StorageFactory storageFactory = new StorageFactory(config);
    TransactionFactory transactionFactory = new TransactionFactory(config);

    storage = storageFactory.getStorage();
    admin = storageFactory.getAdmin();
    tableMetadataManager =
        new TableMetadataManager(admin, config.getTableMetadataCacheExpirationTimeSecs());
    transactionManager = transactionFactory.getTransactionManager();
    twoPhaseCommitTransactionManager = transactionFactory.getTwoPhaseCommitTransactionManager();
  }

  public StorageSqlSession getStorageSqlSession() {
    return new StorageSqlSession(storage, admin, tableMetadataManager);
  }

  public TransactionSqlSession beginTransaction() {
    try {
      DistributedTransaction transaction = transactionManager.start();
      return new TransactionSqlSession(transaction, tableMetadataManager);
    } catch (TransactionException e) {
      throw new SqlException("Failed to start a transaction");
    }
  }

  public TwoPhaseCommitTransactionSqlSession beginTwoPhaseCommitTransaction() {
    try {
      TwoPhaseCommitTransaction transaction = twoPhaseCommitTransactionManager.start();
      return new TwoPhaseCommitTransactionSqlSession(transaction, tableMetadataManager);
    } catch (TransactionException e) {
      throw new SqlException("Failed to start a two-phase commit transaction");
    }
  }

  public TwoPhaseCommitTransactionSqlSession joinTwoPhaseCommitTransaction(String transactionId) {
    try {
      TwoPhaseCommitTransaction transaction = twoPhaseCommitTransactionManager.join(transactionId);
      return new TwoPhaseCommitTransactionSqlSession(transaction, tableMetadataManager);
    } catch (TransactionException e) {
      throw new SqlException("Failed to join a two-phase commit transaction");
    }
  }

  public TwoPhaseCommitTransactionSqlSession resumeTwoPhaseCommitTransaction(String transactionId) {
    try {
      TwoPhaseCommitTransaction transaction =
          twoPhaseCommitTransactionManager.resume(transactionId);
      return new TwoPhaseCommitTransactionSqlSession(transaction, tableMetadataManager);
    } catch (TransactionException e) {
      throw new SqlException("Failed to resume a two-phase commit transaction");
    }
  }

  @Override
  public void close() {
    storage.close();
    admin.close();
    transactionManager.close();
    twoPhaseCommitTransactionManager.close();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Properties properties;

    private Builder() {
      properties = new Properties();
    }

    public Builder withPropertyFile(String path) {
      return withPropertyFile(Paths.get(path));
    }

    public Builder withPropertyFile(Path path) {
      try (InputStream inputStream = Files.newInputStream(path)) {
        properties.load(inputStream);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return null;
    }

    public Builder withProperty(String name, String value) {
      properties.put(name, value);
      return this;
    }

    public SqlSessionFactory build() {
      return new SqlSessionFactory(new DatabaseConfig(properties));
    }
  }
}
