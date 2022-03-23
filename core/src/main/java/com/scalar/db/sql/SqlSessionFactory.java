package com.scalar.db.sql;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.sql.exception.SqlException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public final class SqlSessionFactory implements AutoCloseable {

  private final TableMetadataManager tableMetadataManager;
  private final DistributedStorageAdmin storageAdmin;
  private final DistributedTransactionAdmin transactionAdmin;
  private final DistributedTransactionManager transactionManager;
  private final TwoPhaseCommitTransactionManager twoPhaseCommitTransactionManager;

  private SqlSessionFactory(DatabaseConfig config) {
    StorageFactory storageFactory = new StorageFactory(config);
    TransactionFactory transactionFactory = new TransactionFactory(config);

    storageAdmin = storageFactory.getAdmin();
    tableMetadataManager =
        new TableMetadataManager(storageAdmin, config.getTableMetadataCacheExpirationTimeSecs());
    transactionAdmin = transactionFactory.getTransactionAdmin();
    transactionManager = transactionFactory.getTransactionManager();
    twoPhaseCommitTransactionManager = transactionFactory.getTwoPhaseCommitTransactionManager();
  }

  public SqlSession getTransactionSession() {
    return new TransactionSqlSession(transactionAdmin, transactionManager, tableMetadataManager);
  }

  public SqlSession getTwoPhaseCommitTransactionSession() {
    return new TwoPhaseCommitTransactionSqlSession(
        transactionAdmin, twoPhaseCommitTransactionManager, tableMetadataManager);
  }

  public SqlSession resumeTwoPhaseCommitTransactionSession(String transactionId) {
    try {
      TwoPhaseCommitTransaction transaction =
          twoPhaseCommitTransactionManager.resume(transactionId);
      return new TwoPhaseCommitTransactionSqlSession(
          transactionAdmin, twoPhaseCommitTransactionManager, transaction, tableMetadataManager);
    } catch (TransactionException e) {
      throw new SqlException("Failed to resume a two-phase commit transaction");
    }
  }

  @Override
  public void close() {
    storageAdmin.close();
    transactionAdmin.close();
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
