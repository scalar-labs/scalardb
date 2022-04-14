package com.scalar.db.sql;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.sql.metadata.CachedMetadata;
import com.scalar.db.sql.metadata.Metadata;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class SqlStatementSessionFactory implements AutoCloseable {

  private final DistributedTransactionAdmin transactionAdmin;
  private final DistributedTransactionManager transactionManager;
  private final TwoPhaseCommitTransactionManager twoPhaseCommitTransactionManager;
  private final Metadata metadata;

  private SqlStatementSessionFactory(DatabaseConfig config) {
    TransactionFactory transactionFactory = new TransactionFactory(Objects.requireNonNull(config));
    transactionAdmin = transactionFactory.getTransactionAdmin();
    transactionManager = transactionFactory.getTransactionManager();
    twoPhaseCommitTransactionManager = transactionFactory.getTwoPhaseCommitTransactionManager();
    metadata = CachedMetadata.create(transactionAdmin, config.getMetadataCacheExpirationTimeSecs());
  }

  public SqlStatementSession getTransactionSession() {
    return new TransactionSession(transactionAdmin, transactionManager, metadata);
  }

  public SqlStatementSession getTwoPhaseCommitTransactionSession() {
    return new TwoPhaseCommitTransactionSession(
        transactionAdmin, twoPhaseCommitTransactionManager, metadata);
  }

  @Override
  public void close() {
    transactionAdmin.close();
    transactionManager.close();
    twoPhaseCommitTransactionManager.close();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Properties properties;
    private final List<String> contactPoints;

    private Builder() {
      properties = new Properties();
      contactPoints = new ArrayList<>();
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
      return this;
    }

    public Builder withProperty(String name, String value) {
      properties.put(name, value);
      return this;
    }

    public Builder addContactPoint(String contactPoint) {
      contactPoints.add(contactPoint);
      return this;
    }

    public Builder withContactPort(int contactPort) {
      properties.put(DatabaseConfig.CONTACT_PORT, Integer.toString(contactPort));
      return this;
    }

    public Builder withUsername(String username) {
      properties.put(DatabaseConfig.USERNAME, username);
      return this;
    }

    public Builder withPassword(String password) {
      properties.put(DatabaseConfig.PASSWORD, password);
      return this;
    }

    public Builder withStorage(String storageType) {
      properties.put(DatabaseConfig.STORAGE, storageType);
      return this;
    }

    public Builder withTransactionManager(String transactionManagerType) {
      properties.put(DatabaseConfig.TRANSACTION_MANAGER, transactionManagerType);
      return this;
    }

    public SqlStatementSessionFactory build() {
      if (!contactPoints.isEmpty()) {
        properties.put(DatabaseConfig.CONTACT_POINTS, String.join(",", contactPoints));
      }
      return new SqlStatementSessionFactory(new DatabaseConfig(properties));
    }
  }
}
