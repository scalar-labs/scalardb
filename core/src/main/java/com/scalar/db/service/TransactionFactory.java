package com.scalar.db.service;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * A factory class to instantiate {@link DistributedTransactionManager} and {@link
 * DistributedTransactionAdmin} and {@link TwoPhaseCommitTransactionManager}
 */
public class TransactionFactory {
  private final DatabaseConfig config;

  /**
   * @param config a database config
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     #create(Properties)}, {@link #create(Path)}, {@link #create(File)}, or {@link
   *     #create(String)} instead
   */
  @Deprecated
  public TransactionFactory(DatabaseConfig config) {
    this.config = Objects.requireNonNull(config);
  }

  /**
   * Returns a {@link DistributedTransactionManager} instance
   *
   * @return a {@link DistributedTransactionManager} instance
   */
  public DistributedTransactionManager getTransactionManager() {
    return ProviderManager.createDistributedTransactionManager(config);
  }

  /**
   * Returns a {@link DistributedTransactionAdmin} instance
   *
   * @return a {@link DistributedTransactionAdmin} instance
   */
  public DistributedTransactionAdmin getTransactionAdmin() {
    return ProviderManager.createDistributedTransactionAdmin(config);
  }

  /**
   * Returns a {@link TwoPhaseCommitTransactionManager} instance
   *
   * @return a {@link TwoPhaseCommitTransactionManager} instance. If the transaction manager does
   *     not support the two-phase commit interface, returns {@code null}.
   */
  @Nullable
  public TwoPhaseCommitTransactionManager getTwoPhaseCommitTransactionManager() {
    return ProviderManager.createTwoPhaseCommitTransactionManager(config);
  }

  /**
   * Returns a TransactionFactory instance.
   *
   * @param properties properties to use for configuration
   * @return a TransactionFactory instance
   */
  public static TransactionFactory create(Properties properties) {
    return new TransactionFactory(new DatabaseConfig(properties));
  }

  /**
   * Returns a TransactionFactory instance.
   *
   * @param propertiesPath a properties path
   * @return a TransactionFactory instance
   * @throws IOException if IO error occurs
   */
  public static TransactionFactory create(Path propertiesPath) throws IOException {
    return new TransactionFactory(new DatabaseConfig(propertiesPath));
  }

  /**
   * Returns a TransactionFactory instance.
   *
   * @param propertiesFile a properties file
   * @return a TransactionFactory instance
   * @throws IOException if IO error occurs
   */
  public static TransactionFactory create(File propertiesFile) throws IOException {
    return new TransactionFactory(new DatabaseConfig(propertiesFile));
  }

  /**
   * Returns a TransactionFactory instance.
   *
   * @param propertiesFilePath a properties file path
   * @return a TransactionFactory instance
   * @throws IOException if IO error occurs
   */
  public static TransactionFactory create(String propertiesFilePath) throws IOException {
    return create(Paths.get(propertiesFilePath));
  }
}
