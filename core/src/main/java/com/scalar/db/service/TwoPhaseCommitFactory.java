package com.scalar.db.service;

import com.scalar.db.api.TwoPhaseCommitCoordinator;
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.config.DatabaseConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

/**
 * A factory to instantiate the internal two-phase commit primitives, {@link
 * TwoPhaseCommitCoordinator} and {@link TwoPhaseCommitParticipant}.
 *
 * <p>These are internal primitives (see their Javadoc); they are not part of the application-facing
 * transaction API. This factory is kept separate from {@link TransactionFactory} so that {@link
 * TransactionFactory} stays limited to the application-facing types.
 */
public final class TwoPhaseCommitFactory {
  private final DatabaseConfig config;

  private TwoPhaseCommitFactory(DatabaseConfig config) {
    this.config = Objects.requireNonNull(config);
  }

  /**
   * Returns a {@link TwoPhaseCommitCoordinator} instance.
   *
   * @return a {@link TwoPhaseCommitCoordinator} instance
   * @throws UnsupportedOperationException if the transaction manager does not support the two-phase
   *     commit interface
   */
  public TwoPhaseCommitCoordinator getTwoPhaseCommitCoordinator() {
    return ProviderManager.createTwoPhaseCommitCoordinator(config);
  }

  /**
   * Returns a {@link TwoPhaseCommitParticipant} instance.
   *
   * @return a {@link TwoPhaseCommitParticipant} instance
   * @throws UnsupportedOperationException if the transaction manager does not support the two-phase
   *     commit interface
   */
  public TwoPhaseCommitParticipant getTwoPhaseCommitParticipant() {
    return ProviderManager.createTwoPhaseCommitParticipant(config);
  }

  /**
   * Returns a {@link TwoPhaseCommitFactory} instance.
   *
   * @param properties properties to use for configuration
   * @return a {@link TwoPhaseCommitFactory} instance
   */
  public static TwoPhaseCommitFactory create(Properties properties) {
    return new TwoPhaseCommitFactory(new DatabaseConfig(properties));
  }

  /**
   * Returns a {@link TwoPhaseCommitFactory} instance.
   *
   * @param propertiesPath a properties path
   * @return a {@link TwoPhaseCommitFactory} instance
   * @throws IOException if IO error occurs
   */
  public static TwoPhaseCommitFactory create(Path propertiesPath) throws IOException {
    return new TwoPhaseCommitFactory(new DatabaseConfig(propertiesPath));
  }

  /**
   * Returns a {@link TwoPhaseCommitFactory} instance.
   *
   * @param propertiesFile a properties file
   * @return a {@link TwoPhaseCommitFactory} instance
   * @throws IOException if IO error occurs
   */
  public static TwoPhaseCommitFactory create(File propertiesFile) throws IOException {
    return new TwoPhaseCommitFactory(new DatabaseConfig(propertiesFile));
  }

  /**
   * Returns a {@link TwoPhaseCommitFactory} instance.
   *
   * @param propertiesFilePath a properties file path
   * @return a {@link TwoPhaseCommitFactory} instance
   * @throws IOException if IO error occurs
   */
  public static TwoPhaseCommitFactory create(String propertiesFilePath) throws IOException {
    return create(Paths.get(propertiesFilePath));
  }
}
