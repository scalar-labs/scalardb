package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.Put;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/** A utility class to manipulate the operation attributes for Consensus Commit. */
public final class ConsensusCommitOperationAttributes {

  private static final String OPERATION_ATTRIBUTE_PREFIX = "cc-";

  /** The operation attribute key for whether implicit pre-read is enabled. */
  public static final String IMPLICIT_PRE_READ_ENABLED =
      OPERATION_ATTRIBUTE_PREFIX + "implicit-pre-read-enabled";

  /** The operation attribute key for whether insert mode is enabled. */
  public static final String INSERT_MODE_ENABLED =
      OPERATION_ATTRIBUTE_PREFIX + "insert-mode-enabled";

  /** The operation attribute key for the isolation level. */
  public static final String ISOLATION = OPERATION_ATTRIBUTE_PREFIX + "isolation";

  private ConsensusCommitOperationAttributes() {}

  /**
   * Returns a new {@link Put} that has implicit pre-read enabled.
   *
   * @param put the original {@code Put} operation
   * @return a new {@code Put} with implicit pre-read enabled
   */
  public static Put enableImplicitPreRead(Put put) {
    return Put.newBuilder(put).attribute(IMPLICIT_PRE_READ_ENABLED, "true").build();
  }

  /**
   * Enables implicit pre-read in the operation attributes.
   *
   * @param attributes the operation attributes
   */
  public static void enableImplicitPreRead(Map<String, String> attributes) {
    attributes.put(IMPLICIT_PRE_READ_ENABLED, "true");
  }

  /**
   * Returns a new {@link Put} that has implicit pre-read disabled.
   *
   * @param put the original {@code Put} operation
   * @return a new {@code Put} with implicit pre-read disabled
   */
  public static Put disableImplicitPreRead(Put put) {
    return Put.newBuilder(put).clearAttribute(IMPLICIT_PRE_READ_ENABLED).build();
  }

  /**
   * Disables implicit pre-read in the operation attributes.
   *
   * @param attributes the operation attributes
   */
  public static void disableImplicitPreRead(Map<String, String> attributes) {
    attributes.remove(IMPLICIT_PRE_READ_ENABLED);
  }

  /**
   * Returns a new {@link Put} that has insert mode enabled.
   *
   * @param put the original {@code Put} operation
   * @return a new {@code Put} with insert mode enabled
   */
  public static Put enableInsertMode(Put put) {
    return Put.newBuilder(put).attribute(INSERT_MODE_ENABLED, "true").build();
  }

  /**
   * Enables insert mode in the operation attributes.
   *
   * @param attributes the operation attributes
   */
  public static void enableInsertMode(Map<String, String> attributes) {
    attributes.put(INSERT_MODE_ENABLED, "true");
  }

  /**
   * Returns a new {@link Put} that has insert mode disabled.
   *
   * @param put the original {@code Put} operation
   * @return a new {@code Put} with insert mode disabled
   */
  public static Put disableInsertMode(Put put) {
    return Put.newBuilder(put).clearAttribute(INSERT_MODE_ENABLED).build();
  }

  /**
   * Disables insert mode in the operation attributes.
   *
   * @param attributes the operation attributes
   */
  public static void disableInsertMode(Map<String, String> attributes) {
    attributes.remove(INSERT_MODE_ENABLED);
  }

  /**
   * Returns whether implicit pre-read is enabled for the specified {@link Put} operation.
   *
   * @param put the {@code Put} operation
   * @return {@code true} if implicit pre-read is enabled, {@code false} otherwise
   */
  public static boolean isImplicitPreReadEnabled(Put put) {
    Optional<String> attribute = put.getAttribute(IMPLICIT_PRE_READ_ENABLED);
    return attribute.isPresent() && "true".equalsIgnoreCase(attribute.get());
  }

  /**
   * Returns whether insert mode is enabled for the specified {@link Put} operation.
   *
   * @param put the {@code Put} operation
   * @return {@code true} if insert mode is enabled, {@code false} otherwise
   */
  public static boolean isInsertModeEnabled(Put put) {
    Optional<String> attribute = put.getAttribute(INSERT_MODE_ENABLED);
    return attribute.isPresent() && "true".equalsIgnoreCase(attribute.get());
  }

  /**
   * Sets the specified isolation level in the operation attributes.
   *
   * @param attributes the operation attributes
   * @param isolation the isolation level
   */
  public static void setIsolation(Map<String, String> attributes, Isolation isolation) {
    attributes.put(ISOLATION, isolation.name());
  }

  /**
   * Clears the isolation level from the operation attributes.
   *
   * @param attributes the operation attributes
   */
  public static void clearIsolation(Map<String, String> attributes) {
    attributes.remove(ISOLATION);
  }

  /**
   * Returns the isolation level from the operation attributes.
   *
   * @param attributes the operation attributes
   * @return an {@code Optional} containing the isolation level, or an empty {@code Optional} if not
   *     set
   */
  public static Optional<Isolation> getIsolation(Map<String, String> attributes) {
    String value = attributes.get(ISOLATION);
    if (value == null) {
      return Optional.empty();
    }
    return Optional.of(Isolation.valueOf(value.toUpperCase(Locale.ROOT)));
  }
}
