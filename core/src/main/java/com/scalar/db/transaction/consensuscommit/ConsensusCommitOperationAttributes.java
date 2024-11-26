package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.Put;
import java.util.Map;
import java.util.Optional;

/** A utility class to manipulate the operation attributes for Consensus Commit. */
public final class ConsensusCommitOperationAttributes {

  private static final String OPERATION_ATTRIBUTE_PREFIX = "cc-";
  public static final String IMPLICIT_PRE_READ_ENABLED =
      OPERATION_ATTRIBUTE_PREFIX + "implicit-pre-read-enabled";
  public static final String INSERT_MODE_ENABLED =
      OPERATION_ATTRIBUTE_PREFIX + "insert-mode-enabled";

  private ConsensusCommitOperationAttributes() {}

  public static Put enableImplicitPreRead(Put put) {
    return Put.newBuilder(put).attribute(IMPLICIT_PRE_READ_ENABLED, "true").build();
  }

  public static void enableImplicitPreRead(Map<String, String> attributes) {
    attributes.put(IMPLICIT_PRE_READ_ENABLED, "true");
  }

  public static Put disableImplicitPreRead(Put put) {
    return Put.newBuilder(put).clearAttribute(IMPLICIT_PRE_READ_ENABLED).build();
  }

  public static void disableImplicitPreRead(Map<String, String> attributes) {
    attributes.remove(IMPLICIT_PRE_READ_ENABLED);
  }

  public static Put enableInsertMode(Put put) {
    return Put.newBuilder(put).attribute(INSERT_MODE_ENABLED, "true").build();
  }

  public static void enableInsertMode(Map<String, String> attributes) {
    attributes.put(INSERT_MODE_ENABLED, "true");
  }

  public static Put disableInsertMode(Put put) {
    return Put.newBuilder(put).clearAttribute(INSERT_MODE_ENABLED).build();
  }

  public static void disableInsertMode(Map<String, String> attributes) {
    attributes.remove(INSERT_MODE_ENABLED);
  }

  public static boolean isImplicitPreReadEnabled(Put put) {
    Optional<String> attribute = put.getAttribute(IMPLICIT_PRE_READ_ENABLED);
    return attribute.isPresent() && "true".equalsIgnoreCase(attribute.get());
  }

  public static boolean isInsertModeEnabled(Put put) {
    Optional<String> attribute = put.getAttribute(INSERT_MODE_ENABLED);
    return attribute.isPresent() && "true".equalsIgnoreCase(attribute.get());
  }
}
