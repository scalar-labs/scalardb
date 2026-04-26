package com.scalar.db.api;

import java.util.Map;

/** A utility class to manipulate the operation attributes for database operations. */
public final class DatabaseOperationAttributes {

  private static final String OPERATION_ATTRIBUTE_PREFIX = "db-";

  /** The operation attribute key for whether cross-partition scan is enabled. */
  public static final String CROSS_PARTITION_SCAN_ENABLED =
      OPERATION_ATTRIBUTE_PREFIX + "cross-partition-scan-enabled";

  /** The operation attribute key for whether cross-partition scan filtering is enabled. */
  public static final String CROSS_PARTITION_SCAN_FILTERING_ENABLED =
      OPERATION_ATTRIBUTE_PREFIX + "cross-partition-scan-filtering-enabled";

  /** The operation attribute key for whether cross-partition scan ordering is enabled. */
  public static final String CROSS_PARTITION_SCAN_ORDERING_ENABLED =
      OPERATION_ATTRIBUTE_PREFIX + "cross-partition-scan-ordering-enabled";

  private DatabaseOperationAttributes() {}

  /**
   * Returns whether cross-partition scan is enabled for the specified {@link ScanAll} operation.
   *
   * @param scanAll the {@code ScanAll} operation
   * @return {@code true} if cross-partition scan is enabled, {@code false} otherwise
   */
  public static boolean isCrossPartitionScanEnabled(ScanAll scanAll) {
    return scanAll
        .getAttribute(CROSS_PARTITION_SCAN_ENABLED)
        .map(Boolean::parseBoolean)
        .orElse(false);
  }

  /**
   * Sets whether cross-partition scan is enabled in the operation attributes.
   *
   * @param attributes the operation attributes
   * @param enabled {@code true} to enable cross-partition scan, {@code false} to disable it
   */
  public static void setCrossPartitionScanEnabled(Map<String, String> attributes, boolean enabled) {
    attributes.put(CROSS_PARTITION_SCAN_ENABLED, Boolean.toString(enabled));
  }

  /**
   * Returns whether cross-partition scan filtering is enabled for the specified {@link ScanAll}
   * operation.
   *
   * @param scanAll the {@code ScanAll} operation
   * @return {@code true} if cross-partition scan filtering is enabled, {@code false} otherwise
   */
  public static boolean isCrossPartitionScanFilteringEnabled(ScanAll scanAll) {
    return scanAll
        .getAttribute(CROSS_PARTITION_SCAN_FILTERING_ENABLED)
        .map(Boolean::parseBoolean)
        .orElse(false);
  }

  /**
   * Sets whether cross-partition scan filtering is enabled in the operation attributes.
   *
   * @param attributes the operation attributes
   * @param enabled {@code true} to enable cross-partition scan filtering, {@code false} to disable
   *     it
   */
  public static void setCrossPartitionScanFilteringEnabled(
      Map<String, String> attributes, boolean enabled) {
    attributes.put(CROSS_PARTITION_SCAN_FILTERING_ENABLED, Boolean.toString(enabled));
  }

  /**
   * Returns whether cross-partition scan ordering is enabled for the specified {@link ScanAll}
   * operation.
   *
   * @param scanAll the {@code ScanAll} operation
   * @return {@code true} if cross-partition scan ordering is enabled, {@code false} otherwise
   */
  public static boolean isCrossPartitionScanOrderingEnabled(ScanAll scanAll) {
    return scanAll
        .getAttribute(CROSS_PARTITION_SCAN_ORDERING_ENABLED)
        .map(Boolean::parseBoolean)
        .orElse(false);
  }

  /**
   * Sets whether cross-partition scan ordering is enabled in the operation attributes.
   *
   * @param attributes the operation attributes
   * @param enabled {@code true} to enable cross-partition scan ordering, {@code false} to disable
   *     it
   */
  public static void setCrossPartitionScanOrderingEnabled(
      Map<String, String> attributes, boolean enabled) {
    attributes.put(CROSS_PARTITION_SCAN_ORDERING_ENABLED, Boolean.toString(enabled));
  }
}
