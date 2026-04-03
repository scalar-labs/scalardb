package com.scalar.db.api;

import java.util.Map;

/** A utility class to manipulate the operation attributes for database operations. */
public final class DatabaseOperationAttributes {

  private static final String OPERATION_ATTRIBUTE_PREFIX = "db-";

  public static final String CROSS_PARTITION_SCAN_ENABLED =
      OPERATION_ATTRIBUTE_PREFIX + "cross-partition-scan-enabled";
  public static final String CROSS_PARTITION_SCAN_FILTERING_ENABLED =
      OPERATION_ATTRIBUTE_PREFIX + "cross-partition-scan-filtering-enabled";
  public static final String CROSS_PARTITION_SCAN_ORDERING_ENABLED =
      OPERATION_ATTRIBUTE_PREFIX + "cross-partition-scan-ordering-enabled";

  private DatabaseOperationAttributes() {}

  public static boolean isCrossPartitionScanEnabled(ScanAll scanAll) {
    return scanAll
        .getAttribute(CROSS_PARTITION_SCAN_ENABLED)
        .map(Boolean::parseBoolean)
        .orElse(false);
  }

  public static void setCrossPartitionScanEnabled(Map<String, String> attributes, boolean enabled) {
    attributes.put(CROSS_PARTITION_SCAN_ENABLED, Boolean.toString(enabled));
  }

  public static boolean isCrossPartitionScanFilteringEnabled(ScanAll scanAll) {
    return scanAll
        .getAttribute(CROSS_PARTITION_SCAN_FILTERING_ENABLED)
        .map(Boolean::parseBoolean)
        .orElse(false);
  }

  public static void setCrossPartitionScanFilteringEnabled(
      Map<String, String> attributes, boolean enabled) {
    attributes.put(CROSS_PARTITION_SCAN_FILTERING_ENABLED, Boolean.toString(enabled));
  }

  public static boolean isCrossPartitionScanOrderingEnabled(ScanAll scanAll) {
    return scanAll
        .getAttribute(CROSS_PARTITION_SCAN_ORDERING_ENABLED)
        .map(Boolean::parseBoolean)
        .orElse(false);
  }

  public static void setCrossPartitionScanOrderingEnabled(
      Map<String, String> attributes, boolean enabled) {
    attributes.put(CROSS_PARTITION_SCAN_ORDERING_ENABLED, Boolean.toString(enabled));
  }
}
