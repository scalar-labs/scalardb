package com.scalar.db.storage.jdbc;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Put;
import java.util.Map;

/** A utility class to manipulate the operation attributes for JDBC. */
public final class JdbcOperationAttributes {
  private static final String OPERATION_ATTRIBUTE_PREFIX = "jdbc-";

  public static final String
      LEFT_OUTER_VIRTUAL_TABLE_PUT_IF_IS_NULL_ON_RIGHT_COLUMNS_CONVERSION_ENABLED =
          OPERATION_ATTRIBUTE_PREFIX
              + "left-outer-virtual-table-put-if-is-null-on-right-columns-conversion-enabled";

  public static final String LEFT_OUTER_VIRTUAL_TABLE_DELETE_IF_IS_NULL_ON_RIGHT_COLUMNS_ALLOWED =
      OPERATION_ATTRIBUTE_PREFIX
          + "left-outer-virtual-table-delete-if-is-null-on-right-columns-allowed";

  private JdbcOperationAttributes() {}

  public static boolean isLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled(Put put) {
    return put.getAttribute(
            LEFT_OUTER_VIRTUAL_TABLE_PUT_IF_IS_NULL_ON_RIGHT_COLUMNS_CONVERSION_ENABLED)
        .map(Boolean::parseBoolean)
        .orElse(true);
  }

  public static void setLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled(
      Map<String, String> attributes, boolean enabled) {
    attributes.put(
        LEFT_OUTER_VIRTUAL_TABLE_PUT_IF_IS_NULL_ON_RIGHT_COLUMNS_CONVERSION_ENABLED,
        Boolean.toString(enabled));
  }

  public static boolean isLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed(Delete delete) {
    return delete
        .getAttribute(LEFT_OUTER_VIRTUAL_TABLE_DELETE_IF_IS_NULL_ON_RIGHT_COLUMNS_ALLOWED)
        .map(Boolean::parseBoolean)
        .orElse(false);
  }

  public static void setLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed(
      Map<String, String> attributes, boolean allowed) {
    attributes.put(
        LEFT_OUTER_VIRTUAL_TABLE_DELETE_IF_IS_NULL_ON_RIGHT_COLUMNS_ALLOWED,
        Boolean.toString(allowed));
  }
}
