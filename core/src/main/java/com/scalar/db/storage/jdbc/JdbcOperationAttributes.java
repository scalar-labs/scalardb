package com.scalar.db.storage.jdbc;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Put;
import java.util.Map;

/** A utility class to manipulate the operation attributes for JDBC. */
public final class JdbcOperationAttributes {
  private static final String OPERATION_ATTRIBUTE_PREFIX = "jdbc-";

  /**
   * The operation attribute key for whether converting a {@link Put} condition into {@code
   * PutIfNotExists} is enabled when the {@code Put} targets a {@code LEFT_OUTER} virtual table and
   * all conditions on the right source table columns are {@code IS_NULL}.
   *
   * <p>In a {@code LEFT_OUTER} join, an {@code IS_NULL} result on a right source table column may
   * mean either that a matching right-side record exists with a {@code NULL} value or that no
   * matching right-side record exists at all. To resolve this ambiguity, when this attribute is
   * enabled (the default), the condition is converted to {@code PutIfNotExists}, assuming the
   * common use case is to check that the right-side record does not exist. When disabled, the
   * original {@code IS_NULL} conditions are preserved as a {@code PutIf} condition.
   */
  public static final String
      LEFT_OUTER_VIRTUAL_TABLE_PUT_IF_IS_NULL_ON_RIGHT_COLUMNS_CONVERSION_ENABLED =
          OPERATION_ATTRIBUTE_PREFIX
              + "left-outer-virtual-table-put-if-is-null-on-right-columns-conversion-enabled";

  /**
   * The operation attribute key for whether a {@link Delete} is allowed when the {@code Delete}
   * targets a {@code LEFT_OUTER} virtual table and all conditions on the right source table columns
   * are {@code IS_NULL}.
   *
   * <p>In a {@code LEFT_OUTER} join, an {@code IS_NULL} result on a right source table column may
   * mean either that a matching right-side record exists with a {@code NULL} value or that no
   * matching right-side record exists at all, which makes such a {@code Delete} semantically
   * ambiguous. When this attribute is disabled (the default), the operation is rejected with an
   * exception. When enabled, the original {@code IS_NULL} conditions are preserved as a {@code
   * DeleteIf} condition.
   */
  public static final String LEFT_OUTER_VIRTUAL_TABLE_DELETE_IF_IS_NULL_ON_RIGHT_COLUMNS_ALLOWED =
      OPERATION_ATTRIBUTE_PREFIX
          + "left-outer-virtual-table-delete-if-is-null-on-right-columns-allowed";

  private JdbcOperationAttributes() {}

  /**
   * Returns whether converting a {@link Put} condition into {@code PutIfNotExists} is enabled for
   * the case where the {@code Put} targets a {@code LEFT_OUTER} virtual table and all conditions on
   * the right source table columns are {@code IS_NULL}. The default is {@code true} if the
   * attribute is not set.
   *
   * @param put the {@code Put} operation
   * @return {@code true} if the conversion is enabled, {@code false} otherwise
   */
  public static boolean isLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled(Put put) {
    return put.getAttribute(
            LEFT_OUTER_VIRTUAL_TABLE_PUT_IF_IS_NULL_ON_RIGHT_COLUMNS_CONVERSION_ENABLED)
        .map(Boolean::parseBoolean)
        .orElse(true);
  }

  /**
   * Sets whether converting a {@link Put} condition into {@code PutIfNotExists} is enabled for the
   * case where the {@code Put} targets a {@code LEFT_OUTER} virtual table and all conditions on the
   * right source table columns are {@code IS_NULL}.
   *
   * @param attributes the operation attributes
   * @param enabled {@code true} to enable the conversion, {@code false} to disable it
   */
  public static void setLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled(
      Map<String, String> attributes, boolean enabled) {
    attributes.put(
        LEFT_OUTER_VIRTUAL_TABLE_PUT_IF_IS_NULL_ON_RIGHT_COLUMNS_CONVERSION_ENABLED,
        Boolean.toString(enabled));
  }

  /**
   * Returns whether a {@link Delete} is allowed when the {@code Delete} targets a {@code
   * LEFT_OUTER} virtual table and all conditions on the right source table columns are {@code
   * IS_NULL}. The default is {@code false} if the attribute is not set.
   *
   * @param delete the {@code Delete} operation
   * @return {@code true} if the operation is allowed, {@code false} otherwise
   */
  public static boolean isLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed(Delete delete) {
    return delete
        .getAttribute(LEFT_OUTER_VIRTUAL_TABLE_DELETE_IF_IS_NULL_ON_RIGHT_COLUMNS_ALLOWED)
        .map(Boolean::parseBoolean)
        .orElse(false);
  }

  /**
   * Sets whether a {@link Delete} is allowed when the {@code Delete} targets a {@code LEFT_OUTER}
   * virtual table and all conditions on the right source table columns are {@code IS_NULL}.
   *
   * @param attributes the operation attributes
   * @param allowed {@code true} to allow the operation, {@code false} to disallow it
   */
  public static void setLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed(
      Map<String, String> attributes, boolean allowed) {
    attributes.put(
        LEFT_OUTER_VIRTUAL_TABLE_DELETE_IF_IS_NULL_ON_RIGHT_COLUMNS_ALLOWED,
        Boolean.toString(allowed));
  }
}
