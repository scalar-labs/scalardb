package com.scalar.db.storage.jdbc;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Put;
import java.util.Map;

/** A utility class to manipulate the operation attributes for JDBC. */
public final class JdbcOperationAttributes {
  private static final String OPERATION_ATTRIBUTE_PREFIX = "jdbc-";

  /**
   * The operation attribute key for whether a {@link Put} that targets a {@code LEFT_OUTER} virtual
   * table treats a right-side record whose non-primary-key columns are all {@code NULL} as the same
   * as the absence of a right-side record.
   *
   * <p>In a {@code LEFT_OUTER} join, an {@code IS_NULL} result on a right source table column may
   * mean either that a matching right-side record exists with a {@code NULL} value or that no
   * matching right-side record exists at all. When this attribute is enabled (the default), a
   * right-side record whose non-primary-key columns are all {@code NULL} is treated as the same as
   * no right-side record, as follows:
   *
   * <ul>
   *   <li>When all conditions on the right source table columns are {@code IS_NULL}, the condition
   *       is converted to {@code PutIfNotExists}, assuming the common use case is to check that the
   *       right-side record does not exist. If there is no condition on the left source table
   *       columns, it is also checked that the left-side record exists, so the {@code Put} fails
   *       when the virtual record does not exist, as with any other {@code PutIf} condition. In
   *       Spanner, this check is not performed when the left source table has only primary key
   *       columns, because Spanner does not allow an UPDATE statement to assign a value to a
   *       primary key column.
   *   <li>When the {@code Put} sets all non-primary-key columns of the right source table to {@code
   *       NULL}, the right-side record is deleted instead, except in the case above. The delete has
   *       the conditions on the right source table columns, if any.
   *   <li>When the {@code Put} sets no non-primary-key column of the right source table and has no
   *       condition on the right source table columns, nothing is written to the right source
   *       table.
   * </ul>
   *
   * <p>When disabled, none of the above applies. The original {@code IS_NULL} conditions are
   * preserved as a {@code PutIf} condition, and the right-side record is written as specified.
   */
  public static final String
      LEFT_OUTER_VIRTUAL_TABLE_PUT_TREATING_ALL_NULL_RIGHT_RECORD_AS_ABSENT_ENABLED =
          OPERATION_ATTRIBUTE_PREFIX
              + "left-outer-virtual-table-put-treating-all-null-right-record-as-absent-enabled";

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
   * Returns whether a {@link Put} that targets a {@code LEFT_OUTER} virtual table treats a
   * right-side record whose non-primary-key columns are all {@code NULL} as the same as the absence
   * of a right-side record. See {@link
   * #LEFT_OUTER_VIRTUAL_TABLE_PUT_TREATING_ALL_NULL_RIGHT_RECORD_AS_ABSENT_ENABLED} for details.
   * The default is {@code true} if the attribute is not set.
   *
   * @param put the {@code Put} operation
   * @return {@code true} if the two are treated as the same, {@code false} otherwise
   */
  public static boolean isLeftOuterVirtualTablePutTreatingAllNullRightRecordAsAbsentEnabled(
      Put put) {
    return put.getAttribute(
            LEFT_OUTER_VIRTUAL_TABLE_PUT_TREATING_ALL_NULL_RIGHT_RECORD_AS_ABSENT_ENABLED)
        .map(Boolean::parseBoolean)
        .orElse(true);
  }

  /**
   * Sets whether a {@link Put} that targets a {@code LEFT_OUTER} virtual table treats a right-side
   * record whose non-primary-key columns are all {@code NULL} as the same as the absence of a
   * right-side record. See {@link
   * #LEFT_OUTER_VIRTUAL_TABLE_PUT_TREATING_ALL_NULL_RIGHT_RECORD_AS_ABSENT_ENABLED} for details.
   *
   * @param attributes the operation attributes
   * @param enabled {@code true} to treat the two as the same, {@code false} otherwise
   */
  public static void setLeftOuterVirtualTablePutTreatingAllNullRightRecordAsAbsentEnabled(
      Map<String, String> attributes, boolean enabled) {
    attributes.put(
        LEFT_OUTER_VIRTUAL_TABLE_PUT_TREATING_ALL_NULL_RIGHT_RECORD_AS_ABSENT_ENABLED,
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
