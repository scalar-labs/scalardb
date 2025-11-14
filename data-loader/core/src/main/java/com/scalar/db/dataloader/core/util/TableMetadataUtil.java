package com.scalar.db.dataloader.core.util;

import com.scalar.db.dataloader.core.Constants;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileTable;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Utility class for handling ScalarDB table metadata operations. */
@SuppressWarnings("SameNameButDifferent")
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TableMetadataUtil {

  /**
   * Generates a unique lookup key for a table within a namespace.
   *
   * @param namespace The namespace of the table.
   * @param tableName The name of the table.
   * @return A formatted string representing the table lookup key.
   */
  public static String getTableLookupKey(String namespace, String tableName) {
    return String.format(Constants.TABLE_LOOKUP_KEY_FORMAT, namespace, tableName);
  }

  /**
   * Generates a unique lookup key for a table using control file table data.
   *
   * @param controlFileTable The control file table object containing namespace and table name.
   * @return A formatted string representing the table lookup key.
   */
  public static String getTableLookupKey(ControlFileTable controlFileTable) {
    return String.format(
        Constants.TABLE_LOOKUP_KEY_FORMAT,
        controlFileTable.getNamespace(),
        controlFileTable.getTable());
  }
}
