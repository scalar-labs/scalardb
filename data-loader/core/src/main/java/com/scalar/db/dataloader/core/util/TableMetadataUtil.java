package com.scalar.db.dataloader.core.util;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.Constants;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileTable;
import com.scalar.db.transaction.consensuscommit.Attribute;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Utility class for handling ScalarDB table metadata operations. */
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

  /**
   * Adds metadata columns to a list of projection columns for a ScalarDB table.
   *
   * @param tableMetadata The metadata of the ScalarDB table.
   * @param projections A list of projection column names.
   * @return A new list containing projection columns along with metadata columns.
   */
  public static List<String> populateProjectionsWithMetadata(
      TableMetadata tableMetadata, List<String> projections) {
    List<String> projectionMetadata = new ArrayList<>();
    projections.forEach(
        projection -> {
          projectionMetadata.add(projection);
          if (!isKeyColumn(projection, tableMetadata)) {
            projectionMetadata.add(Attribute.BEFORE_PREFIX + projection);
          }
        });
    projectionMetadata.addAll(ConsensusCommitUtils.getTransactionMetaColumns().keySet());
    return projectionMetadata;
  }

  /**
   * Checks whether a column is a key column (partition key or clustering key) in the table.
   *
   * @param column The name of the column to check.
   * @param tableMetadata The metadata of the ScalarDB table.
   * @return {@code true} if the column is a key column; {@code false} otherwise.
   */
  private static boolean isKeyColumn(String column, TableMetadata tableMetadata) {
    return tableMetadata.getPartitionKeyNames().contains(column)
        || tableMetadata.getClusteringKeyNames().contains(column);
  }
}
