package com.scalar.db.schemaloader.alteration;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.DataType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;

public class TableMetadataAlterationProcessor {

  /**
   * Compute the alteration between old and new table metadata
   *
   * @param namespace the namespace name of the table
   * @param table the table name
   * @param oldMetadata the current table metadata
   * @param newMetadata the new table metadata
   * @return the table alteration
   */
  public TableMetadataAlteration computeAlteration(
      String namespace, String table, TableMetadata oldMetadata, TableMetadata newMetadata) {
    checkUnsupportedAlteration(namespace, table, oldMetadata, newMetadata);

    LinkedHashSet<String> addedColumnNames = new LinkedHashSet<>(newMetadata.getColumnNames());
    addedColumnNames.removeAll(oldMetadata.getColumnNames());

    HashMap<String, DataType> addedColumnDataTypes = new HashMap<>();
    for (String addedColumn : addedColumnNames) {
      addedColumnDataTypes.put(addedColumn, newMetadata.getColumnDataType(addedColumn));
    }

    HashSet<String> addedSecondaryIndexNames = new HashSet<>(newMetadata.getSecondaryIndexNames());
    addedSecondaryIndexNames.removeAll(oldMetadata.getSecondaryIndexNames());

    HashSet<String> deletedSecondaryIndexNames =
        new HashSet<>(oldMetadata.getSecondaryIndexNames());
    deletedSecondaryIndexNames.removeAll(newMetadata.getSecondaryIndexNames());

    return new TableMetadataAlteration(
        addedColumnNames,
        addedColumnDataTypes,
        addedSecondaryIndexNames,
        deletedSecondaryIndexNames);
  }

  private void checkUnsupportedAlteration(
      String namespace, String table, TableMetadata oldMetadata, TableMetadata newMetadata) {
    if (!newMetadata.getPartitionKeyNames().equals(oldMetadata.getPartitionKeyNames())) {
      throw new UnsupportedOperationException(
          CoreError.SCHEMA_LOADER_MODIFYING_PARTITION_KEYS_NOT_SUPPORTED.buildMessage(
              namespace, table));
    }
    if (!newMetadata.getClusteringKeyNames().equals(oldMetadata.getClusteringKeyNames())) {
      throw new UnsupportedOperationException(
          CoreError.SCHEMA_LOADER_MODIFYING_CLUSTERING_KEYS_NOT_SUPPORTED.buildMessage(
              namespace, table));
    }
    if (!newMetadata.getClusteringOrders().equals(oldMetadata.getClusteringOrders())) {
      throw new UnsupportedOperationException(
          CoreError.SCHEMA_LOADER_MODIFYING_CLUSTERING_ORDER_NOT_SUPPORTED.buildMessage(
              namespace, table));
    }
    for (String oldColumn : oldMetadata.getColumnNames()) {
      if (!newMetadata.getColumnNames().contains(oldColumn)) {
        throw new UnsupportedOperationException(
            CoreError.SCHEMA_LOADER_DELETING_COLUMN_NOT_SUPPORTED.buildMessage(
                oldColumn, namespace, table));
      }
    }
    for (String column : oldMetadata.getColumnNames()) {
      if (!oldMetadata.getColumnDataType(column).equals(newMetadata.getColumnDataType(column))) {
        throw new UnsupportedOperationException(
            CoreError.SCHEMA_LOADER_MODIFYING_COLUMN_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                column, namespace, table));
      }
    }
  }
}
