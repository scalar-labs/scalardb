package com.scalar.db.transaction.consensuscommit;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;

public final class TransactionalTable {

  private static final ImmutableMap<String, DataType> TRANSACTION_META_COLUMNS =
      ImmutableMap.<String, DataType>builder()
          .put(Attribute.ID, DataType.TEXT)
          .put(Attribute.STATE, DataType.INT)
          .put(Attribute.VERSION, DataType.INT)
          .put(Attribute.PREPARED_AT, DataType.BIGINT)
          .put(Attribute.COMMITTED_AT, DataType.BIGINT)
          .put(Attribute.BEFORE_ID, DataType.TEXT)
          .put(Attribute.BEFORE_STATE, DataType.INT)
          .put(Attribute.BEFORE_VERSION, DataType.INT)
          .put(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
          .put(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
          .build();

  private TransactionalTable() {}

  /**
   * Converts a table metadata to a transactional one
   *
   * @param tableMetadata a table metadata to be converted
   * @return a transactional table metadata
   */
  public static TableMetadata convertToTransactionalTable(TableMetadata tableMetadata) {
    // Check if the table metadata already has the transactional columns
    TRANSACTION_META_COLUMNS.forEach(
        (c, t) -> {
          if (tableMetadata.getColumnNames().contains(c)) {
            throw new IllegalArgumentException(
                "column \"" + c + "\" is reserved as transaction metadata");
          }
        });
    tableMetadata.getColumnNames().stream()
        .filter(c -> !tableMetadata.getPartitionKeyNames().contains(c))
        .filter(c -> !tableMetadata.getClusteringKeyNames().contains(c))
        .forEach(
            c -> {
              if (tableMetadata.getColumnNames().contains(Attribute.BEFORE_PREFIX + c)) {
                throw new IllegalArgumentException(
                    "non-primary key column with the \""
                        + Attribute.BEFORE_PREFIX
                        + "\" prefix, \""
                        + (Attribute.BEFORE_PREFIX + c)
                        + "\", is reserved as transaction metadata");
              }
            });

    // Build a transactional table metadata
    TableMetadata.Builder builder = TableMetadata.newBuilder(tableMetadata);
    TRANSACTION_META_COLUMNS.forEach(builder::addColumn);
    tableMetadata.getColumnNames().stream()
        .filter(c -> !tableMetadata.getPartitionKeyNames().contains(c))
        .filter(c -> !tableMetadata.getClusteringKeyNames().contains(c))
        .forEach(
            c ->
                builder.addColumn(Attribute.BEFORE_PREFIX + c, tableMetadata.getColumnDataType(c)));
    return builder.build();
  }
}
