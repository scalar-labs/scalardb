package com.scalar.db.transaction.consensuscommit;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.List;
import java.util.stream.Collectors;

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
    List<String> nonPrimaryKeyColumns =
        tableMetadata.getColumnNames().stream()
            .filter(c -> !tableMetadata.getPartitionKeyNames().contains(c))
            .filter(c -> !tableMetadata.getClusteringKeyNames().contains(c))
            .collect(Collectors.toList());

    // Check if the table metadata already has the transactional columns
    TRANSACTION_META_COLUMNS.forEach(
        (c, t) -> {
          if (tableMetadata.getColumnNames().contains(c)) {
            throw new IllegalArgumentException(
                "column \"" + c + "\" is reserved as transaction metadata");
          }
        });
    nonPrimaryKeyColumns.forEach(
        c -> {
          String beforePrefixed = Attribute.BEFORE_PREFIX + c;
          if (tableMetadata.getColumnNames().contains(beforePrefixed)) {
            throw new IllegalArgumentException(
                "non-primary key column with the \""
                    + Attribute.BEFORE_PREFIX
                    + "\" prefix, \""
                    + beforePrefixed
                    + "\", is reserved as transaction metadata");
          }
        });

    // Build a transactional table metadata
    TableMetadata.Builder builder = TableMetadata.newBuilder(tableMetadata);
    TRANSACTION_META_COLUMNS.forEach(builder::addColumn);
    nonPrimaryKeyColumns.forEach(
        c -> builder.addColumn(Attribute.BEFORE_PREFIX + c, tableMetadata.getColumnDataType(c)));
    return builder.build();
  }
}
