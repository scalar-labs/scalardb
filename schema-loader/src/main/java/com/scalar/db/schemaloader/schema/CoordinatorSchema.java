package com.scalar.db.schemaloader.schema;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.Attribute;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.util.Map;

public class CoordinatorSchema {

  private static final String PARTITION_KEY = "tx_id";
  private static final ImmutableMap<String, DataType> columns =
      ImmutableMap.<String, DataType>builder()
          .put(Attribute.ID, DataType.TEXT)
          .put(Attribute.STATE, DataType.INT)
          .put(Attribute.CREATED_AT, DataType.BIGINT)
          .build();
  private final TableMetadata tableMetadata;

  public CoordinatorSchema() {
    TableMetadata.Builder tableBuilder = TableMetadata.newBuilder();
    tableBuilder.addPartitionKey(PARTITION_KEY);
    for (Map.Entry<String, DataType> col : columns.entrySet()) {
      tableBuilder.addColumn(col.getKey(), col.getValue());
    }
    tableMetadata = tableBuilder.build();
  }

  public String getNamespace() {
    return Coordinator.NAMESPACE;
  }

  public String getTable() {
    return Coordinator.TABLE;
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }
}
