package com.scalar.db.sql.metadata;

import com.scalar.db.sql.ClusteringOrder;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface TableMetadata {

  String getNamespaceName();

  String getName();

  List<ColumnMetadata> getPrimaryKey();

  boolean isPrimaryKeyColumn(String columnName);

  List<ColumnMetadata> getPartitionKey();

  boolean isPartitionKeyColumn(String columnName);

  Map<ColumnMetadata, ClusteringOrder> getClusteringKey();

  boolean isClusteringKeyColumn(String columnName);

  Map<String, ColumnMetadata> getColumns();

  Optional<ColumnMetadata> getColumn(String columnName);

  Map<String, IndexMetadata> getIndexes();

  Optional<IndexMetadata> getIndex(String columnName);
}
