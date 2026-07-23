package com.scalar.db.storage.objectstorage;

import com.google.common.collect.Ordering;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.CollationComparator;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

public class ClusteringKeyComparator implements Comparator<Map<String, Object>> {
  private final TableMetadata metadata;
  private final Comparator<Column<?>> perColumn;

  public ClusteringKeyComparator(
      TableMetadata metadata, Optional<CollationComparator> collationComparator) {
    this.metadata = metadata;
    this.perColumn = resolveColumnComparator(collationComparator);
  }

  static Comparator<Column<?>> resolveColumnComparator(
      Optional<CollationComparator> collationComparator) {
    return collationComparator
        .map(CollationComparator::columnComparator)
        .orElseGet(() -> (a, b) -> Ordering.natural().compare(a, b));
  }

  @Override
  public int compare(Map<String, Object> clusteringKey1, Map<String, Object> clusteringKey2) {
    for (String columnName : metadata.getClusteringKeyNames()) {
      Scan.Ordering.Order order = metadata.getClusteringOrder(columnName);

      DataType dataType = metadata.getColumnDataType(columnName);
      Column<?> column1 =
          ColumnValueMapper.convert(clusteringKey1.get(columnName), columnName, dataType);
      Column<?> column2 =
          ColumnValueMapper.convert(clusteringKey2.get(columnName), columnName, dataType);

      int cmp =
          order == Scan.Ordering.Order.ASC
              ? perColumn.compare(column1, column2)
              : perColumn.compare(column2, column1);
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }
}
