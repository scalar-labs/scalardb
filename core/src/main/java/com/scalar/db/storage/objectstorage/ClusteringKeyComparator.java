package com.scalar.db.storage.objectstorage;

import com.google.common.collect.Ordering;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import java.util.Comparator;
import java.util.Map;

public class ClusteringKeyComparator implements Comparator<Map<String, Object>> {
  private final TableMetadata metadata;

  public ClusteringKeyComparator(TableMetadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public int compare(Map<String, Object> clusteringKey1, Map<String, Object> clusteringKey2) {
    for (String columnName : metadata.getClusteringKeyNames()) {
      Scan.Ordering.Order order = metadata.getClusteringOrder(columnName);

      Column<?> column1 =
          ColumnValueMapper.convert(
              clusteringKey1.get(columnName), columnName, metadata.getColumnDataType(columnName));
      Column<?> column2 =
          ColumnValueMapper.convert(
              clusteringKey2.get(columnName), columnName, metadata.getColumnDataType(columnName));

      int cmp =
          order == Scan.Ordering.Order.ASC
              ? Ordering.natural().compare(column1, column2)
              : Ordering.natural().compare(column2, column1);
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }
}
