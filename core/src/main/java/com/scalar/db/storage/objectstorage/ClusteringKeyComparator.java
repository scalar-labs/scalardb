package com.scalar.db.storage.objectstorage;

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
  public int compare(Map<String, Object> key1, Map<String, Object> key2) {
    for (String columnName : metadata.getClusteringKeyNames()) {
      Scan.Ordering.Order order = metadata.getClusteringOrder(columnName);

      Column<?> column1 =
          ColumnValueMapper.convert(
              key1.get(columnName), columnName, metadata.getColumnDataType(columnName));
      Column<?> column2 =
          ColumnValueMapper.convert(
              key2.get(columnName), columnName, metadata.getColumnDataType(columnName));

      int cmp =
          new ColumnComparator(metadata.getColumnDataType(columnName)).compare(column1, column2);
      if (cmp != 0) {
        return order == Scan.Ordering.Order.ASC ? cmp : -cmp;
      }
    }
    return 0;
  }
}
