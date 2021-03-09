package com.scalar.db.storage.common.metadata;

import com.scalar.db.api.Scan;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * An interface for the table metadata
 *
 * @author Yuji Ito
 */
public interface TableMetadata {
  /**
   * Returns the partition key names
   *
   * @return an {@code LinkedHashSet} of partition key names
   */
  LinkedHashSet<String> getPartitionKeyNames();

  /**
   * Returns the clustering key names
   *
   * @return an {@code LinkedHashSet} of clustering key names
   */
  LinkedHashSet<String> getClusteringKeyNames();

  /**
   * Returns the secondary index names
   *
   * @return an {@code Set} of secondary index names
   */
  Set<String> getSecondaryIndexNames();

  /**
   * Returns the column names
   *
   * @return an {@code Set} of column names
   */
  Set<String> getColumnNames();

  /**
   * Returns the data type of the specified column
   *
   * @param columnName a column name to retrieve the data type
   * @return an {@code DataType} of the specified column
   */
  DataType getColumnDataType(String columnName);

  /**
   * Returns the specified clustering order
   *
   * @param clusteringKeyName a clustering key name to retrieve the order
   * @return an {@code Scan.Ordering.Order} of the specified clustering key
   */
  Scan.Ordering.Order getClusteringOrder(String clusteringKeyName);
}
