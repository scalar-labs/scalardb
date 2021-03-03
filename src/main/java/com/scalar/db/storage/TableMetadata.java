package com.scalar.db.storage;

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
}
