package com.scalar.db.storage;

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
   * @return an {@code Set} of partition key names
   */
  public Set<String> getPartitionKeyNames();

  /**
   * Returns the clustering key names
   *
   * @return an {@code Set} of clustering key names
   */
  public Set<String> getClusteringKeyNames();
}
