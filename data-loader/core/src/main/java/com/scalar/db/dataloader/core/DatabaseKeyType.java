package com.scalar.db.dataloader.core;

/** Type of key in database */
public enum DatabaseKeyType {
  /** Represents a partition key, which determines the partition where the data is stored. */
  PARTITION,

  /** Represents a clustering key, which determines the order of data within a partition. */
  CLUSTERING
}
