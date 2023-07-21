package com.scalar.db.api;

/**
 * The level of atomicity of the storage. It is the scope within which the storage can atomically
 * execute mutations.
 */
public enum DistributedStorageAtomicityLevel {
  /** This atomicity level indicates the storage can atomically execute mutations for a record. */
  RECORD,

  /**
   * This atomicity level indicates the storage can atomically execute mutations for a partition.
   */
  PARTITION,

  /** This atomicity level indicates the storage can atomically execute mutations for a table. */
  TABLE,

  /**
   * This atomicity level indicates the storage can atomically execute mutations for a namespace.
   */
  NAMESPACE,

  /** This atomicity level indicates the storage can atomically execute mutations for a storage. */
  STORAGE
}
