package com.scalar.db.api;

/** Represents storage information. */
public interface StorageInfo {
  /**
   * Returns the storage name.
   *
   * @return the storage name
   */
  String getStorageName();

  /**
   * Returns the atomicity unit of the storage.
   *
   * @return the atomicity unit of the storage
   */
  AtomicityUnit getAtomicityUnit();

  /**
   * Returns the maximum number of mutations that can be performed atomically in the storage.
   *
   * @return the maximum number of mutations that can be performed atomically in the storage
   */
  int getMaxAtomicMutationsCount();

  /**
   * Returns whether the storage guarantees consistent reads within its atomicity unit.
   *
   * @return true if the storage guarantees consistent reads within its atomicity unit, false
   *     otherwise
   */
  boolean isConsistentReadGuaranteed();

  /**
   * The atomicity unit of the storage.
   *
   * <p>This enum defines the atomicity unit for operations in the storage.
   */
  enum AtomicityUnit {
    /**
     * The atomicity unit is at the record level, meaning that operations are performed atomically
     * for each record.
     */
    RECORD,

    /**
     * The atomicity unit is at the partition level, meaning that operations are performed
     * atomically for each partition.
     */
    PARTITION,

    /**
     * The atomicity unit is at the table level, meaning that operations are performed atomically
     * for each table.
     */
    TABLE,

    /**
     * The atomicity unit is at the namespace level, meaning that operations are performed
     * atomically for each namespace.
     */
    NAMESPACE,

    /**
     * The atomicity unit is at the storage level, meaning that operations are performed atomically
     * for the entire storage.
     */
    STORAGE
  }
}
