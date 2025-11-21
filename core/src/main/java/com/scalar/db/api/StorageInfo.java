package com.scalar.db.api;

public interface StorageInfo {
  /**
   * Returns the storage name.
   *
   * @return the storage name
   */
  String getStorageName();

  /**
   * Returns the mutation atomicity unit of the storage.
   *
   * @return the mutation atomicity unit of the storage
   */
  MutationAtomicityUnit getMutationAtomicityUnit();

  /**
   * Returns the maximum number of mutations that can be performed atomically in the storage.
   *
   * @return the maximum number of mutations that can be performed atomically in the storage
   */
  int getMaxAtomicMutationsCount();

  /**
   * Returns whether the storage guarantees consistent reads for virtual tables.
   *
   * @return true if the storage guarantees consistent reads for virtual tables, false otherwise
   */
  boolean isConsistentVirtualTableReadGuaranteed();

  /**
   * The mutation atomicity unit of the storage.
   *
   * <p>This enum defines the atomicity unit for mutations in the storage. It determines the scope
   * of atomicity for mutations such as put and delete.
   */
  enum MutationAtomicityUnit {
    /**
     * The atomicity unit is at the record level, meaning that mutations are performed atomically
     * for each record.
     */
    RECORD,

    /**
     * The atomicity unit is at the partition level, meaning that mutations are performed atomically
     * for each partition.
     */
    PARTITION,

    /**
     * The atomicity unit is at the table level, meaning that mutations are performed atomically for
     * each table.
     */
    TABLE,

    /**
     * The atomicity unit is at the namespace level, meaning that mutations are performed atomically
     * for each namespace.
     */
    NAMESPACE,

    /**
     * The atomicity unit is at the storage level, meaning that mutations are performed atomically
     * for the entire storage.
     */
    STORAGE
  }
}
