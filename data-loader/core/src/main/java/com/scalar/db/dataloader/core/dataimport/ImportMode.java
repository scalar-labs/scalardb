package com.scalar.db.dataloader.core.dataimport;

/** Represents the way to be imported data is handled */
public enum ImportMode {
  /**
   * Insert mode: Adds new rows to the database. Fails if the row with the same primary key already
   * exists.
   */
  INSERT,

  /**
   * Update mode: Modifies existing rows in the database. Fails if the row with the specified key
   * does not exist.
   */
  UPDATE,

  /** Upsert mode: Inserts new rows or updates existing ones if a row with the same key exists. */
  UPSERT
}
