package com.scalar.db.dataloader.core.dataimport.task;

/** Represents the type of action to perform for an import task. */
public enum ImportTaskAction {
  /**
   * Insert action: Adds a new record to the database. Fails if the record with the same primary key
   * already exists.
   */
  INSERT,

  /**
   * Update action: Modifies an existing record in the database. Fails if the record does not
   * already exist.
   */
  UPDATE
}
