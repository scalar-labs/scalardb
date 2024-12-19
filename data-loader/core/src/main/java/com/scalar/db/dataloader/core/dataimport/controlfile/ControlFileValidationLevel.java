package com.scalar.db.dataloader.core.dataimport.controlfile;

/** Control file validation level */
public enum ControlFileValidationLevel {
  /* All columns need to be mapped */
  FULL,
  /* All partition key and clustering key columns need to be mapped */
  KEYS,
  /* Only validate the columns that are mapped */
  MAPPED
}
