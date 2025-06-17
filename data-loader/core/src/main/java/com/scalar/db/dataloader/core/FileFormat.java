package com.scalar.db.dataloader.core;

/** The available input and output formats for the data loader import and export commands */
public enum FileFormat {
  /**
   * JSON (JavaScript Object Notation) format. Typically, represents the entire dataset as a single
   * JSON array or object.
   */
  JSON,

  /**
   * JSON Lines (JSONL) format. Each line is a separate JSON object, making it suitable for
   * streaming large datasets.
   */
  JSONL,

  /**
   * CSV (Comma-Separated Values) format. A plain text format where each line represents a row and
   * columns are separated by commas.
   */
  CSV
}
