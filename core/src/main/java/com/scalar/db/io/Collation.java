package com.scalar.db.io;

/**
 * Selects ScalarDB's in-memory text comparison mode: it governs ordering, equality, and — under
 * {@link #ICU}, within the Consensus Commit transaction layer — key identity. Comparisons delegated to the backend
 * are unaffected, and stored bytes are never rewritten.
 */
public enum Collation {
  /** Orders text by unsigned UTF-8 byte sequence; equality is byte-exact. */
  BINARY,

  /**
   * Orders text according to the Unicode Collation Algorithm, configured by locale and optional
   * tailoring rules; equality follows the collation.
   */
  ICU
}
