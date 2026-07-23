package com.scalar.db.io;

/**
 * Selects ScalarDB's in-memory string ordering mode.
 *
 * <p>This governs only the comparisons ScalarDB performs itself on the JVM (for example
 * object-storage scan sorting and range filtering, in-memory cross-partition range filtering, and
 * the Consensus Commit snapshot's scan-after-write range check). It does not affect comparisons
 * delegated to the backend, nor equality/identity comparisons, which stay byte-exact.
 *
 * <ul>
 *   <li>{@link #BINARY} orders text by unsigned UTF-8 byte sequence.
 *   <li>{@link #ICU} orders text according to the Unicode Collation Algorithm, configured by
 *       locale, strength, and optional tailoring rules.
 * </ul>
 *
 * <p>When {@code scalar.db.collation} is unset, ScalarDB keeps its current comparison behavior
 * (Java UTF-16 code-unit order) and no {@code Collation} is selected.
 */
public enum Collation {
  BINARY,
  ICU
}
