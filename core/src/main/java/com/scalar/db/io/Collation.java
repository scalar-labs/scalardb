package com.scalar.db.io;

/**
 * Selects ScalarDB's in-memory text comparison mode: it governs ordering, equality, and — under
 * {@link #ICU}, within the Consensus Commit transaction layer — key identity.
 *
 * <p>This covers only the comparisons ScalarDB performs itself on the JVM (for example
 * object-storage scan sorting and filtering, in-memory cross-partition filtering, conditional
 * mutations, and the Consensus Commit snapshot's bookkeeping and scan-after-write checks).
 * Comparisons delegated to the backend are unaffected, and stored bytes are never rewritten.
 *
 * <ul>
 *   <li>{@link #BINARY} — the default when {@code scalar.db.collation} is absent. Orders text by
 *       unsigned UTF-8 byte sequence; equality and key identity are byte-exact.
 *   <li>{@link #ICU} — orders text according to the Unicode Collation Algorithm, configured by
 *       locale, strength, and optional tailoring rules; equality and Consensus Commit key identity
 *       follow the collation (for example, case-insensitive at {@code PRIMARY} strength).
 * </ul>
 */
public enum Collation {
  BINARY,
  ICU
}
