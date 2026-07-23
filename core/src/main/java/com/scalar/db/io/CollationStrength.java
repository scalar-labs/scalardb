package com.scalar.db.io;

/**
 * The collation strength used in {@link Collation#ICU} mode, controlling how much detail the
 * ordering distinguishes (case- and accent-sensitivity).
 *
 * <p>These values correspond to the strength levels of the Unicode Collation Algorithm (UCA) as
 * implemented by ICU. This is ScalarDB's own enum so that this configuration does not depend on
 * ICU4J; the mapping to ICU's {@code Collator} strength constants is performed elsewhere.
 *
 * <ul>
 *   <li>{@link #PRIMARY} distinguishes base characters only (case- and accent-insensitive).
 *   <li>{@link #SECONDARY} adds accent sensitivity.
 *   <li>{@link #TERTIARY} adds case sensitivity.
 *   <li>{@link #QUATERNARY} adds further distinctions (for example punctuation in some locales).
 *   <li>{@link #IDENTICAL} distinguishes code point differences as a last resort.
 * </ul>
 */
public enum CollationStrength {
  PRIMARY,
  SECONDARY,
  TERTIARY,
  QUATERNARY,
  IDENTICAL
}
