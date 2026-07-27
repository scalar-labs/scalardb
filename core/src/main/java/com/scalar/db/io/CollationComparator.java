package com.scalar.db.io;

import com.google.common.collect.Ordering;
import com.google.common.primitives.UnsignedBytes;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;
import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A thread-safe, immutable comparator that orders text according to the configured {@link
 * Collation}.
 *
 * <p>This is the shared ordering primitive consumed by the in-memory comparison sites (object
 * storage scan sorting and range filtering, in-memory range filtering, and the Consensus Commit
 * snapshot's scan-after-write range check). All three sites order text identically because they all
 * build on {@link #textComparator()} through {@link #columnComparator()} and {@link
 * #keyComparator()}.
 *
 * <p>It governs ordering only. Equality and identity comparisons are unchanged and stay byte-exact.
 *
 * <ul>
 *   <li>{@link Collation#BINARY} orders text by unsigned UTF-8 byte sequence, using Guava's {@link
 *       UnsignedBytes#lexicographicalComparator()} over {@link
 *       String#getBytes(java.nio.charset.Charset)} with UTF-8. This intentionally diverges from
 *       Java's natural UTF-16 code-unit order above U+FFFF, matching byte-order backends.
 *   <li>{@link Collation#ICU} orders text according to a frozen ICU {@link Collator} built from the
 *       configured locale and strength, or from a custom {@link RuleBasedCollator} tailoring-rule
 *       string. The collator is frozen at construction time so it is immutable and safe for
 *       concurrent {@code compare} calls.
 * </ul>
 *
 * <p>When {@code scalar.db.collation} is unset, {@link #from(DatabaseConfig)} returns {@link
 * Optional#empty()} so callers keep ScalarDB's current natural-order behavior.
 */
@Immutable
@ThreadSafe
public final class CollationComparator {

  private final Comparator<String> textComparator;
  private final Comparator<Column<?>> columnComparator;
  private final Comparator<Key> keyComparator;

  private CollationComparator(Comparator<String> textComparator) {
    this.textComparator = textComparator;
    Comparator<String> nullsFirstText = Comparator.nullsFirst(textComparator);
    this.columnComparator = buildColumnComparator(nullsFirstText);
    this.keyComparator = buildKeyComparator(this.columnComparator);
  }

  /**
   * Creates a {@code CollationComparator} from the given configuration.
   *
   * @param config the database configuration
   * @return an {@code Optional} holding the comparator when {@code scalar.db.collation} is set, or
   *     {@link Optional#empty()} when it is unset (callers keep current natural-order behavior)
   * @throws IllegalArgumentException if an ICU custom tailoring-rule string is malformed
   */
  public static Optional<CollationComparator> from(DatabaseConfig config) {
    Optional<Collation> collation = config.getCollation();
    if (!collation.isPresent()) {
      return Optional.empty();
    }
    switch (collation.get()) {
      case BINARY:
        return Optional.of(new CollationComparator(binaryTextComparator()));
      case ICU:
        return Optional.of(new CollationComparator(icuTextComparator(config)));
      default:
        throw new AssertionError("Unknown collation: " + collation.get());
    }
  }

  private static Comparator<String> binaryTextComparator() {
    Comparator<byte[]> byteComparator = UnsignedBytes.lexicographicalComparator();
    return (left, right) ->
        byteComparator.compare(
            left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));
  }

  private static Comparator<String> icuTextComparator(DatabaseConfig config) {
    Collator collator;
    Optional<String> rules = config.getCollationRules();
    if (rules.isPresent()) {
      try {
        collator = new RuleBasedCollator(rules.get());
      } catch (Exception e) {
        throw new IllegalArgumentException(
            CoreError.COLLATION_INVALID_RULES.buildMessage(rules.get()), e);
      }
    } else {
      Optional<String> localeName = config.getCollationLocale();
      if (localeName.isPresent()) {
        ULocale locale = new ULocale(localeName.get());
        collator = Collator.getInstance(locale);
        // ICU silently falls back to the root collation for a locale it has no collation data for,
        // which would order text differently from the intended locale with no error. Reject such a
        // locale so a misconfiguration fails at startup instead of producing wrong ordering. A
        // recognized locale resolves to a non-root VALID_LOCALE; ACTUAL_LOCALE is not usable here
        // because locales whose collation equals the root order (e.g. English) legitimately have an
        // empty ACTUAL_LOCALE.
        ULocale validLocale = collator.getLocale(ULocale.VALID_LOCALE);
        if (validLocale == null || validLocale.getName().isEmpty()) {
          throw new IllegalArgumentException(
              CoreError.COLLATION_UNRECOGNIZED_LOCALE.buildMessage(localeName.get()));
        }
      } else {
        collator = Collator.getInstance(ULocale.ROOT);
      }
    }
    config
        .getCollationStrength()
        .ifPresent(strength -> collator.setStrength(toIcuStrength(strength)));

    // Freeze so the collator is immutable and safe for concurrent compare: an unfrozen ICU
    // Collator is mutable and not thread-safe, while a frozen one is safe for concurrent compare.
    Collator frozen = collator.freeze();
    return frozen::compare;
  }

  private static int toIcuStrength(CollationStrength strength) {
    switch (strength) {
      case PRIMARY:
        return Collator.PRIMARY;
      case SECONDARY:
        return Collator.SECONDARY;
      case TERTIARY:
        return Collator.TERTIARY;
      case QUATERNARY:
        return Collator.QUATERNARY;
      case IDENTICAL:
        return Collator.IDENTICAL;
      default:
        throw new AssertionError("Unknown collation strength: " + strength);
    }
  }

  private static Comparator<Column<?>> buildColumnComparator(Comparator<String> nullsFirstText) {
    return (left, right) -> {
      if (left.getDataType() == DataType.TEXT && right.getDataType() == DataType.TEXT) {
        // Preserve TextColumn's null-first semantics; value ordering is collation-aware. Names are
        // equal at the call sites, so we compare values only (identity is unchanged).
        return nullsFirstText.compare(left.getTextValue(), right.getTextValue());
      }
      // Non-text columns keep natural ordering.
      return Ordering.natural().compare(left, right);
    };
  }

  private static Comparator<Key> buildKeyComparator(Comparator<Column<?>> columnComparator) {
    return (left, right) ->
        Ordering.from(columnComparator)
            .lexicographical()
            .compare(left.getColumns(), right.getColumns());
  }

  /**
   * Returns the collation-aware comparator over non-null text values.
   *
   * @return the text comparator
   */
  public Comparator<String> textComparator() {
    return textComparator;
  }

  /**
   * Returns the shared per-column comparator. Two {@code TEXT} columns are ordered by value using
   * {@link #textComparator()} with null-first semantics; any other column type delegates to natural
   * ordering.
   *
   * @return the column comparator
   */
  public Comparator<Column<?>> columnComparator() {
    return columnComparator;
  }

  /**
   * Returns a lexicographical (always-ascending) comparator over a {@link Key}'s columns, built on
   * {@link #columnComparator()}.
   *
   * @return the key comparator
   */
  public Comparator<Key> keyComparator() {
    return keyComparator;
  }
}
