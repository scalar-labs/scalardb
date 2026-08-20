package com.scalar.db.io;

import com.google.common.collect.Ordering;
import com.google.common.primitives.UnsignedBytes;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;
import com.ibm.icu.util.VersionInfo;
import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <ul>
 *   <li>{@link Collation#BINARY} orders text by unsigned UTF-8 byte sequence, using Guava's {@link
 *       UnsignedBytes#lexicographicalComparator()} over {@link
 *       String#getBytes(java.nio.charset.Charset)} with UTF-8. This intentionally diverges from
 *       Java's natural UTF-16 code-unit order above U+FFFF, matching byte-order backends.
 *   <li>{@link Collation#ICU} orders text according to a frozen ICU {@link Collator} built from the
 *       configured locale and strength, optionally extended by a custom {@link RuleBasedCollator}
 *       tailoring-rule string that builds on the locale's collation. The collator is frozen at
 *       construction time so it is immutable and safe for concurrent {@code compare} calls.
 * </ul>
 *
 * <p>A comparator always exists: when {@code scalar.db.collation} is unset, the configuration
 * defaults to {@link Collation#BINARY}, so {@link #from(DatabaseConfig)} never returns absent.
 */
@Immutable
@ThreadSafe
public final class CollationComparator {
  private static final Logger logger = LoggerFactory.getLogger(CollationComparator.class);

  /**
   * Resource carrying the ICU4J version this build was verified against, expanded from the Gradle
   * {@code icu4jVersion} property at build time.
   */
  private static final String VERIFIED_ICU_VERSION_RESOURCE = "scalardb-collation.properties";

  private static final AtomicBoolean ICU_VERSION_CHECKED = new AtomicBoolean();

  private final Comparator<String> textComparator;
  private final Comparator<Column<?>> columnComparator;
  private final Comparator<Key> keyComparator;
  private final boolean byteExactEquality;

  /**
   * Per-thread canonical-form producer, present only for {@link Collation#ICU}. Collation-key
   * generation on a shared frozen collator serializes on a JVM-wide lock, so each thread gets its
   * own thawed clone; a thawed collator is safe for single-threaded use and produces the same
   * collation keys as the frozen original it was cloned from.
   */
  @Nullable private final ThreadLocal<Collator> canonicalizer;

  private CollationComparator(
      Comparator<String> textComparator,
      boolean byteExactEquality,
      @Nullable ThreadLocal<Collator> canonicalizer) {
    this.textComparator = textComparator;
    Comparator<String> nullsFirstText = Comparator.nullsFirst(textComparator);
    this.columnComparator = buildColumnComparator(nullsFirstText);
    this.keyComparator = buildKeyComparator(this.columnComparator);
    this.byteExactEquality = byteExactEquality;
    this.canonicalizer = canonicalizer;
  }

  /**
   * Creates a {@code CollationComparator} from the given configuration.
   *
   * @param config the database configuration
   * @return the comparator for the configured collation ({@link Collation#BINARY} when {@code
   *     scalar.db.collation} is unset)
   * @throws IllegalArgumentException if an ICU custom tailoring-rule string is malformed or the
   *     configured ICU locale is not recognized
   */
  public static CollationComparator from(DatabaseConfig config) {
    Collation collation = config.getCollation();
    switch (collation) {
      case BINARY:
        // BINARY equality must be exact String equality: UTF-8 encoding is injective for
        // well-formed strings, but String#getBytes replaces unpaired surrogates with '?', which
        // would conflate distinct ill-formed strings if equality went through the comparator.
        // BINARY has no canonical text form: identity is the value itself.
        return new CollationComparator(binaryTextComparator(), true, null);
      case ICU:
        {
          warnOnIcuVersionMismatch();
          Collator frozen = buildFrozenIcuCollator(config);
          return new CollationComparator(
              frozen::compare, false, ThreadLocal.withInitial(frozen::cloneAsThawed));
        }
      default:
        throw new AssertionError("Unknown collation: " + collation);
    }
  }

  /**
   * Warns once per JVM when the ICU4J library loaded at runtime differs from the version this build
   * was verified against. ICU4J is a plain transitive dependency, so an embedding application's
   * dependency resolution can substitute another version, and ICU collation behavior (root sort
   * order, CLDR data) shifts between versions — ordering and equality results would silently differ
   * from the verified behavior. The check never fails collation setup: it cannot tell a harmless
   * substitution from a harmful one, only make it diagnosable.
   */
  private static void warnOnIcuVersionMismatch() {
    if (!ICU_VERSION_CHECKED.compareAndSet(false, true)) {
      return;
    }
    String verifiedVersion = null;
    try (InputStream stream =
        CollationComparator.class
            .getClassLoader()
            .getResourceAsStream(VERIFIED_ICU_VERSION_RESOURCE)) {
      if (stream != null) {
        Properties properties = new Properties();
        properties.load(stream);
        verifiedVersion = properties.getProperty("icu4j.version");
      }
    } catch (IOException e) {
      logger.debug("Failed to read {}", VERIFIED_ICU_VERSION_RESOURCE, e);
    }
    if (verifiedVersion == null) {
      logger.debug(
          "The verified ICU4J version is unavailable from {}; skipping the ICU4J version check",
          VERIFIED_ICU_VERSION_RESOURCE);
      return;
    }
    try {
      if (loadedIcuVersionDiffersFrom(verifiedVersion)) {
        logger.warn(
            "The ICU4J library loaded at runtime (version {}) differs from the version this "
                + "ScalarDB build was verified against ({}). ICU collation behavior shifts "
                + "between ICU4J versions, so ICU-mode text ordering and equality may differ "
                + "from the verified behavior. Align the application's icu4j dependency with "
                + "ScalarDB's",
            VersionInfo.ICU_VERSION,
            verifiedVersion);
      }
    } catch (IllegalArgumentException e) {
      logger.debug("Failed to compare the ICU4J versions", e);
    }
  }

  /**
   * Returns whether the ICU4J library on the classpath differs from the given version.
   *
   * <p>Package-private and pure so it can be unit-tested without manipulating the classpath.
   *
   * @param version an ICU version string (e.g. {@code "77.1"})
   * @return true if the loaded ICU4J version differs from the given version
   * @throws IllegalArgumentException if the given version string is malformed
   */
  static boolean loadedIcuVersionDiffersFrom(String version) {
    return !VersionInfo.getInstance(version).equals(VersionInfo.ICU_VERSION);
  }

  private static Comparator<String> binaryTextComparator() {
    Comparator<byte[]> byteComparator = UnsignedBytes.lexicographicalComparator();
    return (left, right) ->
        byteComparator.compare(
            left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));
  }

  private static Collator buildFrozenIcuCollator(DatabaseConfig config) {
    Collator collator = buildIcuCollator(config);
    config
        .getCollationIcuStrength()
        .ifPresent(strength -> collator.setStrength(toIcuStrength(strength)));

    // Freeze so the collator is immutable and safe for concurrent compare: an unfrozen ICU
    // Collator is mutable and not thread-safe, while a frozen one is safe for concurrent compare.
    return collator.freeze();
  }

  private static Collator buildIcuCollator(DatabaseConfig config) {
    // The base collation is the configured locale's collation, or the root collation when no locale
    // is configured. Custom tailoring rules, when present, extend that base rather than replacing
    // it
    // so they fine-tune ordering beyond the locale (and strength), not instead of it.
    Optional<String> localeName = config.getCollationIcuLocale();
    Collator base =
        localeName.isPresent()
            ? buildValidatedLocaleCollator(localeName.get())
            : Collator.getInstance(ULocale.ROOT);

    Optional<String> rules = config.getCollationIcuRules();
    if (!rules.isPresent()) {
      return base;
    }

    // Compose: prepend the base collation's rules so the custom tailoring builds on the locale.
    // For the root base this is empty, so rules-only ordering is unchanged.
    String baseRules =
        base instanceof RuleBasedCollator ? ((RuleBasedCollator) base).getRules() : "";
    try {
      return new RuleBasedCollator(baseRules + rules.get());
    } catch (Exception e) {
      throw new IllegalArgumentException(
          CoreError.COLLATION_INVALID_RULES.buildMessage(rules.get()), e);
    }
  }

  private static Collator buildValidatedLocaleCollator(String localeName) {
    Collator collator = Collator.getInstance(new ULocale(localeName));
    // ICU silently falls back to the root collation for a locale it has no collation data for,
    // which would order text differently from the intended locale with no error. Reject such a
    // locale so a misconfiguration fails at startup instead of producing wrong ordering. A
    // recognized locale resolves to a non-root VALID_LOCALE; ACTUAL_LOCALE is not usable here
    // because locales whose collation equals the root order (e.g. English) legitimately have an
    // empty ACTUAL_LOCALE.
    ULocale validLocale = collator.getLocale(ULocale.VALID_LOCALE);
    if (validLocale == null || validLocale.getName().isEmpty()) {
      throw new IllegalArgumentException(
          CoreError.COLLATION_UNRECOGNIZED_LOCALE.buildMessage(localeName));
    }
    return collator;
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

  /**
   * Returns whether the two non-null text values are equal under the configured collation. For
   * {@link Collation#BINARY} this is exact {@link String#equals} (byte-exact; equivalent to UTF-8
   * byte equality for all well-formed strings, and stricter for ill-formed ones with unpaired
   * surrogates, which {@code String#getBytes} would conflate via replacement bytes). For {@link
   * Collation#ICU} it is the collation's equality, {@code textComparator().compare(a, b) == 0}.
   * Callers use this only for {@code TEXT} and handle nulls themselves; like {@link
   * #textComparator()}, both arguments must be non-null.
   *
   * @param a the first text value (non-null)
   * @param b the second text value (non-null)
   * @return {@code true} when the values are equal under the collation
   */
  public boolean textEquals(String a, String b) {
    if (byteExactEquality) {
      return a.equals(b);
    }
    return textComparator.compare(a, b) == 0;
  }

  /**
   * Returns whether this collation materializes a canonical text form. {@code true} for {@link
   * Collation#ICU}: two text values have equal canonical forms exactly when they collate-equal.
   * {@code false} for {@link Collation#BINARY}: identity is the value itself (byte-exact), so no
   * canonical form is materialized.
   *
   * @return {@code true} when {@link #canonicalTextFormOf(String)} is usable
   */
  public boolean hasCanonicalTextForm() {
    return canonicalizer != null;
  }

  /**
   * Returns the canonical byte form of the given non-null text value under the configured {@link
   * Collation#ICU} collation — the collation key bytes, satisfying: {@code
   * Arrays.equals(canonicalTextFormOf(a), canonicalTextFormOf(b))} iff {@code
   * textComparator().compare(a, b) == 0}. Generation uses a per-thread collator, so this is safe
   * for concurrent use and does not contend on a shared lock.
   *
   * @param text the text value (non-null)
   * @return the canonical collation-key bytes
   * @throws IllegalStateException if this collation has no canonical text form ({@link
   *     Collation#BINARY}; check {@link #hasCanonicalTextForm()} first)
   */
  public byte[] canonicalTextFormOf(String text) {
    if (canonicalizer == null) {
      throw new IllegalStateException(
          "The BINARY collation has no canonical text form; identity is the value itself");
    }
    return canonicalizer.get().getCollationKey(text).toByteArray();
  }
}
