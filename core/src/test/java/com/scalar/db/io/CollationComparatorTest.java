package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class CollationComparatorTest {

  private static final String ANY_HOST = "localhost";

  private static DatabaseConfig config(Properties extra) {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.putAll(extra);
    return new DatabaseConfig(props);
  }

  private static Properties props(String... keyValues) {
    Properties props = new Properties();
    for (int i = 0; i < keyValues.length; i += 2) {
      props.setProperty(keyValues[i], keyValues[i + 1]);
    }
    return props;
  }

  private static int sign(int value) {
    return Integer.compare(value, 0);
  }

  // ---- Unset (defaults to BINARY) ----

  @Test
  public void from_WhenCollationUnset_ShouldReturnBinaryComparator() {
    // Arrange: scalar.db.collation unset defaults to BINARY, so a comparator always exists.
    DatabaseConfig config = config(new Properties());

    // Act
    CollationComparator comparator = CollationComparator.from(config);

    // Assert: byte-exact equality and unsigned UTF-8 byte order, i.e. the BINARY collation.
    assertThat(comparator).isNotNull();
    assertThat(comparator.textEquals("Apple", "apple")).isFalse();
    assertThat(comparator.textEquals("apple", "apple")).isTrue();
    assertThat(comparator.textComparator().compare("A", "a")).isNegative();
  }

  // ---- Collation-aware equality (textEquals follows the collation) ----

  @Test
  public void textEquals_WhenIcuCaseInsensitive_ShouldFollowCollation() {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_STRENGTH, "PRIMARY")));

    // Act Assert: equality follows the collation whenever one is configured (no flag).
    assertThat(comparator.textEquals("Apple", "apple")).isTrue();
    assertThat(comparator.textEquals("apple", "banana")).isFalse();
  }

  @Test
  public void textEquals_WhenBinary_ShouldBeByteExact() {
    // Arrange (Covers KTD2/AE2): BINARY collation equality is byte-exact.
    CollationComparator comparator =
        CollationComparator.from(config(props(DatabaseConfig.COLLATION, "BINARY")));

    // Act Assert
    assertThat(comparator.textEquals("Apple", "apple")).isFalse();
    assertThat(comparator.textEquals("apple", "apple")).isTrue();
    // For well-formed strings, byte-exact equality agrees with String equality across non-ASCII
    // and supplementary-plane (4-byte UTF-8) text, so it matches ScalarDB's unset byte-exact
    // behavior there too.
    assertThat(comparator.textEquals("café", "café")).isTrue();
    assertThat(comparator.textEquals("café", "cafe")).isFalse();
    assertThat(comparator.textEquals("😀", "😀")).isTrue();
    assertThat(comparator.textEquals("😀", "😁")).isFalse();
  }

  @Test
  public void textEquals_WhenBinaryWithUnpairedSurrogates_ShouldMatchStringEquals() {
    // Arrange: String#getBytes(UTF_8) replaces every unpaired surrogate with '?', so distinct
    // ill-formed strings encode to the same bytes. BINARY equality must stay exact String
    // equality (the pre-collation default), not the conflating byte view.
    CollationComparator comparator =
        CollationComparator.from(config(props(DatabaseConfig.COLLATION, "BINARY")));

    // Act Assert
    assertThat(comparator.textEquals("\uD800", "\uDC00")).isFalse();
    assertThat(comparator.textEquals("\uD800", "?")).isFalse();
    assertThat(comparator.textEquals("a\uD800", "a?")).isFalse();
    assertThat(comparator.textEquals("\uD800", "\uD800")).isTrue();
  }

  // ---- BINARY happy path ----

  @Test
  public void textComparator_WhenBinary_ShouldOrderAsciiByByteValue() {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(config(props(DatabaseConfig.COLLATION, "BINARY")));
    Comparator<String> textComparator = comparator.textComparator();

    // Act Assert
    // In ASCII/UTF-8, 'A' (0x41) < 'a' (0x61).
    assertThat(textComparator.compare("A", "a")).isNegative();
    assertThat(textComparator.compare("a", "A")).isPositive();
    assertThat(textComparator.compare("a", "a")).isZero();
    assertThat(textComparator.compare("apple", "banana")).isNegative();
  }

  // ---- ICU PRIMARY / TERTIARY ----

  @Test
  public void textComparator_WhenIcuPrimaryStrength_ShouldTreatCaseAndAccentAsEqualOrdering() {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_STRENGTH, "PRIMARY")));
    Comparator<String> textComparator = comparator.textComparator();

    // Act Assert
    assertThat(textComparator.compare("a", "A")).isZero();
    assertThat(textComparator.compare("a", "á")).isZero(); // 'a' vs 'á'
    assertThat(textComparator.compare("A", "á")).isZero(); // 'A' vs 'á'
  }

  @Test
  public void textComparator_WhenIcuTertiaryStrength_ShouldDistinguishCase() {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_STRENGTH, "TERTIARY")));
    Comparator<String> textComparator = comparator.textComparator();

    // Act Assert
    assertThat(textComparator.compare("a", "A")).isNotZero();
  }

  // ---- Supplementary plane divergence (BINARY vs Java natural order) ----

  @Test
  public void textComparator_WhenBinary_ShouldDivergeFromJavaNaturalOrderAboveBmp() {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(config(props(DatabaseConfig.COLLATION, "BINARY")));
    Comparator<String> textComparator = comparator.textComparator();
    String uFFFF = "￿"; // U+FFFF, UTF-8 EF BF BF
    String u10000 = "𐀀"; // U+10000, UTF-8 F0 90 80 80

    // Act Assert
    // Under BINARY (UTF-8 bytes), EF BF BF < F0 90 80 80, so U+FFFF < U+10000.
    assertThat(textComparator.compare(uFFFF, u10000)).isNegative();
    // Java String.compareTo orders them the other way (surrogate 0xD800 < 0xFFFF),
    // documenting the divergence.
    assertThat(u10000.compareTo(uFFFF)).isNegative();
  }

  // ---- Custom tailoring rules ----

  @Test
  public void textComparator_WhenIcuWithCustomRules_ShouldProduceTailoredOrder() {
    // Arrange: reorder so 'b' sorts before 'a'.
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_RULES, "& b < a")));
    Comparator<String> textComparator = comparator.textComparator();

    // Act Assert
    assertThat(textComparator.compare("b", "a")).isNegative();
  }

  @Test
  public void from_WhenIcuWithMalformedRules_ShouldThrowIllegalArgumentException() {
    // Arrange
    DatabaseConfig config =
        config(
            props(
                DatabaseConfig.COLLATION, "ICU",
                DatabaseConfig.COLLATION_ICU_RULES, "this is not a valid rule <<< &&&"));

    // Act Assert
    assertThatThrownBy(() -> CollationComparator.from(config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void textComparator_WhenIcuWithLocaleAndCustomRules_ShouldComposeBaseLocaleWithRules() {
    // Arrange: the same custom rule (reorder so 'b' sorts before 'a') with and without a locale.
    Comparator<String> localePlusRules =
        CollationComparator.from(
                config(
                    props(
                        DatabaseConfig.COLLATION, "ICU",
                        DatabaseConfig.COLLATION_ICU_LOCALE, "sv",
                        DatabaseConfig.COLLATION_ICU_RULES, "& b < a")))
            .textComparator();
    Comparator<String> rulesOnly =
        CollationComparator.from(
                config(
                    props(
                        DatabaseConfig.COLLATION, "ICU",
                        DatabaseConfig.COLLATION_ICU_RULES, "& b < a")))
            .textComparator();

    // Assert: the custom rule applies in both cases.
    assertThat(localePlusRules.compare("b", "a")).isNegative();
    assertThat(rulesOnly.compare("b", "a")).isNegative();

    // The Swedish base collation is retained when rules are present (Swedish sorts 'z' before 'ö'),
    // unlike the root base used when only rules are set (root sorts 'ö' before 'z'). This proves
    // the
    // locale is composed with the rules rather than ignored.
    assertThat(localePlusRules.compare("z", "ö")).isNegative();
    assertThat(rulesOnly.compare("z", "ö")).isPositive();
  }

  // ---- Canonical text form (increment B: ICU-only, per-thread) ----

  private static CollationComparator icuPrimary() {
    return CollationComparator.from(
        config(
            props(
                DatabaseConfig.COLLATION, "ICU",
                DatabaseConfig.COLLATION_ICU_STRENGTH, "PRIMARY")));
  }

  @Test
  public void canonicalTextForm_WhenIcuPrimary_ShouldEquateCollateEqualValues() {
    // Arrange
    CollationComparator comparator = icuPrimary();

    // Act Assert: collate-equal values share one canonical form; distinct values do not.
    assertThat(comparator.hasCanonicalTextForm()).isTrue();
    assertThat(comparator.canonicalTextFormOf("Apple"))
        .isEqualTo(comparator.canonicalTextFormOf("apple"));
    assertThat(comparator.canonicalTextFormOf("Apple"))
        .isNotEqualTo(comparator.canonicalTextFormOf("banana"));
  }

  @Test
  public void canonicalTextForm_WhenIcuTertiary_ShouldDistinguishCase() {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_STRENGTH, "TERTIARY")));

    // Act Assert: strength is honored by the canonical form.
    assertThat(comparator.canonicalTextFormOf("Apple"))
        .isNotEqualTo(comparator.canonicalTextFormOf("apple"));
  }

  @Test
  public void canonicalTextForm_WhenIcuWithLocaleAndRules_ShouldFollowTailoredCollator() {
    // Arrange: with '& b < a', 'b' and 'a' stay distinct but tailored; the canonical form must
    // come from the same tailored collator as ordering, so equality classes match compare()==0.
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_LOCALE, "sv",
                    DatabaseConfig.COLLATION_ICU_RULES, "& b < a")));

    // Act Assert
    assertThat(comparator.canonicalTextFormOf("b"))
        .isNotEqualTo(comparator.canonicalTextFormOf("a"));
    assertThat(comparator.canonicalTextFormOf("v")).isEqualTo(comparator.canonicalTextFormOf("v"));
  }

  @Test
  public void canonicalTextForm_InvariantSweep_CanonicalEqualityMatchesCompareZero() {
    // Arrange: across ASCII, accented, and supplementary-plane text, canonical-bytes equality must
    // coincide exactly with compare == 0 (KTD2 invariant).
    CollationComparator comparator = icuPrimary();
    String[] corpus = {
      "apple", "Apple", "APPLE", "banana", "café", "cafe", "", "á", "a", "😀", "𐀀"
    };

    // Act Assert
    for (String a : corpus) {
      for (String b : corpus) {
        boolean canonicalEqual =
            Arrays.equals(comparator.canonicalTextFormOf(a), comparator.canonicalTextFormOf(b));
        boolean compareEqual = comparator.textComparator().compare(a, b) == 0;
        assertThat(canonicalEqual)
            .as("canonical equality must match compare==0 for ('%s', '%s')", a, b)
            .isEqualTo(compareEqual);
      }
    }
  }

  @Test
  public void canonicalTextForm_WhenBinary_ShouldSignalNoMaterialization() {
    // Arrange (BINARY identity is the value itself; nothing is materialized).
    CollationComparator comparator =
        CollationComparator.from(config(props(DatabaseConfig.COLLATION, "BINARY")));

    // Act Assert
    assertThat(comparator.hasCanonicalTextForm()).isFalse();
    assertThatThrownBy(() -> comparator.canonicalTextFormOf("apple"))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void canonicalTextForm_ConcurrentGeneration_ShouldMatchSingleThreadedOutput()
      throws Exception {
    // Arrange: per-thread collators must produce identical canonical bytes across threads.
    CollationComparator comparator = icuPrimary();
    String[] values = {"apple", "Apple", "café", "😀", "banana"};
    List<byte[]> expected = new ArrayList<>();
    for (String v : values) {
      expected.add(comparator.canonicalTextFormOf(v));
    }
    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      List<Callable<Boolean>> jobs = new ArrayList<>();
      for (int i = 0; i < 64; i++) {
        jobs.add(
            () -> {
              for (int j = 0; j < values.length; j++) {
                if (!Arrays.equals(expected.get(j), comparator.canonicalTextFormOf(values[j]))) {
                  return false;
                }
              }
              return true;
            });
      }

      // Act Assert
      for (Future<Boolean> result : executor.invokeAll(jobs)) {
        assertThat(result.get()).isTrue();
      }
    } finally {
      executor.shutdownNow();
    }
  }

  // ---- Locale ----

  @Test
  public void from_WhenIcuWithLocale_ShouldBuildComparator() {
    // Arrange Act
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_LOCALE, "de")));

    // Assert
    assertThat(comparator).isNotNull();
    assertThat(comparator.textComparator().compare("a", "b")).isNegative();
  }

  @Test
  public void from_WhenIcuWithRegionQualifiedLocale_ShouldBuildComparator() {
    // Arrange Act: a region-qualified locale ICU recognizes (falls back to the language collation).
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_LOCALE, "en_US")));

    // Assert
    assertThat(comparator).isNotNull();
  }

  @Test
  public void from_WhenIcuWithUnrecognizedLocale_ShouldThrowIllegalArgumentException() {
    // Arrange: a locale ICU has no collation data for (it would silently fall back to root order).
    DatabaseConfig config =
        config(
            props(
                DatabaseConfig.COLLATION, "ICU",
                DatabaseConfig.COLLATION_ICU_LOCALE, "not_a_locale"));

    // Act Assert
    assertThatThrownBy(() -> CollationComparator.from(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not_a_locale");
  }

  // ---- Concurrency (guards KTD5) ----

  @Test
  public void textComparator_WhenFrozenIcuSharedAcrossThreads_ShouldBeConsistent()
      throws Exception {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_STRENGTH, "TERTIARY")));
    Comparator<String> textComparator = comparator.textComparator();
    int expected = sign(textComparator.compare("apple", "Banana"));

    int threads = 16;
    int iterationsPerThread = 5000;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    List<Callable<Boolean>> tasks = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      tasks.add(
          () -> {
            for (int i = 0; i < iterationsPerThread; i++) {
              if (sign(textComparator.compare("apple", "Banana")) != expected) {
                return false;
              }
            }
            return true;
          });
    }

    // Act
    List<Future<Boolean>> results = executor.invokeAll(tasks);
    executor.shutdown();

    // Assert
    for (Future<Boolean> result : results) {
      assertThat(result.get()).isTrue();
    }
  }

  // ---- Consistency across text / column / key comparators (guards drift) ----

  @Test
  public void columnAndKeyComparators_ShouldBeSignConsistentWithTextComparator_Binary() {
    assertConsistency(config(props(DatabaseConfig.COLLATION, "BINARY")));
  }

  @Test
  public void columnAndKeyComparators_ShouldBeSignConsistentWithTextComparator_Icu() {
    assertConsistency(
        config(
            props(
                DatabaseConfig.COLLATION, "ICU",
                DatabaseConfig.COLLATION_ICU_STRENGTH, "PRIMARY")));
  }

  private void assertConsistency(DatabaseConfig config) {
    CollationComparator comparator = CollationComparator.from(config);
    Comparator<String> textComparator = comparator.textComparator();
    Comparator<Column<?>> columnComparator = comparator.columnComparator();
    Comparator<Key> keyComparator = comparator.keyComparator();

    List<String> corpus =
        Arrays.asList("apple", "Apple", "banana", "Banana", "ápple", "cherry", "APPLE");
    String columnName = "c";

    for (String left : corpus) {
      for (String right : corpus) {
        int textSign = sign(textComparator.compare(left, right));

        int columnSign =
            sign(
                columnComparator.compare(
                    TextColumn.of(columnName, left), TextColumn.of(columnName, right)));
        assertThat(columnSign).as("column vs text for (%s, %s)", left, right).isEqualTo(textSign);

        int keySign =
            sign(
                keyComparator.compare(Key.ofText(columnName, left), Key.ofText(columnName, right)));
        assertThat(keySign).as("key vs text for (%s, %s)", left, right).isEqualTo(textSign);
      }
    }
  }

  // ---- columnComparator preserves TextColumn null-first semantics ----

  @Test
  public void columnComparator_ShouldOrderNullTextFirst() {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(config(props(DatabaseConfig.COLLATION, "BINARY")));
    Comparator<Column<?>> columnComparator = comparator.columnComparator();
    Column<?> nullColumn = TextColumn.ofNull("c");
    Column<?> valueColumn = TextColumn.of("c", "a");

    // Act Assert
    assertThat(columnComparator.compare(nullColumn, valueColumn)).isNegative();
    assertThat(columnComparator.compare(valueColumn, nullColumn)).isPositive();
    assertThat(columnComparator.compare(nullColumn, TextColumn.ofNull("c"))).isZero();
  }

  // ---- columnComparator delegates non-text to natural order ----

  @Test
  public void columnComparator_WhenNonTextColumns_ShouldUseNaturalOrder() {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(config(props(DatabaseConfig.COLLATION, "BINARY")));
    Comparator<Column<?>> columnComparator = comparator.columnComparator();
    IntColumn one = IntColumn.of("c", 1);
    IntColumn two = IntColumn.of("c", 2);

    // Act Assert
    assertThat(sign(columnComparator.compare(one, two))).isEqualTo(sign(one.compareTo(two)));
    assertThat(columnComparator.compare(one, two)).isNegative();
  }

  // ---- keyComparator lexicographical across multiple columns ----

  @Test
  public void keyComparator_ShouldOrderLexicographicallyAcrossColumns() {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_STRENGTH, "PRIMARY")));
    Comparator<Key> keyComparator = comparator.keyComparator();
    Key k1 = Key.newBuilder().addText("p", "apple").addInt("c", 1).build();
    Key k2 = Key.newBuilder().addText("p", "apple").addInt("c", 2).build();
    Key k3 = Key.newBuilder().addText("p", "APPLE").addInt("c", 5).build();

    // Act Assert
    assertThat(keyComparator.compare(k1, k2)).isNegative();
    // At PRIMARY strength, "apple" and "APPLE" collate equal, so the INT column breaks the tie.
    assertThat(keyComparator.compare(k1, k3)).isNegative(); // 1 < 5
    assertThat(keyComparator.compare(k3, k2)).isPositive(); // 5 > 2

    List<Key> sorted =
        java.util.stream.Stream.of(k2, k3, k1).sorted(keyComparator).collect(Collectors.toList());
    assertThat(sorted).containsExactly(k1, k2, k3);
  }

  @Test
  public void loadedIcuVersionDiffersFrom_LoadedVersion_ShouldReturnFalse() {
    // Act Assert
    assertThat(
            CollationComparator.loadedIcuVersionDiffersFrom(
                com.ibm.icu.util.VersionInfo.ICU_VERSION.toString()))
        .isFalse();
  }

  @Test
  public void loadedIcuVersionDiffersFrom_DifferentVersion_ShouldReturnTrue() {
    // Act Assert
    assertThat(CollationComparator.loadedIcuVersionDiffersFrom("1.0")).isTrue();
  }

  @Test
  public void loadedIcuVersionDiffersFrom_MalformedVersion_ShouldThrowIllegalArgumentException() {
    // Act Assert
    assertThatThrownBy(() -> CollationComparator.loadedIcuVersionDiffersFrom("not-a-version"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
