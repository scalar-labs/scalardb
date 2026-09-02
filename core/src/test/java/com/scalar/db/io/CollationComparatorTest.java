package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.common.CoreError;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

  @Test
  public void from_WhenCollationUnset_ShouldReturnBinaryComparator() {
    // Arrange
    DatabaseConfig config = config(new Properties());

    // Act
    CollationComparator comparator = CollationComparator.from(config);

    // Assert
    assertThat(comparator).isNotNull();
    assertThat(comparator.textEquals("Apple", "apple")).isFalse();
    assertThat(comparator.textEquals("apple", "apple")).isTrue();
    assertThat(comparator.textComparator().compare("A", "a")).isNegative();
  }

  @Test
  public void textEquals_WhenIcuCaseInsensitive_ShouldFollowCollation() {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_RULES, "[strength 1]")));

    // Act Assert
    assertThat(comparator.textEquals("Apple", "apple")).isTrue();
    assertThat(comparator.textEquals("apple", "banana")).isFalse();
  }

  @Test
  public void textEquals_WhenBinary_ShouldBeByteExact() {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(config(props(DatabaseConfig.COLLATION, "BINARY")));

    // Act Assert
    assertThat(comparator.textEquals("Apple", "apple")).isFalse();
    assertThat(comparator.textEquals("apple", "apple")).isTrue();
    assertThat(comparator.textEquals("café", "café")).isTrue();
    assertThat(comparator.textEquals("café", "cafe")).isFalse();
    assertThat(comparator.textEquals("😀", "😀")).isTrue();
    assertThat(comparator.textEquals("😀", "😁")).isFalse();
  }

  @Test
  public void textEquals_WhenBinaryWithUnpairedSurrogates_ShouldMatchStringEquals() {
    // Arrange: String#getBytes(UTF_8) replaces every unpaired surrogate with '?', so distinct
    // ill-formed strings encode to the same bytes.
    CollationComparator comparator =
        CollationComparator.from(config(props(DatabaseConfig.COLLATION, "BINARY")));

    // Act Assert
    assertThat(comparator.textEquals("\uD800", "\uDC00")).isFalse();
    assertThat(comparator.textEquals("\uD800", "?")).isFalse();
    assertThat(comparator.textEquals("a\uD800", "a?")).isFalse();
    assertThat(comparator.textEquals("\uD800", "\uD800")).isTrue();
  }

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

  @Test
  public void textComparator_WhenIcuPrimaryStrength_ShouldTreatCaseAndAccentAsEqualOrdering() {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_RULES, "[strength 1]")));
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
                    DatabaseConfig.COLLATION_ICU_RULES, "[strength 3]")));
    Comparator<String> textComparator = comparator.textComparator();

    // Act Assert
    assertThat(textComparator.compare("a", "A")).isNotZero();
  }

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
    // Java String.compareTo orders them the other way (surrogate 0xD800 < 0xFFFF).
    assertThat(u10000.compareTo(uFFFF)).isNegative();
  }

  @Test
  public void textComparator_WhenIcuWithCustomRules_ShouldProduceTailoredOrder() {
    // Arrange
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
    // Arrange
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

    // Assert
    assertThat(localePlusRules.compare("b", "a")).isNegative();
    assertThat(rulesOnly.compare("b", "a")).isNegative();

    // Swedish sorts 'z' before 'ö'; the root base used when only rules are set sorts 'ö' before
    // 'z'.
    assertThat(localePlusRules.compare("z", "ö")).isNegative();
    assertThat(rulesOnly.compare("z", "ö")).isPositive();
  }

  private static CollationComparator icuPrimary() {
    return CollationComparator.from(
        config(
            props(
                DatabaseConfig.COLLATION, "ICU",
                DatabaseConfig.COLLATION_ICU_RULES, "[strength 1]")));
  }

  @Test
  public void canonicalTextForm_WhenIcuPrimary_ShouldEquateCollateEqualValues() {
    // Arrange
    CollationComparator comparator = icuPrimary();

    // Act Assert
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
                    DatabaseConfig.COLLATION_ICU_RULES, "[strength 3]")));

    // Act Assert
    assertThat(comparator.canonicalTextFormOf("Apple"))
        .isNotEqualTo(comparator.canonicalTextFormOf("apple"));
  }

  @Test
  public void canonicalTextForm_WhenIcuWithLocaleAndRules_ShouldFollowTailoredCollator() {
    // Arrange: the canonical form must come from the same tailored collator as the ordering.
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
    // Arrange
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
    // Arrange
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
    // Arrange
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
    // Arrange Act
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_LOCALE, "en-US")));

    // Assert
    assertThat(comparator).isNotNull();
  }

  @Test
  public void from_WhenIcuWithCollationKeywordLocale_ShouldApplyThatTailoring() {
    // Arrange Act
    CollationComparator unihan =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_LOCALE, "ja-u-co-unihan")));
    CollationComparator standard =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_LOCALE, "ja")));

    // Assert: the unihan tailoring orders these two kanji opposite to the standard ja collation.
    assertThat(sign(unihan.textComparator().compare("一", "亜"))).isNegative();
    assertThat(sign(standard.textComparator().compare("一", "亜"))).isPositive();
  }

  @Test
  public void from_WhenIcuWithRedundantStandardCollationKeywordLocale_ShouldBuildComparator() {
    // Arrange Act
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_LOCALE, "th-u-co-standard")));

    // Assert
    assertThat(comparator).isNotNull();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "en",
        "en-US",
        "ja",
        "en-XX",
        "de-u-co-phonebk",
        "ja-u-co-unihan",
        "ja-u-kn-true",
        "zh-Hant-u-co-zhuyin",
        "und-u-co-emoji",
        "de-DE-u-co-phonebk",
        "en-US-POSIX",
        "th-u-co-standard"
      })
  public void from_WhenIcuWithAcceptedBcp47Locale_ShouldBuildComparator(String locale) {
    // Arrange Act
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU", DatabaseConfig.COLLATION_ICU_LOCALE, locale)));

    // Assert
    assertThat(comparator).isNotNull();
  }

  @Test
  public void from_WhenIcuWithUnderscoreSeparatedLocale_ShouldThrowIllegalArgumentException() {
    // Arrange
    DatabaseConfig config =
        config(
            props(
                DatabaseConfig.COLLATION, "ICU",
                DatabaseConfig.COLLATION_ICU_LOCALE, "en_US"));

    // Act Assert
    assertThatThrownBy(() -> CollationComparator.from(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(CoreError.COLLATION_INVALID_LOCALE_TAG.buildCode())
        .hasMessageContaining("en_US");
  }

  @Test
  public void from_WhenIcuWithGlibcStyleLocale_ShouldThrowIllegalArgumentException() {
    // Arrange
    DatabaseConfig config =
        config(
            props(
                DatabaseConfig.COLLATION, "ICU",
                DatabaseConfig.COLLATION_ICU_LOCALE, "en_US.UTF-8"));

    // Act Assert
    assertThatThrownBy(() -> CollationComparator.from(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(CoreError.COLLATION_INVALID_LOCALE_TAG.buildCode())
        .hasMessageContaining("en_US.UTF-8");
  }

  @Test
  public void from_WhenIcuWithLegacyKeywordSyntaxLocale_ShouldThrowIllegalArgumentException() {
    // Arrange
    DatabaseConfig config =
        config(
            props(
                DatabaseConfig.COLLATION, "ICU",
                DatabaseConfig.COLLATION_ICU_LOCALE, "ja@collation=unihan"));

    // Act Assert
    assertThatThrownBy(() -> CollationComparator.from(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(CoreError.COLLATION_INVALID_LOCALE_TAG.buildCode())
        .hasMessageContaining("ja@collation=unihan");
  }

  @Test
  public void from_WhenIcuWithUnrecognizedLocale_ShouldThrowIllegalArgumentException() {
    // Arrange: a well-formed tag ICU has no collation data for (it would fall back to root order).
    DatabaseConfig config =
        config(
            props(
                DatabaseConfig.COLLATION, "ICU",
                DatabaseConfig.COLLATION_ICU_LOCALE, "zz"));

    // Act Assert
    assertThatThrownBy(() -> CollationComparator.from(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(CoreError.COLLATION_UNRECOGNIZED_LOCALE.buildMessage("zz"));
  }

  @Test
  public void
      from_WhenIcuWithUnsupportedCollationKeywordLocale_ShouldThrowIllegalArgumentException() {
    // Arrange: "unihan" is not a de tailoring, so ICU would silently order by standard de.
    DatabaseConfig config =
        config(
            props(
                DatabaseConfig.COLLATION, "ICU",
                DatabaseConfig.COLLATION_ICU_LOCALE, "de-u-co-unihan"));

    // Act Assert
    assertThatThrownBy(() -> CollationComparator.from(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(CoreError.COLLATION_UNSUPPORTED_LOCALE_COLLATION.buildCode())
        .hasMessageContaining("de-u-co-unihan");
  }

  @Test
  public void from_WhenIcuWithExplicitRootLocale_ShouldBuildComparator() {
    // Arrange Act: an explicit root request is the collation used when no locale is configured.
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU", DatabaseConfig.COLLATION_ICU_LOCALE, "und")));

    // Assert
    assertThat(comparator).isNotNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"zz-u-co-emoji", "xx-u-co-search", "zz-u-co-eor"})
  public void
      from_WhenIcuWithUnknownLanguageAndAvailableCollationType_ShouldThrowIllegalArgumentException(
          String locale) {
    // Arrange: the collation keyword survives ICU's fallback to root, so the resolved locale is
    // non-empty even though the language does not exist.
    DatabaseConfig config =
        config(props(DatabaseConfig.COLLATION, "ICU", DatabaseConfig.COLLATION_ICU_LOCALE, locale));

    // Act Assert
    assertThatThrownBy(() -> CollationComparator.from(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(CoreError.COLLATION_UNRECOGNIZED_LOCALE.buildMessage(locale));
  }

  @Test
  public void from_WhenIcuWithNumericOrderingKeywordLocale_ShouldOrderDigitRunsNumerically() {
    // Arrange Act
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION,
                    "ICU",
                    DatabaseConfig.COLLATION_ICU_LOCALE,
                    "ja-u-kn-true")));

    // Assert
    assertThat(comparator.textComparator().compare("file2", "file10")).isNegative();
  }

  @Test
  public void from_WhenIcuWithStrengthKeywordLocale_ShouldApplyThatStrength() {
    // Arrange Act
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION,
                    "ICU",
                    DatabaseConfig.COLLATION_ICU_LOCALE,
                    "und-u-ks-level1")));

    // Assert
    assertThat(comparator.textEquals("Alice", "alice")).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"de-u-kn-true", "de-u-ks-level1", "de-u-co-phonebk-kn-true"})
  public void from_WhenIcuWithLocaleSettingAndRules_ShouldThrowIllegalArgumentException(
      String locale) {
    // Arrange
    DatabaseConfig config =
        config(
            props(
                DatabaseConfig.COLLATION, "ICU",
                DatabaseConfig.COLLATION_ICU_LOCALE, locale,
                DatabaseConfig.COLLATION_ICU_RULES, "&a < b"));

    // Act Assert
    assertThatThrownBy(() -> CollationComparator.from(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            CoreError.COLLATION_ICU_LOCALE_SETTING_WITH_RULES_NOT_SUPPORTED.buildCode())
        .hasMessageContaining(locale);
  }

  @ParameterizedTest
  @ValueSource(strings = {"# [strength 1] noted here\n&a < b", "&a < '[strength 1]'"})
  public void from_WhenIcuWithStrengthTextInRuleCommentOrLiteral_ShouldNotApplyThatStrength(
      String rules) {
    // Arrange Act
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(DatabaseConfig.COLLATION, "ICU", DatabaseConfig.COLLATION_ICU_RULES, rules)));

    // Assert: the collator keeps ICU's default tertiary strength.
    assertThat(comparator.textEquals("Alice", "alice")).isFalse();
  }

  @Test
  public void from_WhenIcuWithStrengthOptionInRules_ShouldApplyThatStrength() {
    // Arrange Act
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION,
                    "ICU",
                    DatabaseConfig.COLLATION_ICU_RULES,
                    "[strength 1] &a < b")));

    // Assert
    assertThat(comparator.textEquals("Alice", "alice")).isTrue();
  }

  @Test
  public void from_WhenIcuWithCollationKeywordAndRules_ShouldKeepTheTailoring() {
    // Arrange Act
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_LOCALE, "ja-u-co-unihan",
                    DatabaseConfig.COLLATION_ICU_RULES, "&a < b")));

    // Assert
    assertThat(sign(comparator.textComparator().compare("一", "亜"))).isNegative();
  }

  @Test
  public void from_WhenIcuWithNumericOrderingRuleOption_ShouldOrderDigitRunsNumerically() {
    // Arrange Act
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_LOCALE, "de",
                    DatabaseConfig.COLLATION_ICU_RULES, "[numericOrdering on] &a < b")));

    // Assert
    assertThat(comparator.textComparator().compare("file2", "file10")).isNegative();
  }

  @Test
  public void textComparator_WhenFrozenIcuSharedAcrossThreads_ShouldBeConsistent()
      throws Exception {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_RULES, "[strength 3]")));
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
                DatabaseConfig.COLLATION_ICU_RULES, "[strength 1]")));
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

  @Test
  public void keyComparator_ShouldOrderLexicographicallyAcrossColumns() {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_ICU_RULES, "[strength 1]")));
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
