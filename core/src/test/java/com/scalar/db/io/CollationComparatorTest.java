package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
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

  // ---- Unset ----

  @Test
  public void from_WhenCollationUnset_ShouldReturnEmpty() {
    // Arrange
    DatabaseConfig config = config(new Properties());

    // Act
    Optional<CollationComparator> comparator = CollationComparator.from(config);

    // Assert
    assertThat(comparator).isEmpty();
  }

  // ---- BINARY happy path ----

  @Test
  public void textComparator_WhenBinary_ShouldOrderAsciiByByteValue() {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(config(props(DatabaseConfig.COLLATION, "BINARY"))).get();
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
                        DatabaseConfig.COLLATION_STRENGTH, "PRIMARY")))
            .get();
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
                        DatabaseConfig.COLLATION_STRENGTH, "TERTIARY")))
            .get();
    Comparator<String> textComparator = comparator.textComparator();

    // Act Assert
    assertThat(textComparator.compare("a", "A")).isNotZero();
  }

  // ---- Supplementary plane divergence (BINARY vs Java natural order) ----

  @Test
  public void textComparator_WhenBinary_ShouldDivergeFromJavaNaturalOrderAboveBmp() {
    // Arrange
    CollationComparator comparator =
        CollationComparator.from(config(props(DatabaseConfig.COLLATION, "BINARY"))).get();
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
                        DatabaseConfig.COLLATION_RULES, "& b < a")))
            .get();
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
                DatabaseConfig.COLLATION_RULES, "this is not a valid rule <<< &&&"));

    // Act Assert
    assertThatThrownBy(() -> CollationComparator.from(config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---- Locale ----

  @Test
  public void from_WhenIcuWithLocale_ShouldBuildComparator() {
    // Arrange Act
    Optional<CollationComparator> comparator =
        CollationComparator.from(
            config(
                props(
                    DatabaseConfig.COLLATION, "ICU",
                    DatabaseConfig.COLLATION_LOCALE, "de")));

    // Assert
    assertThat(comparator).isPresent();
    assertThat(comparator.get().textComparator().compare("a", "b")).isNegative();
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
                        DatabaseConfig.COLLATION_STRENGTH, "TERTIARY")))
            .get();
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
                DatabaseConfig.COLLATION_STRENGTH, "PRIMARY")));
  }

  private void assertConsistency(DatabaseConfig config) {
    CollationComparator comparator = CollationComparator.from(config).get();
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
        CollationComparator.from(config(props(DatabaseConfig.COLLATION, "BINARY"))).get();
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
        CollationComparator.from(config(props(DatabaseConfig.COLLATION, "BINARY"))).get();
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
                        DatabaseConfig.COLLATION_STRENGTH, "PRIMARY")))
            .get();
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
}
