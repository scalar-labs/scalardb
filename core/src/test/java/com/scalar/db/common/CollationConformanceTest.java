package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Comparator-level ordering conformance.
 *
 * <p>Checks that the orderings a {@link CollationComparator} exposes agree with each other:
 *
 * <ul>
 *   <li>{@link CollationComparator#textComparator()} on text values;
 *   <li>{@link CollationComparator#columnComparator()} on TEXT columns;
 *   <li>{@link CollationComparator#keyComparator()} on single-text-column keys;
 *   <li>the range decisions of {@link ScalarDbUtils#columnsMatchAnyOfConjunctions} given the same
 *       comparator.
 * </ul>
 *
 * <p>The test calls these directly and does not exercise the storage or transaction code that uses
 * them. It covers a shared text corpus (including nulls, mixed text/non-text keys, and
 * supplementary-plane characters) for {@code BINARY} and ICU, and that an unset collation defaults
 * to the {@code BINARY} (UTF-8 byte order) collation.
 */
public class CollationConformanceTest {

  // The last two entries are U+10000, the first supplementary-plane code point, and U+FFFF, the
  // last BMP one: UTF-16 and UTF-8 disagree on their relative order.
  private static final String[] CORPUS = {
    "apple", "Apple", "APPLE", "banana", "Banana", "", "á", "z", "𐀀", "￿"
  };

  private static DatabaseConfig config(Properties props) {
    return new DatabaseConfig(props);
  }

  private static CollationComparator binary() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.COLLATION, "BINARY");
    return CollationComparator.from(config(props));
  }

  private static CollationComparator icu(String strengthOption) {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.COLLATION, "ICU");
    props.setProperty(DatabaseConfig.COLLATION_ICU_LOCALE, "en-US");
    props.setProperty(DatabaseConfig.COLLATION_ICU_RULES, strengthOption);
    return CollationComparator.from(config(props));
  }

  @ParameterizedTest
  @ValueSource(strings = {"BINARY", "ICU_PRIMARY", "ICU_TERTIARY"})
  void textColumnKeyComparatorsAndRangeFilter_OrderSharedTextCorpusIdentically(String mode) {
    CollationComparator comparator = comparatorFor(mode);

    Comparator<String> textCmp = comparator.textComparator();
    Comparator<Column<?>> columnCmp = comparator.columnComparator();
    Comparator<Key> keyCmp = comparator.keyComparator();

    for (String a : CORPUS) {
      for (String b : CORPUS) {
        int text = sign(textCmp.compare(a, b));

        // Per-column comparator on TEXT columns.
        int column = sign(columnCmp.compare(TextColumn.of("col", a), TextColumn.of("col", b)));

        // Key comparator on single-text-column keys.
        int key = sign(keyCmp.compare(Key.ofText("col", a), Key.ofText("col", b)));

        assertThat(column)
            .as("column vs text ordering for (%s, %s) under %s", a, b, mode)
            .isEqualTo(text);
        assertThat(key)
            .as("key vs text ordering for (%s, %s) under %s", a, b, mode)
            .isEqualTo(text);

        // ScalarDbUtils range filter: a `>= b` range decision must agree with the ordering, i.e.
        // `a` matches `col >= b` iff columnComparator(a, b) >= 0.
        boolean matchesGte = filterMatchesGte(comparator, a, b);
        assertThat(matchesGte)
            .as("ScalarDbUtils `>= %s` decision for %s under %s", b, a, mode)
            .isEqualTo(column >= 0);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"BINARY", "ICU_PRIMARY", "ICU_TERTIARY"})
  void keyComparator_OrdersMixedTextAndNonTextKeys_ByNonTextFirstThenCollatedText(String mode) {
    CollationComparator comparator = comparatorFor(mode);
    Comparator<Key> keyCmp = comparator.keyComparator();
    Comparator<String> textCmp = comparator.textComparator();

    Key k1 = Key.newBuilder().addInt("i", 1).addText("t", "Apple").build();
    Key k2 = Key.newBuilder().addInt("i", 2).addText("t", "apple").build();
    Key k3 = Key.newBuilder().addInt("i", 1).addText("t", "banana").build();

    assertThat(sign(keyCmp.compare(k1, k2))).isEqualTo(-1);
    assertThat(sign(keyCmp.compare(k1, k3))).isEqualTo(sign(textCmp.compare("Apple", "banana")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"BINARY", "ICU_PRIMARY", "ICU_TERTIARY"})
  void columnAndKeyComparators_OrderNullTextConsistently_NullFirst(String mode) {
    CollationComparator comparator = comparatorFor(mode);
    Comparator<Column<?>> columnCmp = comparator.columnComparator();
    Comparator<Key> keyCmp = comparator.keyComparator();

    Column<?> nullCol = TextColumn.ofNull("col");
    Column<?> valueCol = TextColumn.of("col", "apple");

    assertThat(sign(columnCmp.compare(nullCol, valueCol))).isEqualTo(-1);
    assertThat(sign(columnCmp.compare(valueCol, nullCol))).isEqualTo(1);
    assertThat(sign(columnCmp.compare(nullCol, nullCol))).isEqualTo(0);

    Key nullKey = Key.ofText("col", null);
    Key valueKey = Key.ofText("col", "apple");
    assertThat(sign(keyCmp.compare(nullKey, valueKey))).isEqualTo(-1);
    assertThat(sign(keyCmp.compare(valueKey, nullKey))).isEqualTo(1);
  }

  @Test
  void unsetCollation_DefaultsToBinaryOrder() {
    CollationComparator unset = CollationComparator.from(config(new Properties()));
    assertThat(unset).isNotNull();

    Comparator<String> unsetCmp = unset.textComparator();
    Comparator<String> binaryCmp = binary().textComparator();
    for (String a : CORPUS) {
      for (String b : CORPUS) {
        assertThat(sign(unsetCmp.compare(a, b)))
            .as("unset vs explicit BINARY order for (%s, %s)", a, b)
            .isEqualTo(sign(binaryCmp.compare(a, b)));
      }
    }
    assertThat(unset.textEquals("Apple", "apple")).isFalse();
    assertThat(unset.textEquals("apple", "apple")).isTrue();
  }

  private static CollationComparator comparatorFor(String mode) {
    switch (mode) {
      case "BINARY":
        return binary();
      case "ICU_PRIMARY":
        return icu("[strength 1]");
      case "ICU_TERTIARY":
        return icu("[strength 3]");
      default:
        throw new AssertionError("Unknown mode: " + mode);
    }
  }

  private static boolean filterMatchesGte(
      CollationComparator comparator, String value, String bound) {
    Map<String, Column<?>> columns = new HashMap<>();
    columns.put("col", TextColumn.of("col", value));
    Set<Conjunction> conjunctions =
        ImmutableSet.of(
            Conjunction.of(ConditionBuilder.column("col").isGreaterThanOrEqualToText(bound)));
    return ScalarDbUtils.columnsMatchAnyOfConjunctions(columns, conjunctions, comparator);
  }

  private static int sign(int value) {
    return Integer.compare(value, 0);
  }
}
