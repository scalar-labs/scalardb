package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.CollationComparator;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class FilterableScannerTest {

  @Mock private Scan scan;
  @Mock private Scanner scanner;
  @Mock private Result result1, result2, result3;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    when(scanner.one())
        .thenReturn(Optional.of(result1))
        .thenReturn(Optional.of(result2))
        .thenReturn(Optional.of(result3))
        .thenReturn(Optional.empty());
    when(result1.getColumns()).thenReturn(ImmutableMap.of("col", IntColumn.of("col", 0)));
    when(result2.getColumns()).thenReturn(ImmutableMap.of("col", IntColumn.of("col", 1)));
    when(result3.getColumns()).thenReturn(ImmutableMap.of("col", IntColumn.of("col", 2)));
    when(scan.getConjunctions())
        .thenReturn(
            ImmutableSet.of(Conjunction.of(ConditionBuilder.column("col").isGreaterThanInt(0))));
  }

  private static CollationComparator binaryCollation() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, "localhost");
    return CollationComparator.from(new DatabaseConfig(props));
  }

  @Test
  public void one_ShouldReturnResult() throws ExecutionException {
    // Arrange
    FilterableScanner filterableScanner = new FilterableScanner(scan, scanner, binaryCollation());

    // Act
    Optional<Result> actual1 = filterableScanner.one();
    Optional<Result> actual2 = filterableScanner.one();
    Optional<Result> actual3 = filterableScanner.one();

    // Assert
    assertThat(actual1).isPresent();
    assertThat(actual1.get()).isEqualTo(result2);
    assertThat(actual2).isPresent();
    assertThat(actual2.get()).isEqualTo(result3);
    assertThat(actual3).isNotPresent();
    verify(scanner, times(4)).one();
  }

  @Test
  public void one_AfterExceedingLimit_ShouldReturnEmpty() throws ExecutionException {
    // Arrange
    when(scan.getLimit()).thenReturn(1);
    FilterableScanner filterableScanner = new FilterableScanner(scan, scanner, binaryCollation());

    // Act
    Optional<Result> actual1 = filterableScanner.one();
    Optional<Result> actual2 = filterableScanner.one();

    // Assert
    assertThat(actual1).isPresent();
    assertThat(actual1.get()).isEqualTo(result2);
    assertThat(actual2).isNotPresent();
    verify(scanner, times(2)).one();
  }

  @Test
  public void all_ShouldReturnResults() throws ExecutionException {
    // Arrange
    FilterableScanner filterableScanner = new FilterableScanner(scan, scanner, binaryCollation());

    // Act
    List<Result> results1 = filterableScanner.all();
    List<Result> results2 = filterableScanner.all();

    // Assert
    assertThat(results1.size()).isEqualTo(2);
    assertThat(results1.get(0)).isEqualTo(result2);
    assertThat(results1.get(1)).isEqualTo(result3);
    assertThat(results2).isEmpty();
    verify(scanner, times(5)).one();
  }

  @Test
  public void all_WithLimit_ShouldReturnLimitedResults() throws ExecutionException {
    // Arrange
    when(scan.getLimit()).thenReturn(1);
    FilterableScanner filterableScanner = new FilterableScanner(scan, scanner, binaryCollation());

    // Act
    List<Result> results1 = filterableScanner.all();
    List<Result> results2 = filterableScanner.all();

    // Assert
    assertThat(results1.size()).isEqualTo(1);
    assertThat(results1.get(0)).isEqualTo(result2);
    assertThat(results2).isEmpty();
    verify(scanner, times(2)).one();
  }

  @Test
  public void one_WithCollationComparator_ShouldFilterRangeByCollation() throws ExecutionException {
    // Arrange: a case-insensitive ICU PRIMARY collation. The scan filters `col >= 'apple'` on a
    // TEXT column. 'Apple' is excluded by byte order but included by the case-insensitive
    // collation.
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, "localhost");
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");
    props.setProperty(DatabaseConfig.COLLATION, "ICU");
    props.setProperty(DatabaseConfig.COLLATION_ICU_RULES, "[strength 1]");
    CollationComparator comparator = CollationComparator.from(new DatabaseConfig(props));

    Scanner textScanner = mock(Scanner.class);
    Result apple = mock(Result.class);
    Result zebra = mock(Result.class);
    when(textScanner.one())
        .thenReturn(Optional.of(apple))
        .thenReturn(Optional.of(zebra))
        .thenReturn(Optional.empty());
    when(apple.getColumns()).thenReturn(ImmutableMap.of("col", TextColumn.of("col", "Apple")));
    when(zebra.getColumns()).thenReturn(ImmutableMap.of("col", TextColumn.of("col", "zebra")));
    Scan textScan = mock(Scan.class);
    when(textScan.getConjunctions())
        .thenReturn(
            ImmutableSet.of(
                Conjunction.of(
                    ConditionBuilder.column("col").isGreaterThanOrEqualToText("apple"))));

    // Act: with the collation, both rows pass; under BINARY, 'Apple' would be filtered out.
    FilterableScanner withCollation = new FilterableScanner(textScan, textScanner, comparator);

    // Assert
    assertThat(withCollation.all()).containsExactly(apple, zebra);
  }

  @Test
  public void one_WithBinaryCollationComparator_ShouldFilterRangeByByteOrder()
      throws ExecutionException {
    // Arrange: BINARY collation. 'Apple' (0x41) < 'apple' (0x61) so it is excluded; 'zebra'
    // passes.
    Scanner textScanner = mock(Scanner.class);
    Result apple = mock(Result.class);
    Result zebra = mock(Result.class);
    when(textScanner.one())
        .thenReturn(Optional.of(apple))
        .thenReturn(Optional.of(zebra))
        .thenReturn(Optional.empty());
    when(apple.getColumns()).thenReturn(ImmutableMap.of("col", TextColumn.of("col", "Apple")));
    when(zebra.getColumns()).thenReturn(ImmutableMap.of("col", TextColumn.of("col", "zebra")));
    Scan textScan = mock(Scan.class);
    when(textScan.getConjunctions())
        .thenReturn(
            ImmutableSet.of(
                Conjunction.of(
                    ConditionBuilder.column("col").isGreaterThanOrEqualToText("apple"))));

    // Act
    FilterableScanner withBinaryCollation =
        new FilterableScanner(textScan, textScanner, binaryCollation());

    // Assert
    assertThat(withBinaryCollation.all()).containsExactly(zebra);
  }

  @Test
  public void iterator_ShouldReturnResults() throws ExecutionException {
    // Arrange
    FilterableScanner filterableScanner = new FilterableScanner(scan, scanner, binaryCollation());

    // Act
    Iterator<Result> iterator = filterableScanner.iterator();

    // Assert
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result2);
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result3);
    assertThat(iterator.hasNext()).isFalse();
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    verify(scanner, times(5)).one();
  }
}
