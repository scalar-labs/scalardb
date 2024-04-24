package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Conjunction;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.IntColumn;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
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

  @Test
  public void one_ShouldReturnResult() throws ExecutionException {
    // Arrange
    FilterableScanner filterableScanner = new FilterableScanner(scan, scanner);

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
    FilterableScanner filterableScanner = new FilterableScanner(scan, scanner);

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
    FilterableScanner filterableScanner = new FilterableScanner(scan, scanner);

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
    FilterableScanner filterableScanner = new FilterableScanner(scan, scanner);

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
  public void iterator_ShouldReturnResults() throws ExecutionException {
    // Arrange
    FilterableScanner filterableScanner = new FilterableScanner(scan, scanner);

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
