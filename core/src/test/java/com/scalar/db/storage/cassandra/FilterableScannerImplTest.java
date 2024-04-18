package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Conjunction;
import com.scalar.db.io.IntColumn;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class FilterableScannerImplTest {

  @Mock private Scan scan;
  @Mock private ResultSet resultSet;
  @Mock private ResultInterpreter resultInterpreter;
  @Mock private Row row1, row2, row3;
  @Mock private Result result1, result2, result3;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    when(resultInterpreter.interpret(row1)).thenReturn(result1);
    when(resultInterpreter.interpret(row2)).thenReturn(result2);
    when(resultInterpreter.interpret(row3)).thenReturn(result3);
    when(result1.getColumns()).thenReturn(ImmutableMap.of("col", IntColumn.of("col", 0)));
    when(result2.getColumns()).thenReturn(ImmutableMap.of("col", IntColumn.of("col", 1)));
    when(result3.getColumns()).thenReturn(ImmutableMap.of("col", IntColumn.of("col", 2)));
    when(scan.getConjunctions())
        .thenReturn(
            ImmutableSet.of(Conjunction.of(ConditionBuilder.column("col").isGreaterThanInt(0))));
  }

  @Test
  public void one_ShouldReturnResult() {
    // Arrange
    when(resultSet.one()).thenReturn(row1, row2, row3, null);
    ScannerImpl scanner = new FilterableScannerImpl(scan, resultSet, resultInterpreter);

    // Act
    Optional<Result> actual1 = scanner.one();
    Optional<Result> actual2 = scanner.one();
    Optional<Result> actual3 = scanner.one();

    // Assert
    assertThat(actual1).isPresent();
    assertThat(actual1.get()).isEqualTo(result2);
    assertThat(actual2).isPresent();
    assertThat(actual2.get()).isEqualTo(result3);
    assertThat(actual3).isNotPresent();

    verify(resultInterpreter, times(3)).interpret(any(Row.class));
  }

  @Test
  public void one_AfterExceedingLimit_ShouldReturnEmpty() {
    // Arrange
    when(scan.getLimit()).thenReturn(1);
    when(resultSet.one()).thenReturn(row1, row2, row3, null);
    ScannerImpl scanner = new FilterableScannerImpl(scan, resultSet, resultInterpreter);

    // Act
    Optional<Result> actual1 = scanner.one();
    Optional<Result> actual2 = scanner.one();

    // Assert
    assertThat(actual1).isPresent();
    assertThat(actual1.get()).isEqualTo(result2);
    assertThat(actual2).isNotPresent();

    verify(resultInterpreter, times(2)).interpret(any(Row.class));
  }

  @Test
  public void all_ShouldReturnResults() {
    // Arrange
    Iterator mockIterator = mock(Iterator.class);
    doCallRealMethod().when(resultSet).forEach(any(Consumer.class));
    when(resultSet.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(true, true, true, false);
    when(mockIterator.next()).thenReturn(row1, row2, row3);
    ScannerImpl scanner = new FilterableScannerImpl(scan, resultSet, resultInterpreter);

    // Act
    List<Result> results1 = scanner.all();
    List<Result> results2 = scanner.all();

    // Assert
    assertThat(results1.size()).isEqualTo(2);
    assertThat(results1.get(0)).isEqualTo(result2);
    assertThat(results1.get(1)).isEqualTo(result3);
    assertThat(results2).isEmpty();

    verify(resultInterpreter, times(3)).interpret(any(Row.class));
  }

  @Test
  public void all_WithLimit_ShouldReturnLimitedResults() {
    // Arrange
    when(scan.getLimit()).thenReturn(1);
    Iterator mockIterator = mock(Iterator.class);
    doCallRealMethod().when(resultSet).forEach(any(Consumer.class));
    when(resultSet.iterator()).thenReturn(mockIterator);
    when(mockIterator.hasNext()).thenReturn(true, true, true, false);
    when(mockIterator.next()).thenReturn(row1, row2, row3);
    ScannerImpl scanner = new FilterableScannerImpl(scan, resultSet, resultInterpreter);

    // Act
    List<Result> results1 = scanner.all();
    List<Result> results2 = scanner.all();

    // Assert
    assertThat(results1.size()).isEqualTo(1);
    assertThat(results1.get(0)).isEqualTo(result2);
    assertThat(results2).isEmpty();

    verify(resultInterpreter, times(2)).interpret(any(Row.class));
  }

  @Test
  public void iterator_ShouldReturnResults() {
    // Arrange
    when(resultSet.one()).thenReturn(row1, row2, row3, null);
    ScannerImpl scanner = new FilterableScannerImpl(scan, resultSet, resultInterpreter);

    // Act
    Iterator<Result> iterator = scanner.iterator();

    // Assert
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result2);
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result3);
    assertThat(iterator.hasNext()).isFalse();
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);

    verify(resultInterpreter, times(3)).interpret(any(Row.class));
  }
}
