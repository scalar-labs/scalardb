package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.cosmos.models.FeedResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Conjunction;
import com.scalar.db.io.IntColumn;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FilterableScannerImplTest {

  @Mock Scan scan;
  @Mock ResultInterpreter resultInterpreter;
  @Mock Record record1;
  @Mock Record record2;
  @Mock Record record3;
  @Mock Record record4;
  @Mock Record record5;
  @Mock Result result1;
  @Mock Result result2;
  @Mock Result result3;
  @Mock Result result4;
  @Mock Result result5;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    Conjunction conjunction = Conjunction.of(ConditionBuilder.column("col").isGreaterThanInt(0));
    when(scan.getConjunctions()).thenReturn(ImmutableSet.of(conjunction));
    when(result1.getColumns()).thenReturn(ImmutableMap.of("col", IntColumn.of("col", 0)));
    when(result2.getColumns()).thenReturn(ImmutableMap.of("col", IntColumn.of("col", 1)));
    when(result3.getColumns()).thenReturn(ImmutableMap.of("col", IntColumn.of("col", 2)));
    when(result4.getColumns()).thenReturn(ImmutableMap.of("col", IntColumn.of("col", 0)));
    when(result5.getColumns()).thenReturn(ImmutableMap.of("col", IntColumn.of("col", 1)));
    when(resultInterpreter.interpret(record1)).thenReturn(result1);
    when(resultInterpreter.interpret(record2)).thenReturn(result2);
    when(resultInterpreter.interpret(record3)).thenReturn(result3);
    when(resultInterpreter.interpret(record4)).thenReturn(result4);
    when(resultInterpreter.interpret(record5)).thenReturn(result5);
  }

  @Test
  public void one_ShouldReturnResult() {
    // Arrange
    ScannerImpl scanner = buildScanner(Lists.newArrayList(record1, record2, record3));

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

    verify(resultInterpreter, times(3)).interpret(any(Record.class));
  }

  @Test
  public void one_AfterExceedingLimit_ShouldReturnEmpty() {
    // Arrange
    when(scan.getLimit()).thenReturn(1);
    ScannerImpl scanner = buildScanner(Lists.newArrayList(record1, record2, record3));

    // Act
    Optional<Result> actual1 = scanner.one();
    Optional<Result> actual2 = scanner.one();

    // Assert
    assertThat(actual1).isPresent();
    assertThat(actual1.get()).isEqualTo(result2);
    assertThat(actual2).isNotPresent();

    verify(resultInterpreter, times(2)).interpret(any(Record.class));
  }

  @Test
  public void all_ShouldReturnResults() {
    // Arrange
    ScannerImpl scanner = buildScanner(Lists.newArrayList(record1, record2, record3));

    // Act
    List<Result> results1 = scanner.all();
    List<Result> results2 = scanner.all();

    // Assert
    assertThat(results1.size()).isEqualTo(2);
    assertThat(results1.get(0)).isEqualTo(result2);
    assertThat(results1.get(1)).isEqualTo(result3);
    assertThat(results2).isEmpty();
  }

  @Test
  public void all_WithLimit_ShouldReturnLimitedResults() {
    // Arrange
    when(scan.getLimit()).thenReturn(1);
    ScannerImpl scanner = buildScanner(Lists.newArrayList(record1, record2, record3));

    // Act
    List<Result> results1 = scanner.all();
    List<Result> results2 = scanner.all();

    // Assert
    assertThat(results1.size()).isEqualTo(1);
    assertThat(results1.get(0)).isEqualTo(result2);
    assertThat(results2).isEmpty();

    verify(resultInterpreter, times(2)).interpret(any(Record.class));
  }

  @Test
  public void all_WithLimit_FoundInFirstPageOfTotalTwoPages_ShouldReturnLimitedResults() {
    // Arrange
    when(scan.getLimit()).thenReturn(1);
    ScannerImpl scanner =
        buildScanner(Lists.newArrayList(record1, record2), Lists.newArrayList(record3, record4));

    // Act
    List<Result> actualResults = scanner.all();

    // Assert
    assertThat(actualResults).containsExactly(result2);
  }

  @Test
  public void all_WithoutLimit_WithTwoPages_ShouldReturnAllResults() {
    // Arrange
    ScannerImpl scanner =
        buildScanner(Lists.newArrayList(record1, record2), Lists.newArrayList(record3, record4));

    // Act
    List<Result> actualResults = scanner.all();

    // Assert
    assertThat(actualResults).containsExactly(result2, result3);
  }

  @Test
  public void one_WithTwoPages_ShouldReturnAllResults() {
    // Arrange
    ScannerImpl scanner =
        buildScanner(Lists.newArrayList(record1, record2), Lists.newArrayList(record3, record4));

    // Act
    Optional<Result> actualResult1 = scanner.one();
    Optional<Result> actualResult2 = scanner.one();
    Optional<Result> actualResult3 = scanner.one();

    // Assert
    assertThat(actualResult1).contains(result2);
    assertThat(actualResult2).contains(result3);
    assertThat(actualResult3).isEmpty();
  }

  @Test
  public void oneAndAll_WithTwoPages_ShouldReturnAllResults() {
    // Arrange
    ScannerImpl scanner =
        buildScanner(
            Lists.newArrayList(record1, record2, record3), Lists.newArrayList(record4, record5));

    // Act
    Optional<Result> oneResult = scanner.one();
    List<Result> remainingResults = scanner.all();
    Optional<Result> emptyResultForOne = scanner.one();
    List<Result> emptyResultForAll = scanner.all();

    // Assert
    assertThat(oneResult).contains(result2);
    assertThat(remainingResults).containsExactly(result3, result5);
    assertThat(emptyResultForOne).isEmpty();
    assertThat(emptyResultForAll).isEmpty();
  }

  @Test
  public void oneAndAll_WithTwoPages_WithLimit_ShouldReturnAllResults() {
    // Arrange
    when(scan.getLimit()).thenReturn(2);
    ScannerImpl scanner =
        buildScanner(
            Lists.newArrayList(record1, record2, record3, record4), Lists.newArrayList(record5));

    // Act
    Optional<Result> oneResult = scanner.one();
    List<Result> remainingResults = scanner.all();
    Optional<Result> emptyResultForOne = scanner.one();
    List<Result> emptyResultForAll = scanner.all();

    // Assert
    assertThat(oneResult).contains(result2);
    assertThat(remainingResults).containsExactly(result3);
    assertThat(emptyResultForOne).isEmpty();
    assertThat(emptyResultForAll).isEmpty();
  }

  @Test
  public void one_WithNoRecord_ShouldReturnEmpty() {
    // Arrange
    ScannerImpl scanner = buildScanner();

    // Act
    Optional<Result> oneResult = scanner.one();

    // Assert
    assertThat(oneResult).isEmpty();
  }

  @Test
  public void all_WithNoRecord_ShouldReturnEmpty() {
    // Arrange
    ScannerImpl scanner = buildScanner();

    // Act
    List<Result> allResults = scanner.all();

    // Assert
    assertThat(allResults).isEmpty();
  }

  @SafeVarargs
  private final ScannerImpl buildScanner(List<Record>... pages) {
    List<FeedResponse<Record>> pagesFeed = new ArrayList<>();
    for (List<Record> page : pages) {
      @SuppressWarnings("unchecked")
      FeedResponse<Record> pageFeed = (FeedResponse<Record>) mock(FeedResponse.class);
      when(pageFeed.getResults()).thenReturn(page);
      pagesFeed.add(pageFeed);
    }

    return new FilterableScannerImpl(scan, pagesFeed.iterator(), resultInterpreter);
  }
}
