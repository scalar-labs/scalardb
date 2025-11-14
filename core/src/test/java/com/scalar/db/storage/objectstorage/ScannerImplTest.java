package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Result;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ScannerImplTest {

  @Mock ResultInterpreter resultInterpreter;
  @Mock ObjectStorageRecord record1;
  @Mock ObjectStorageRecord record2;
  @Mock ObjectStorageRecord record3;
  @Mock ObjectStorageRecord record4;
  @Mock Result result1;
  @Mock Result result2;
  @Mock Result result3;
  @Mock Result result4;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    when(resultInterpreter.interpret(record1)).thenReturn(result1);
    when(resultInterpreter.interpret(record2)).thenReturn(result2);
    when(resultInterpreter.interpret(record3)).thenReturn(result3);
    when(resultInterpreter.interpret(record4)).thenReturn(result4);
  }

  @Test
  public void one_WithSingleRecord_ShouldContainOnlyOneResult() {
    // Arrange
    ScannerImpl scanner = buildScanner(Collections.singletonList(record1));

    // Act
    Optional<Result> actualResult1 = scanner.one();
    Optional<Result> emptyResult = scanner.one();

    // Assert
    assertThat(actualResult1).contains(result1);
    assertThat(emptyResult).isEmpty();
  }

  @Test
  public void all_WithSingleRecord_ShouldContainOnlyOneResult() {
    // Arrange
    ScannerImpl scanner = buildScanner(Collections.singletonList(record1));

    // Act
    List<Result> actualResults = scanner.all();
    List<Result> emptyResults = scanner.all();

    // Assert
    assertThat(actualResults).containsExactly(result1);
    assertThat(emptyResults).isEmpty();
  }

  @Test
  public void all_WithMultipleRecords_ShouldReturnAllResults() {
    // Arrange
    ScannerImpl scanner = buildScanner(Arrays.asList(record1, record2, record3, record4));

    // Act
    List<Result> actualResults = scanner.all();

    // Assert
    assertThat(actualResults).containsExactly(result1, result2, result3, result4);
  }

  @Test
  public void one_WithMultipleRecords_ShouldReturnAllResults() {
    // Arrange
    ScannerImpl scanner = buildScanner(Arrays.asList(record1, record2, record3, record4));

    // Act
    Optional<Result> actualResult1 = scanner.one();
    Optional<Result> actualResult2 = scanner.one();
    Optional<Result> actualResult3 = scanner.one();
    Optional<Result> actualResult4 = scanner.one();
    Optional<Result> actualResult5 = scanner.one();

    // Assert
    assertThat(actualResult1).contains(result1);
    assertThat(actualResult2).contains(result2);
    assertThat(actualResult3).contains(result3);
    assertThat(actualResult4).contains(result4);
    assertThat(actualResult5).isEmpty();
  }

  @Test
  public void oneAndAll_WithMultipleRecords_ShouldReturnAllResults() {
    // Arrange
    ScannerImpl scanner = buildScanner(Arrays.asList(record1, record2, record3, record4));

    // Act
    Optional<Result> oneResult = scanner.one();
    List<Result> remainingResults = scanner.all();
    Optional<Result> emptyResultForOne = scanner.one();
    List<Result> emptyResultForAll = scanner.all();

    // Assert
    assertThat(oneResult).contains(result1);
    assertThat(remainingResults).containsExactly(result2, result3, result4);
    assertThat(emptyResultForOne).isEmpty();
    assertThat(emptyResultForAll).isEmpty();
  }

  @Test
  public void one_WithNoRecord_ShouldReturnEmpty() {
    // Arrange
    ScannerImpl scanner = buildScanner(Collections.emptyList());

    // Act
    Optional<Result> oneResult = scanner.one();

    // Assert
    assertThat(oneResult).isEmpty();
  }

  @Test
  public void all_WithNoRecord_ShouldReturnEmpty() {
    // Arrange
    ScannerImpl scanner = buildScanner(Collections.emptyList());

    // Act
    List<Result> allResults = scanner.all();

    // Assert
    assertThat(allResults).isEmpty();
  }

  @Test
  public void one_WithRecordCountLimit_ShouldReturnLimitedResults() {
    // Arrange
    ScannerImpl scanner =
        buildScannerWithLimit(Arrays.asList(record1, record2, record3, record4), 2);

    // Act
    Optional<Result> actualResult1 = scanner.one();
    Optional<Result> actualResult2 = scanner.one();
    Optional<Result> actualResult3 = scanner.one();

    // Assert
    assertThat(actualResult1).contains(result1);
    assertThat(actualResult2).contains(result2);
    assertThat(actualResult3).isEmpty();
  }

  @Test
  public void all_WithRecordCountLimit_ShouldReturnLimitedResults() {
    // Arrange
    ScannerImpl scanner =
        buildScannerWithLimit(Arrays.asList(record1, record2, record3, record4), 2);

    // Act
    List<Result> actualResults = scanner.all();

    // Assert
    assertThat(actualResults).containsExactly(result1, result2);
  }

  @Test
  public void oneAndAll_WithRecordCountLimit_ShouldReturnLimitedResults() {
    // Arrange
    ScannerImpl scanner =
        buildScannerWithLimit(Arrays.asList(record1, record2, record3, record4), 3);

    // Act
    Optional<Result> oneResult = scanner.one();
    List<Result> remainingResults = scanner.all();

    // Assert
    assertThat(oneResult).contains(result1);
    assertThat(remainingResults).containsExactly(result2, result3);
  }

  @Test
  public void all_WithZeroRecordCountLimit_ShouldReturnAllResults() {
    // Arrange
    ScannerImpl scanner = buildScannerWithLimit(Arrays.asList(record1, record2, record3), 0);

    // Act
    List<Result> actualResults = scanner.all();

    // Assert
    assertThat(actualResults).containsExactly(result1, result2, result3);
  }

  private ScannerImpl buildScanner(List<ObjectStorageRecord> records) {
    return buildScannerWithLimit(records, 0);
  }

  private ScannerImpl buildScannerWithLimit(List<ObjectStorageRecord> records, int limit) {
    List<ObjectStorageRecord> recordList = new ArrayList<>(records);
    Iterator<ObjectStorageRecord> iterator = recordList.iterator();
    return new ScannerImpl(iterator, resultInterpreter, limit);
  }
}
