package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.cosmos.models.FeedResponse;
import com.scalar.db.api.Result;
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
public class ScannerImplTest {

  @Mock ResultInterpreter resultInterpreter;
  @Mock Record record1;
  @Mock Record record2;
  @Mock Record record3;
  @Mock Record record4;
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
    ScannerImpl scanner = buildScanner(Lists.newArrayList(record1));

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
    ScannerImpl scanner = buildScanner(Lists.newArrayList(record1));

    // Act
    List<Result> actualResults = scanner.all();
    List<Result> emptyResults = scanner.all();

    // Assert
    assertThat(actualResults).containsExactly(result1);
    assertThat(emptyResults).isEmpty();
  }

  @Test
  public void all_WithTwoPages_ShouldReturnAllResults() {
    // Arrange
    ScannerImpl scanner =
        buildScanner(Lists.newArrayList(record1, record2), Lists.newArrayList(record3, record4));

    // Act
    List<Result> actualResults = scanner.all();

    // Assert
    assertThat(actualResults).containsExactly(result1, result2, result3, result4);
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
  public void oneAndAll_WithTwoPages_ShouldReturnAllResults() {
    // Arrange
    ScannerImpl scanner =
        buildScanner(Lists.newArrayList(record1, record2), Lists.newArrayList(record3, record4));

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

    return new ScannerImpl(pagesFeed.iterator(), resultInterpreter);
  }
}
