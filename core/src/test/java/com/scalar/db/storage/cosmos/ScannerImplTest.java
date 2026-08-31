package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.FeedResponse;
import com.scalar.db.api.Result;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ScannerImplTest {

  private static final String ANY_SHORT_MESSAGE = "[\"Request rate is large\"]";
  // getMessage() of CosmosException embeds the whole CosmosDiagnostics, which makes the message too
  // large for callers that impose a size limit on error messages. It must not appear in the built
  // message.
  private static final String ANY_MESSAGE_WITH_DIAGNOSTICS = "diagnostics-must-not-appear";

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
  public void one_WithSingleRecord_ShouldContainOnlyOneResult() throws Exception {
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
  public void all_WithSingleRecord_ShouldContainOnlyOneResult() throws Exception {
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
  public void all_WithTwoPages_ShouldReturnAllResults() throws Exception {
    // Arrange
    ScannerImpl scanner =
        buildScanner(Lists.newArrayList(record1, record2), Lists.newArrayList(record3, record4));

    // Act
    List<Result> actualResults = scanner.all();

    // Assert
    assertThat(actualResults).containsExactly(result1, result2, result3, result4);
  }

  @Test
  public void one_WithTwoPages_ShouldReturnAllResults() throws Exception {
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
  public void oneAndAll_WithTwoPages_ShouldReturnAllResults() throws Exception {
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
  public void one_WithNoRecord_ShouldReturnEmpty() throws Exception {
    // Arrange
    ScannerImpl scanner = buildScanner();

    // Act
    Optional<Result> oneResult = scanner.one();

    // Assert
    assertThat(oneResult).isEmpty();
  }

  @Test
  public void all_WithNoRecord_ShouldReturnEmpty() throws Exception {
    // Arrange
    ScannerImpl scanner = buildScanner();

    // Act
    List<Result> allResults = scanner.all();

    // Assert
    assertThat(allResults).isEmpty();
  }

  @Test
  public void
      one_WhenFetchingNextPageThrowsCosmosException_ShouldThrowExecutionExceptionWithShortMessage() {
    // Arrange
    // The pages are fetched lazily, so a CosmosException can be thrown while advancing to the next
    // page.
    ScannerImpl scanner = buildScannerThrowingOnSecondPage(Lists.newArrayList(record1));

    // Act Assert
    assertThatCode(scanner::one).doesNotThrowAnyException();
    assertThatThrownBy(scanner::one)
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(CosmosException.class)
        .hasMessageContaining("statusCode=429")
        .hasMessageContaining("subStatusCode=3200")
        .hasMessageContaining(ANY_SHORT_MESSAGE)
        .hasMessageNotContaining(ANY_MESSAGE_WITH_DIAGNOSTICS);
  }

  @Test
  public void
      all_WhenFetchingNextPageThrowsCosmosException_ShouldThrowExecutionExceptionWithShortMessage() {
    // Arrange
    ScannerImpl scanner = buildScannerThrowingOnSecondPage(Lists.newArrayList(record1));

    // Act Assert
    assertThatThrownBy(scanner::all)
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(CosmosException.class)
        .hasMessageContaining("statusCode=429")
        .hasMessageContaining("subStatusCode=3200")
        .hasMessageContaining(ANY_SHORT_MESSAGE)
        .hasMessageNotContaining(ANY_MESSAGE_WITH_DIAGNOSTICS);
  }

  private ScannerImpl buildScannerThrowingOnSecondPage(List<Record> firstPage) {
    CosmosException toThrow = mock(CosmosException.class);
    when(toThrow.getStatusCode()).thenReturn(429);
    when(toThrow.getSubStatusCode()).thenReturn(3200);
    when(toThrow.getShortMessage()).thenReturn(ANY_SHORT_MESSAGE);
    when(toThrow.getMessage()).thenReturn(ANY_MESSAGE_WITH_DIAGNOSTICS);

    @SuppressWarnings("unchecked")
    FeedResponse<Record> firstPageFeed = (FeedResponse<Record>) mock(FeedResponse.class);
    when(firstPageFeed.getResults()).thenReturn(firstPage);

    // A real iterator is used here instead of a mock since all() relies on the default
    // implementation of Iterator.forEachRemaining(), which a mocked Iterator does not run.
    Iterator<FeedResponse<Record>> pagesIterator =
        new Iterator<FeedResponse<Record>>() {
          private int position;

          @Override
          public boolean hasNext() {
            return position < 2;
          }

          @Override
          public FeedResponse<Record> next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            if (position++ == 0) {
              return firstPageFeed;
            }
            // Fetching the second page fails with a CosmosException
            throw toThrow;
          }
        };

    return new ScannerImpl(pagesIterator, resultInterpreter);
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
