package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Conjunction;
import com.scalar.db.io.IntColumn;
import com.scalar.db.storage.dynamo.request.PaginatedRequest;
import com.scalar.db.storage.dynamo.request.PaginatedRequestResponse;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class FilterableQueryScannerTest {

  @Mock PaginatedRequest request;
  @Mock private ResultInterpreter resultInterpreter;
  @Mock private PaginatedRequestResponse response;
  @Mock private Scan scan;
  @Mock private Result result1, result2, result3;
  @Mock private Map<String, AttributeValue> item1, item2, item3;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    item1 = ImmutableMap.of("col", AttributeValue.fromN("1"));
    item2 = ImmutableMap.of("col", AttributeValue.fromN("2"));
    item3 = ImmutableMap.of("col", AttributeValue.fromN("3"));
    when(resultInterpreter.interpret(item1)).thenReturn(result1);
    when(resultInterpreter.interpret(item2)).thenReturn(result2);
    when(resultInterpreter.interpret(item3)).thenReturn(result3);
    when(result1.getColumns()).thenReturn(ImmutableMap.of("col", IntColumn.of("col", 0)));
    when(result2.getColumns()).thenReturn(ImmutableMap.of("col", IntColumn.of("col", 1)));
    when(result3.getColumns()).thenReturn(ImmutableMap.of("col", IntColumn.of("col", 2)));
    when(request.execute()).thenReturn(response);
    when(request.limit()).thenReturn(null);
    when(scan.getConjunctions())
        .thenReturn(
            ImmutableSet.of(Conjunction.of(ConditionBuilder.column("col").isGreaterThanInt(0))));
  }

  @Test
  public void one_ShouldReturnResult() {
    // Arrange
    when(response.items()).thenReturn(Arrays.asList(item1, item2, item3));
    QueryScanner queryScanner = new FilterableQueryScanner(scan, request, resultInterpreter);

    // Act
    Optional<Result> actual1 = queryScanner.one();
    Optional<Result> actual2 = queryScanner.one();
    Optional<Result> actual3 = queryScanner.one();

    // Assert
    assertThat(actual1).isPresent();
    assertThat(actual1.get()).isEqualTo(result2);
    assertThat(actual2).isPresent();
    assertThat(actual2.get()).isEqualTo(result3);
    assertThat(actual3).isNotPresent();

    verify(resultInterpreter, times(3)).interpret(anyMap());
    verify(request).execute();
  }

  @Test
  public void one_AfterExceedingLimit_ShouldReturnEmpty() {
    // Arrange
    when(response.items()).thenReturn(Arrays.asList(item1, item2, item3));
    when(scan.getLimit()).thenReturn(1);
    QueryScanner queryScanner = new FilterableQueryScanner(scan, request, resultInterpreter);

    // Act
    Optional<Result> actual1 = queryScanner.one();
    Optional<Result> actual2 = queryScanner.one();

    // Assert
    assertThat(actual1).isPresent();
    assertThat(actual1.get()).isEqualTo(result2);
    assertThat(actual2).isNotPresent();

    verify(resultInterpreter, times(2)).interpret(anyMap());
  }

  @Test
  public void all_ShouldReturnResults() {
    // Arrange
    when(response.items()).thenReturn(Arrays.asList(item1, item2, item3));
    QueryScanner queryScanner = new FilterableQueryScanner(scan, request, resultInterpreter);

    // Act
    List<Result> results1 = queryScanner.all();
    List<Result> results2 = queryScanner.all();

    // Assert
    assertThat(results1.size()).isEqualTo(2);
    assertThat(results1.get(0)).isEqualTo(result2);
    assertThat(results1.get(1)).isEqualTo(result3);
    assertThat(results2).isEmpty();

    verify(resultInterpreter, times(3)).interpret(anyMap());
    verify(request).execute();
  }

  @Test
  public void all_WithLimit_ShouldReturnLimitedResults() {
    // Arrange
    when(response.items()).thenReturn(Arrays.asList(item1, item2, item3));
    when(scan.getLimit()).thenReturn(1);
    QueryScanner queryScanner = new FilterableQueryScanner(scan, request, resultInterpreter);

    // Act
    List<Result> results1 = queryScanner.all();
    List<Result> results2 = queryScanner.all();

    // Assert
    assertThat(results1.size()).isEqualTo(1);
    assertThat(results1.get(0)).isEqualTo(result2);
    assertThat(results2).isEmpty();

    verify(resultInterpreter, times(2)).interpret(anyMap());
  }

  @Test
  public void iterator_ShouldReturnResults() {
    // Arrange
    when(response.items()).thenReturn(Arrays.asList(item1, item2, item3));
    QueryScanner queryScanner = new FilterableQueryScanner(scan, request, resultInterpreter);

    // Act
    Iterator<Result> iterator = queryScanner.iterator();

    // Assert
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result2);
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result3);
    assertThat(iterator.hasNext()).isFalse();
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);

    verify(resultInterpreter, times(3)).interpret(anyMap());
    verify(request).execute();
  }
}
