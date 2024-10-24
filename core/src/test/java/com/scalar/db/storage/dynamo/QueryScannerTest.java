package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Result;
import com.scalar.db.storage.dynamo.request.PaginatedRequest;
import com.scalar.db.storage.dynamo.request.PaginatedRequestResponse;
import java.util.Arrays;
import java.util.Collections;
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

public class QueryScannerTest {

  @Mock PaginatedRequest request;
  @Mock private ResultInterpreter resultInterpreter;
  @Mock private PaginatedRequestResponse response;
  @Mock private Result result;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void one_ShouldReturnResult() {
    // Arrange
    Map<String, AttributeValue> item = Collections.emptyMap();
    List<Map<String, AttributeValue>> items = Arrays.asList(item, item, item);
    when(request.execute()).thenReturn(response);
    when(response.items()).thenReturn(items);
    when(resultInterpreter.interpret(item)).thenReturn(result);

    QueryScanner queryScanner = new QueryScanner(request, 0, resultInterpreter);

    // Act
    Optional<Result> actual1 = queryScanner.one();
    Optional<Result> actual2 = queryScanner.one();
    Optional<Result> actual3 = queryScanner.one();
    Optional<Result> actual4 = queryScanner.one();

    // Assert
    assertThat(actual1).isPresent();
    assertThat(actual1.get()).isEqualTo(result);
    assertThat(actual2).isPresent();
    assertThat(actual2.get()).isEqualTo(result);
    assertThat(actual3).isPresent();
    assertThat(actual3.get()).isEqualTo(result);
    assertThat(actual4).isNotPresent();

    verify(resultInterpreter, times(3)).interpret(item);
    verify(request).execute();
  }

  @Test
  public void all_ShouldReturnResults() {
    // Arrange
    Map<String, AttributeValue> item = Collections.emptyMap();
    List<Map<String, AttributeValue>> items = Arrays.asList(item, item, item);
    when(request.execute()).thenReturn(response);
    when(response.items()).thenReturn(items);
    when(resultInterpreter.interpret(item)).thenReturn(result);

    QueryScanner queryScanner = new QueryScanner(request, 0, resultInterpreter);

    // Act
    List<Result> results1 = queryScanner.all();
    List<Result> results2 = queryScanner.all();

    // Assert
    assertThat(results1.size()).isEqualTo(3);
    assertThat(results1.get(0)).isEqualTo(result);
    assertThat(results1.get(1)).isEqualTo(result);
    assertThat(results1.get(2)).isEqualTo(result);
    assertThat(results2).isEmpty();

    verify(resultInterpreter, times(3)).interpret(item);
    verify(request).execute();
  }

  @Test
  public void iterator_ShouldReturnResults() {
    // Arrange
    Map<String, AttributeValue> item = Collections.emptyMap();
    List<Map<String, AttributeValue>> items = Arrays.asList(item, item, item);
    when(response.items()).thenReturn(items);
    when(resultInterpreter.interpret(item)).thenReturn(result);
    when(request.execute()).thenReturn(response);

    QueryScanner queryScanner = new QueryScanner(request, 0, resultInterpreter);

    // Act
    Iterator<Result> iterator = queryScanner.iterator();

    // Assert
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result);
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result);
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(result);
    assertThat(iterator.hasNext()).isFalse();
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);

    verify(resultInterpreter, times(3)).interpret(item);
    verify(request).execute();
  }

  @Test
  public void one_ResponseWithLastEvaluatedKey_ShouldReturnResults() {
    // Arrange
    Map<String, AttributeValue> item = Collections.emptyMap();
    List<Map<String, AttributeValue>> items = Arrays.asList(item, item);
    Map<String, AttributeValue> lastEvaluatedKey = Collections.emptyMap();

    when(response.items()).thenReturn(items).thenReturn(items);
    when(response.hasLastEvaluatedKey()).thenReturn(true).thenReturn(false);
    when(response.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);
    when(resultInterpreter.interpret(item)).thenReturn(result);
    when(request.execute()).thenReturn(response);
    when(request.execute(lastEvaluatedKey)).thenReturn(response);

    QueryScanner queryScanner = new QueryScanner(request, 0, resultInterpreter);

    // Act
    Optional<Result> actual1 = queryScanner.one();
    Optional<Result> actual2 = queryScanner.one();
    Optional<Result> actual3 = queryScanner.one();
    Optional<Result> actual4 = queryScanner.one();
    Optional<Result> actual5 = queryScanner.one();

    // Assert
    assertThat(actual1).isPresent();
    assertThat(actual1.get()).isEqualTo(result);
    assertThat(actual2).isPresent();
    assertThat(actual2.get()).isEqualTo(result);
    assertThat(actual3).isPresent();
    assertThat(actual3.get()).isEqualTo(result);
    assertThat(actual4).isPresent();
    assertThat(actual4.get()).isEqualTo(result);
    assertThat(actual5).isNotPresent();

    verify(resultInterpreter, times(4)).interpret(item);
    verify(request).execute(lastEvaluatedKey);
    verify(request).execute();
  }

  @Test
  public void one_RequestWithLimitAndResponseWithLastEvaluatedKey_ShouldReturnResults() {
    // Arrange
    int limit = 3;

    Map<String, AttributeValue> item = Collections.emptyMap();
    List<Map<String, AttributeValue>> items1 = Arrays.asList(item, item);
    List<Map<String, AttributeValue>> items2 = Collections.singletonList(item);
    Map<String, AttributeValue> lastEvaluatedKey = Collections.emptyMap();

    when(response.items()).thenReturn(items1).thenReturn(items2);
    when(response.hasLastEvaluatedKey()).thenReturn(true);
    when(response.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);
    when(request.execute(limit)).thenReturn(response);
    when(request.execute(lastEvaluatedKey, limit - items1.size())).thenReturn(response);
    when(resultInterpreter.interpret(item)).thenReturn(result);

    QueryScanner queryScanner = new QueryScanner(request, limit, resultInterpreter);

    // Act
    Optional<Result> actual1 = queryScanner.one();
    Optional<Result> actual2 = queryScanner.one();
    Optional<Result> actual3 = queryScanner.one();
    Optional<Result> actual4 = queryScanner.one();

    // Assert
    assertThat(actual1).isPresent();
    assertThat(actual1.get()).isEqualTo(result);
    assertThat(actual2).isPresent();
    assertThat(actual2.get()).isEqualTo(result);
    assertThat(actual3).isPresent();
    assertThat(actual3.get()).isEqualTo(result);
    assertThat(actual4).isNotPresent();

    verify(resultInterpreter, times(limit)).interpret(item);
    verify(request).execute(limit);
    verify(request).execute(lastEvaluatedKey, limit - items1.size());
  }
}
