package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Result;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

public class QueryScannerTest {

  @Mock private DynamoDbClient client;
  @Mock private QueryRequest request;
  @Mock private ResultInterpreter resultInterpreter;

  @Mock private QueryRequest.Builder builder;
  @Mock private QueryResponse response;
  @Mock private Result result;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(client.query(request)).thenReturn(response);

    when(request.limit()).thenReturn(null);
    when(request.toBuilder()).thenReturn(builder);
    when(builder.build()).thenReturn(request);
  }

  @Test
  public void one_ShouldReturnResult() {
    // Arrange
    Map<String, AttributeValue> item = Collections.emptyMap();
    List<Map<String, AttributeValue>> items = Arrays.asList(item, item, item);
    when(response.items()).thenReturn(items);
    when(resultInterpreter.interpret(item)).thenReturn(result);

    QueryScanner queryScanner = new QueryScanner(client, request, resultInterpreter);

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
  }

  @Test
  public void all_ShouldReturnResults() {
    // Arrange
    Map<String, AttributeValue> item = Collections.emptyMap();
    List<Map<String, AttributeValue>> items = Arrays.asList(item, item, item);
    when(response.items()).thenReturn(items);
    when(resultInterpreter.interpret(item)).thenReturn(result);

    QueryScanner queryScanner = new QueryScanner(client, request, resultInterpreter);

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
  }

  @Test
  public void iterator_ShouldReturnResults() {
    // Arrange
    Map<String, AttributeValue> item = Collections.emptyMap();
    List<Map<String, AttributeValue>> items = Arrays.asList(item, item, item);
    when(response.items()).thenReturn(items);
    when(resultInterpreter.interpret(item)).thenReturn(result);

    QueryScanner queryScanner = new QueryScanner(client, request, resultInterpreter);

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
  }

  @Test
  public void one_ResponseWithLastEvaluatedKey_ShouldReturnResults() {
    // Arrange
    Map<String, AttributeValue> item = Collections.emptyMap();
    List<Map<String, AttributeValue>> items = Arrays.asList(item, item);
    Map<String, AttributeValue> lastEvaluatedKey = Collections.emptyMap();

    when(response.items()).thenReturn(items);
    when(response.hasLastEvaluatedKey()).thenReturn(true).thenReturn(false);
    when(response.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);
    when(resultInterpreter.interpret(item)).thenReturn(result);

    QueryScanner queryScanner = new QueryScanner(client, request, resultInterpreter);

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
    verify(builder).exclusiveStartKey(lastEvaluatedKey);
  }

  @Test
  public void one_RequestWithLimitAndResponseWithLastEvaluatedKey_ShouldReturnResults() {
    // Arrange
    Map<String, AttributeValue> item = Collections.emptyMap();
    List<Map<String, AttributeValue>> items = Arrays.asList(item, item);
    Map<String, AttributeValue> lastEvaluatedKey = Collections.emptyMap();

    when(request.limit()).thenReturn(4);
    when(response.items()).thenReturn(items);
    when(response.hasLastEvaluatedKey()).thenReturn(true);
    when(response.lastEvaluatedKey()).thenReturn(lastEvaluatedKey);
    when(resultInterpreter.interpret(item)).thenReturn(result);

    QueryScanner queryScanner = new QueryScanner(client, request, resultInterpreter);

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
    verify(builder).exclusiveStartKey(lastEvaluatedKey);
  }
}
