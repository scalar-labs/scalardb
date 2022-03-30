package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Result;
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
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

public class GetItemScannerTest {

  @Mock private DynamoDbClient client;
  @Mock private GetItemRequest request;
  @Mock private ResultInterpreter resultInterpreter;

  @Mock private GetItemResponse response;
  @Mock private Result result;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(client.getItem(request)).thenReturn(response);
  }

  @Test
  public void one_WhenItemReturned_ShouldReturnResult() {
    // Arrange
    Map<String, AttributeValue> item = Collections.emptyMap();

    when(response.hasItem()).thenReturn(true);
    when(response.item()).thenReturn(item);
    when(resultInterpreter.interpret(item)).thenReturn(result);

    GetItemScanner getItemScanner = new GetItemScanner(client, request, resultInterpreter);

    // Act
    Optional<Result> actual1 = getItemScanner.one();
    Optional<Result> actual2 = getItemScanner.one();

    // Assert
    assertThat(actual1).isPresent();
    assertThat(actual1.get()).isEqualTo(result);

    assertThat(actual2).isNotPresent();
  }

  @Test
  public void one_WhenNoItemReturned_ShouldReturnEmpty() {
    // Arrange
    when(response.hasItem()).thenReturn(false);

    GetItemScanner getItemScanner = new GetItemScanner(client, request, resultInterpreter);

    // Act
    Optional<Result> actual = getItemScanner.one();

    // Assert
    assertThat(actual).isNotPresent();
  }

  @Test
  public void all_WhenItemReturned_ShouldReturnResult() {
    // Arrange
    Map<String, AttributeValue> item = Collections.emptyMap();

    when(response.hasItem()).thenReturn(true);
    when(response.item()).thenReturn(item);
    when(resultInterpreter.interpret(item)).thenReturn(result);

    GetItemScanner getItemScanner = new GetItemScanner(client, request, resultInterpreter);

    // Act
    List<Result> results1 = getItemScanner.all();
    List<Result> results2 = getItemScanner.all();

    // Assert
    assertThat(results1.size()).isEqualTo(1);
    assertThat(results1.get(0)).isEqualTo(result);

    assertThat(results2).isEmpty();
  }

  @Test
  public void all_WhenNoItemReturned_ShouldReturnEmpty() {
    // Arrange
    when(response.hasItem()).thenReturn(false);

    GetItemScanner getItemScanner = new GetItemScanner(client, request, resultInterpreter);

    // Act
    List<Result> results = getItemScanner.all();

    // Assert
    assertThat(results).isEmpty();
  }

  @Test
  public void iterator_WhenItemReturned_ShouldReturnResult() {
    // Arrange
    Map<String, AttributeValue> item = Collections.emptyMap();

    when(response.hasItem()).thenReturn(true);
    when(response.item()).thenReturn(item);
    when(resultInterpreter.interpret(item)).thenReturn(result);

    GetItemScanner getItemScanner = new GetItemScanner(client, request, resultInterpreter);

    // Act
    Iterator<Result> iterator = getItemScanner.iterator();

    // Assert
    assertThat(iterator.hasNext()).isTrue();
    Result actual1 = iterator.next();
    assertThat(actual1).isEqualTo(result);
    assertThat(iterator.hasNext()).isFalse();
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void iterator_WhenNoItemReturned_ShouldReturnEmpty() {
    // Arrange
    when(response.hasItem()).thenReturn(false);

    GetItemScanner getItemScanner = new GetItemScanner(client, request, resultInterpreter);

    // Act
    Iterator<Result> iterator = getItemScanner.iterator();

    // Assert
    assertThat(iterator.hasNext()).isFalse();
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
  }
}
