package com.scalar.db.storage.objectstorage.blobstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.HttpPipelinePosition;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.FixedDelay;
import com.azure.core.http.policy.RetryPolicy;
import com.azure.core.util.Context;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

public class ConditionalRequestAttemptCounterTest {
  private static final String ANY_URL = "http://localhost/container/blob";
  private static final String ANY_ETAG = "\"any_etag\"";

  private HttpResponse response;

  @BeforeEach
  public void setUp() {
    response = mock(HttpResponse.class);
    when(response.getStatusCode()).thenReturn(201);
  }

  private HttpPipeline createPipeline(HttpClient httpClient) {
    return new HttpPipelineBuilder()
        .httpClient(httpClient)
        .policies(
            new RetryPolicy(new FixedDelay(3, Duration.ZERO)),
            new ConditionalRequestAttemptCounter())
        .build();
  }

  @Test
  public void process_IfMatchRequestGiven_ShouldCountAttempt() {
    // Arrange
    HttpPipeline pipeline = createPipeline(request -> Mono.just(response));
    HttpRequest request =
        new HttpRequest(HttpMethod.PUT, ANY_URL).setHeader(HttpHeaderName.IF_MATCH, ANY_ETAG);
    AtomicInteger attempts = new AtomicInteger();

    // Act
    pipeline.send(request, ConditionalRequestAttemptCounter.newContext(attempts)).block();

    // Assert
    assertThat(attempts.get()).isEqualTo(1);
  }

  @Test
  public void process_IfNoneMatchRequestGiven_ShouldCountAttempt() {
    // Arrange
    HttpPipeline pipeline = createPipeline(request -> Mono.just(response));
    HttpRequest request =
        new HttpRequest(HttpMethod.PUT, ANY_URL).setHeader(HttpHeaderName.IF_NONE_MATCH, "*");
    AtomicInteger attempts = new AtomicInteger();

    // Act
    pipeline.send(request, ConditionalRequestAttemptCounter.newContext(attempts)).block();

    // Assert
    assertThat(attempts.get()).isEqualTo(1);
  }

  @Test
  public void process_UnconditionalRequestGiven_ShouldNotCountAttempt() {
    // Arrange
    HttpPipeline pipeline = createPipeline(request -> Mono.just(response));
    HttpRequest request = new HttpRequest(HttpMethod.PUT, ANY_URL);
    AtomicInteger attempts = new AtomicInteger();

    // Act
    pipeline.send(request, ConditionalRequestAttemptCounter.newContext(attempts)).block();

    // Assert
    assertThat(attempts.get()).isEqualTo(0);
  }

  @Test
  public void process_ConditionalRequestResent_ShouldCountEachAttempt() {
    // Arrange
    AtomicInteger sent = new AtomicInteger();
    HttpPipeline pipeline =
        createPipeline(
            request ->
                sent.getAndIncrement() == 0
                    ? Mono.error(new IOException("connection reset"))
                    : Mono.just(response));
    HttpRequest request =
        new HttpRequest(HttpMethod.PUT, ANY_URL).setHeader(HttpHeaderName.IF_MATCH, ANY_ETAG);
    AtomicInteger attempts = new AtomicInteger();

    // Act
    pipeline.send(request, ConditionalRequestAttemptCounter.newContext(attempts)).block();

    // Assert
    assertThat(attempts.get()).isEqualTo(2);
  }

  @Test
  public void processSync_ConditionalRequestResent_ShouldCountEachAttempt() {
    // Arrange
    AtomicInteger sent = new AtomicInteger();
    HttpPipeline pipeline =
        createPipeline(
            request ->
                sent.getAndIncrement() == 0
                    ? Mono.error(new IOException("connection reset"))
                    : Mono.just(response));
    HttpRequest request =
        new HttpRequest(HttpMethod.PUT, ANY_URL).setHeader(HttpHeaderName.IF_MATCH, ANY_ETAG);
    AtomicInteger attempts = new AtomicInteger();

    // Act
    pipeline.sendSync(request, ConditionalRequestAttemptCounter.newContext(attempts));

    // Assert
    assertThat(attempts.get()).isEqualTo(2);
  }

  @Test
  public void process_ContextWithoutCounterGiven_ShouldSendRequest() {
    // Arrange
    HttpPipeline pipeline = createPipeline(request -> Mono.just(response));
    HttpRequest request =
        new HttpRequest(HttpMethod.PUT, ANY_URL).setHeader(HttpHeaderName.IF_MATCH, ANY_ETAG);

    // Act & Assert
    assertThatCode(() -> pipeline.send(request, Context.NONE).block()).doesNotThrowAnyException();
  }

  @Test
  public void getPipelinePosition_ShouldReturnPerRetry() {
    // Arrange
    ConditionalRequestAttemptCounter counter = new ConditionalRequestAttemptCounter();

    // Act
    HttpPipelinePosition position = counter.getPipelinePosition();

    // Assert
    assertThat(position).isEqualTo(HttpPipelinePosition.PER_RETRY);
  }
}
