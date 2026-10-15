package com.scalar.db.storage.objectstorage.blobstorage;

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpPipelineNextSyncPolicy;
import com.azure.core.http.HttpPipelinePosition;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.Context;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;
import reactor.core.publisher.Mono;

/**
 * A pipeline policy that counts the attempts of the conditional requests sent in an operation into
 * the counter carried by the {@link Context} of the operation. The policy is invoked for each
 * attempt of a request, including the attempts that the Azure SDK resends after a lost response or
 * a server error. Only requests with a condition are counted, since an operation can also send
 * requests without a condition, such as the requests that stage the blocks of a large upload.
 */
@ThreadSafe
final class ConditionalRequestAttemptCounter implements HttpPipelinePolicy {
  @VisibleForTesting
  static final String CONTEXT_KEY = ConditionalRequestAttemptCounter.class.getName();

  /**
   * Returns a context that makes this policy count the attempts of the conditional requests into
   * the given counter.
   *
   * @param attempts the counter
   * @return the context
   */
  static Context newContext(AtomicInteger attempts) {
    return new Context(CONTEXT_KEY, attempts);
  }

  @Override
  public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
    countAttempt(context);
    return next.process();
  }

  @Override
  public HttpResponse processSync(
      HttpPipelineCallContext context, HttpPipelineNextSyncPolicy next) {
    countAttempt(context);
    return next.processSync();
  }

  @Override
  public HttpPipelinePosition getPipelinePosition() {
    return HttpPipelinePosition.PER_RETRY;
  }

  private static void countAttempt(HttpPipelineCallContext context) {
    HttpHeaders headers = context.getHttpRequest().getHeaders();
    if (headers.getValue(HttpHeaderName.IF_MATCH) == null
        && headers.getValue(HttpHeaderName.IF_NONE_MATCH) == null) {
      return;
    }
    context
        .getData(CONTEXT_KEY)
        .ifPresent(attempts -> ((AtomicInteger) attempts).incrementAndGet());
  }
}
