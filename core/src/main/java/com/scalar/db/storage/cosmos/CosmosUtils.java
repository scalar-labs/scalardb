package com.scalar.db.storage.cosmos;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.common.CoreError;
import java.util.Locale;

public final class CosmosUtils {

  private CosmosUtils() {}

  public static String quoteKeyword(String keyword) {
    return "[\"" + keyword + "\"]";
  }

  /**
   * Builds the error details of the specified {@code CosmosException} for an exception message.
   *
   * <p>This method uses {@link CosmosException#getShortMessage()} instead of {@link
   * CosmosException#getMessage()} on purpose. {@code getMessage()} embeds the whole {@code
   * CosmosDiagnostics}, which can grow to tens of kilobytes; a message of 35,955 bytes has been
   * observed for a throttling error. Callers that carry error messages across a boundary generally
   * impose size limits far below that, so an oversized message can be truncated or dropped
   * entirely, losing all the error information. The diagnostics are still available through the
   * {@code CosmosException} attached as the cause.
   *
   * @param exception a {@code CosmosException} to build the error details of
   * @return the built error details
   */
  public static String buildErrorDetails(CosmosException exception) {
    return "statusCode="
        + exception.getStatusCode()
        + ", subStatusCode="
        + exception.getSubStatusCode()
        + ", message="
        + exception.getShortMessage();
  }

  public static CosmosClient buildCosmosClient(CosmosConfig config) {
    return new CosmosClientBuilder()
        .endpoint(config.getEndpoint())
        .key(config.getKey())
        .directMode()
        .consistencyLevel(getConsistencyLevel(config))
        .buildClient();
  }

  @VisibleForTesting
  static ConsistencyLevel getConsistencyLevel(CosmosConfig config) {
    ConsistencyLevel consistencyLevel =
        config
            .getConsistencyLevel()
            .map(c -> ConsistencyLevel.valueOf(c.toUpperCase(Locale.ROOT)))
            .orElse(ConsistencyLevel.STRONG);

    // Only STRONG and BOUNDED_STALENESS are supported
    if (consistencyLevel != ConsistencyLevel.STRONG
        && consistencyLevel != ConsistencyLevel.BOUNDED_STALENESS) {
      throw new IllegalArgumentException(
          CoreError.INVALID_CONSISTENCY_LEVEL.buildMessage(consistencyLevel));
    }

    return consistencyLevel;
  }
}
