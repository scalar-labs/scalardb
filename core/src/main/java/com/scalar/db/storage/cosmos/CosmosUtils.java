package com.scalar.db.storage.cosmos;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.google.common.annotations.VisibleForTesting;
import java.util.Locale;

public final class CosmosUtils {

  private CosmosUtils() {}

  public static String quoteKeyword(String keyword) {
    return "[\"" + keyword + "\"]";
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
          "The specified consistency level is not supported:" + consistencyLevel);
    }

    return consistencyLevel;
  }
}
