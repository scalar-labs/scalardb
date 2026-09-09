package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.PartitionKeyDefinition;
import com.azure.cosmos.models.PartitionKeyDefinitionVersion;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;

/** Utilities for Cosmos DB partition key V1/V2 investigation tests. */
public final class CosmosPartitionKeyTestUtils {

  private CosmosPartitionKeyTestUtils() {}

  /** Returns a string of exactly {@code numBytes} ASCII characters (1 byte each in UTF-8). */
  public static String asciiOfLength(int numBytes) {
    if (numBytes <= 0) {
      return "";
    }
    return String.join("", Collections.nCopies(numBytes, "x"));
  }

  /**
   * Two distinct keys of {@code totalBytes} length that share the same first {@code totalBytes - 1}
   * bytes and differ only in the last byte.
   */
  public static String[] collisionPairAtByteBoundary(int totalBytes) {
    if (totalBytes < 2) {
      throw new IllegalArgumentException("totalBytes must be at least 2");
    }
    String prefix = asciiOfLength(totalBytes - 1);
    return new String[] {prefix + "A", prefix + "B"};
  }

  /** UTF-8 byte length of the given string. */
  public static int utf8ByteLength(String value) {
    return value.getBytes(StandardCharsets.UTF_8).length;
  }

  /** Reads the partition key definition version for the given container. */
  public static Optional<PartitionKeyDefinitionVersion> readPartitionKeyVersion(
      CosmosClient client, String namespace, String table) {
    CosmosContainer container = client.getDatabase(namespace).getContainer(table);
    PartitionKeyDefinition definition =
        container.read().getProperties().getPartitionKeyDefinition();
    return Optional.ofNullable(definition.getVersion());
  }

  /** Returns true when the container uses V1 or leaves the version unset (ScalarDB default). */
  public static boolean isV1OrUnset(Optional<PartitionKeyDefinitionVersion> version) {
    return version.isEmpty() || version.get() == PartitionKeyDefinitionVersion.V1;
  }
}
