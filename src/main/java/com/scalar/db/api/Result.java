package com.scalar.db.api;

import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.Map;
import java.util.Optional;

/**
 * A result retrieved from {@link DistributedStorage}.
 *
 * @author Hiroyuki Yamada
 */
public interface Result {

  /**
   * Returns the partition {@link Key}
   *
   * @return an {@code Optional} with the partition {@code Key}
   */
  Optional<Key> getPartitionKey();

  /**
   * Returns the clustering {@link Key}
   *
   * @return an {@code Optional} with the clustering {@code Key}
   */
  Optional<Key> getClusteringKey();

  /**
   * Returns the {@link Value} which the specified name is mapped to
   *
   * @param name name of the {@code Value}
   * @return an {@code Optional} with the {@code Value}
   */
  Optional<Value<?>> getValue(String name);

  /**
   * Returns a map of {@link Value}s
   *
   * @return a map of {@code Value}s
   */
  Map<String, Value<?>> getValues();
}
