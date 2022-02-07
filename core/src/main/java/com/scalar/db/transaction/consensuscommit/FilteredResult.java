package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@code Result} to filter out unprojected columns and transactional columns.
 */
@Immutable
public class FilteredResult implements Result {
  private static final Logger LOGGER = LoggerFactory.getLogger(FilteredResult.class);

  private final Result result;
  private final Set<String> projections;
  private final Supplier<Map<String, Value<?>>> values;

  public FilteredResult(Result result, List<String> projections) {
    // assume that all the values are projected to the original result
    this.result = Objects.requireNonNull(result);
    this.projections = new HashSet<>(Objects.requireNonNull(projections));
    values = Suppliers.memoize(this::filterValues); // lazy loading
  }

  private Map<String, Value<?>> filterValues() {
    return result.getValues().entrySet().stream()
        .filter(e -> projections.isEmpty() || projections.contains(e.getKey()))
        .filter(
            e ->
                !ConsensusCommitUtils.isTransactionalMetaColumn(
                    e.getKey(), result.getValues().keySet()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Optional<Key> getPartitionKey() {
    return getKey(result.getPartitionKey());
  }

  @Override
  public Optional<Key> getClusteringKey() {
    return getKey(result.getClusteringKey());
  }

  private Optional<Key> getKey(Optional<Key> key) {
    if (!key.isPresent()) {
      return Optional.empty();
    }
    if (projections.isEmpty()) {
      return key;
    }
    for (Value<?> value : key.get()) {
      if (!projections.contains(value.getName())) {
        LOGGER.warn("full key doesn't seem to be projected into the result");
        return Optional.empty();
      }
    }
    return key;
  }

  @Override
  public Optional<Value<?>> getValue(String name) {
    return Optional.ofNullable(values.get().get(name));
  }

  @Override
  public Map<String, Value<?>> getValues() {
    return ImmutableMap.copyOf(values.get());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FilteredResult)) {
      return false;
    }
    FilteredResult that = (FilteredResult) o;
    return values.get().equals(that.values.get());
  }

  @Override
  public int hashCode() {
    return Objects.hash(values.get());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("values", values.get()).toString();
  }

  @VisibleForTesting
  Result getOriginalResult() {
    return result;
  }
}
