package com.scalar.db.storage.cosmos;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import com.scalar.db.api.Selection;
import com.scalar.db.exception.storage.InvalidMetadataException;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class ResultImpl implements Result {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultImpl.class);
  private final TableMetadata metadata;
  private final Map<String, Value> values;

  public ResultImpl(Record record, Selection selection, TableMetadata metadata) {
    checkNotNull(record);
    this.metadata = checkNotNull(metadata);
    values = new HashMap<>();
    interpret(record, selection, metadata);
  }

  @Override
  public Optional<Key> getPartitionKey() {
    return getKey(metadata.getPartitionKeyNames());
  }

  @Override
  public Optional<Key> getClusteringKey() {
    return getKey(metadata.getClusteringKeyNames());
  }

  @Override
  public Optional<Value> getValue(String name) {
    return Optional.ofNullable(values.get(name));
  }

  @Override
  @Nonnull
  public Map<String, Value> getValues() {
    return ImmutableMap.copyOf(values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(values);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof ResultImpl)) {
      return false;
    }
    ResultImpl other = (ResultImpl) o;
    if (this.values.equals(other.values)) {
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("values", values).toString();
  }

  @VisibleForTesting
  void interpret(Record record, Selection selection, TableMetadata metadata) {
    record.getPartitionKey().forEach((name, value) -> add(name, value));
    record.getClusteringKey().forEach((name, value) -> add(name, value));

    // This isn't actual projection...
    if (selection.getProjections().isEmpty()) {
      record.getValues().forEach((name, value) -> add(name, value));
    } else {
      Map<String, Object> recordValues = record.getValues();
      selection
          .getProjections()
          .forEach(
              name -> {
                if (recordValues.containsKey(name)) {
                  add(name, recordValues.get(name));
                }
              });
    }
  }

  private void add(String name, Object value) {
    if (metadata.getColumns().containsKey(name)) {
      values.put(name, convert(value, name, metadata.getColumns().get(name)));
    } else {
      throw new InvalidMetadataException("metadata doesn't have the specified column: " + name);
    }
  }

  private Optional<Key> getKey(Set<String> names) {
    List<Value> list = new ArrayList<>();
    for (String name : names) {
      Value value = values.get(name);
      if (value == null) {
        LOGGER.warn("full key doesn't seem to be projected into the result");
        return Optional.empty();
      }
      list.add(value);
    }
    return Optional.of(new Key(list));
  }

  private Value convert(Object recordValue, String name, String type)
      throws UnsupportedTypeException {
    switch (type) {
      case "boolean":
        return new BooleanValue(name, (boolean) recordValue);
      case "int":
        return new IntValue(name, (int) recordValue);
      case "bigint":
        return new BigIntValue(name, (long) recordValue);
      case "float":
        return new FloatValue(name, (float) recordValue);
      case "double":
        return new DoubleValue(name, (double) recordValue);
      case "text": // for backwards compatibility
      case "varchar":
        return new TextValue(name, (String) recordValue);
      case "blob":
        return new BlobValue(name, ((String) recordValue).getBytes(StandardCharsets.UTF_8));
      default:
        throw new UnsupportedTypeException(type);
    }
  }
}
