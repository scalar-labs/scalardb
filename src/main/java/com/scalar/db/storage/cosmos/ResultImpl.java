package com.scalar.db.storage.cosmos;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ResultImpl implements Result {
  private final TableMetadata metadata;
  private final Map<String, Value<?>> values;

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
  public Optional<Value<?>> getValue(String name) {
    return Optional.ofNullable(values.get(name));
  }

  @Override
  @Nonnull
  public Map<String, Value<?>> getValues() {
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
    Map<String, Object> recordValues = record.getValues();
    if (selection.getProjections().isEmpty()) {
      metadata
          .getColumnNames()
          .forEach(
              name -> {
                add(name, recordValues.get(name));
              });
    } else {
      // This isn't actual projection...
      selection
          .getProjections()
          .forEach(
              name -> {
                add(name, recordValues.get(name));
              });
    }

    metadata.getPartitionKeyNames().forEach(name -> add(name, record.getPartitionKey().get(name)));
    metadata
        .getClusteringKeyNames()
        .forEach(name -> add(name, record.getClusteringKey().get(name)));
  }

  private void add(String name, Object value) {
    values.put(name, convert(value, name, metadata.getColumnDataType(name)));
  }

  private Optional<Key> getKey(LinkedHashSet<String> names) {
    List<Value<?>> list = new ArrayList<>();
    for (String name : names) {
      Value<?> value = values.get(name);
      list.add(value);
    }
    return Optional.of(new Key(list));
  }

  private Value<?> convert(Object recordValue, String name, DataType dataType)
      throws UnsupportedTypeException {
    // When recordValue is NULL, the value will be the default value.
    // It is the same behavior as the datastax C* driver
    switch (dataType) {
      case BOOLEAN:
        return new BooleanValue(name, recordValue == null ? false : (boolean) recordValue);
      case INT:
        return new IntValue(name, recordValue == null ? 0 : ((Number) recordValue).intValue());
      case BIGINT:
        return new BigIntValue(name, recordValue == null ? 0L : ((Number) recordValue).longValue());
      case FLOAT:
        return new FloatValue(
            name, recordValue == null ? 0.0f : ((Number) recordValue).floatValue());
      case DOUBLE:
        return new DoubleValue(
            name, recordValue == null ? 0.0 : ((Number) recordValue).doubleValue());
      case TEXT:
        return new TextValue(
            name,
            recordValue == null
                ? null
                : new String(
                    ((String) recordValue).getBytes(StandardCharsets.UTF_8),
                    StandardCharsets.UTF_8));
      case BLOB:
        return new BlobValue(
            name,
            recordValue == null
                ? null
                : Base64.getDecoder()
                    .decode(((String) recordValue).getBytes(StandardCharsets.UTF_8)));
      default:
        throw new AssertionError();
    }
  }
}
