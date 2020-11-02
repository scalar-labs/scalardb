package com.scalar.db.storage.dynamo;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import com.scalar.db.api.Selection;
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
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@Immutable
public class ResultImpl implements Result {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultImpl.class);
  private final Selection selection;
  private final TableMetadata metadata;
  private final Map<String, Value> values;

  public ResultImpl(Map<String, AttributeValue> item, Selection selection, TableMetadata metadata) {
    checkNotNull(item);
    this.selection = selection;
    this.metadata = checkNotNull(metadata);
    values = new HashMap<>();
    interpret(item, metadata);
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
  void interpret(Map<String, AttributeValue> item, TableMetadata metadata) {
    if (selection.getProjections().isEmpty()) {
      metadata
          .getColumns()
          .forEach(
              (name, type) -> {
                add(name, item.get(name));
              });
    } else {
      selection
          .getProjections()
          .forEach(
              name -> {
                add(name, item.get(name));
              });
    }

    metadata.getPartitionKeyNames().forEach(name -> add(name, item.get(name)));
    metadata.getClusteringKeyNames().forEach(name -> add(name, item.get(name)));
  }

  private void add(String name, AttributeValue itemValue) {
    values.put(name, convert(itemValue, name, metadata.getColumns().get(name)));
  }

  private Optional<Key> getKey(Set<String> names) {
    List<Value> list = new ArrayList<>();
    for (String name : names) {
      Value value = values.get(name);
      list.add(value);
    }
    return Optional.of(new Key(list));
  }

  private Value convert(AttributeValue itemValue, String name, String type)
      throws UnsupportedTypeException {
    // When itemValue is NULL, the value will be the default value.
    // It is the same behavior as the datastax C* driver
    boolean isNull = itemValue == null || (itemValue.nul() != null && itemValue.nul());
    switch (type) {
      case "boolean":
        return new BooleanValue(name, isNull ? false : itemValue.bool());
      case "int":
        return new IntValue(name, isNull ? 0 : Integer.valueOf(itemValue.n()));
      case "bigint":
        return new BigIntValue(name, isNull ? 0L : Long.valueOf(itemValue.n()));
      case "float":
        return new FloatValue(name, isNull ? 0.0f : Float.valueOf(itemValue.n()));
      case "double":
        return new DoubleValue(name, isNull ? 0.0 : Double.valueOf(itemValue.n()));
      case "text": // for backwards compatibility
      case "varchar":
        return new TextValue(name, isNull ? (String) null : itemValue.s());
      case "blob":
        return new BlobValue(name, isNull ? null : itemValue.b().asByteArray());
      default:
        throw new UnsupportedTypeException(type);
    }
  }
}
