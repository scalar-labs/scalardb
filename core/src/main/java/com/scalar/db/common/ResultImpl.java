package com.scalar.db.common;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class ResultImpl extends AbstractResult {
  private static final Logger logger = LoggerFactory.getLogger(ResultImpl.class);

  private final Map<String, Column<?>> columns;
  private final TableMetadata metadata;

  public ResultImpl(Map<String, Column<?>> columns, TableMetadata metadata) {
    this.columns = ImmutableMap.copyOf(Objects.requireNonNull(columns));
    this.metadata = Objects.requireNonNull(metadata);
  }

  @Override
  public Optional<Key> getPartitionKey() {
    return getKey(metadata.getPartitionKeyNames());
  }

  @Override
  public Optional<Key> getClusteringKey() {
    return getKey(metadata.getClusteringKeyNames());
  }

  private Optional<Key> getKey(LinkedHashSet<String> names) {
    if (names.isEmpty()) {
      return Optional.empty();
    }
    Key.Builder builder = Key.newBuilder();
    for (String name : names) {
      Column<?> column = columns.get(name);
      if (column == null) {
        logger.warn("full key doesn't seem to be projected into the result");
        return Optional.empty();
      }
      builder.add(column);
    }
    return Optional.of(builder.build());
  }

  @Override
  public boolean isNull(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).hasNullValue();
  }

  @Override
  public boolean getBoolean(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getBooleanValue();
  }

  @Override
  public int getInt(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getIntValue();
  }

  @Override
  public long getBigInt(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getBigIntValue();
  }

  @Override
  public float getFloat(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getFloatValue();
  }

  @Override
  public double getDouble(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getDoubleValue();
  }

  @Nullable
  @Override
  public String getText(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getTextValue();
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getBlobValueAsByteBuffer();
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getBlobValueAsBytes();
  }

  @Nullable
  @Override
  public Object getAsObject(String columnName) {
    checkIfExists(columnName);
    return columns.get(columnName).getValueAsObject();
  }

  @Override
  public boolean contains(String columnName) {
    return columns.containsKey(columnName);
  }

  @Override
  public Set<String> getContainedColumnNames() {
    return columns.keySet();
  }

  @Override
  public Map<String, Column<?>> getColumns() {
    return columns;
  }
}
