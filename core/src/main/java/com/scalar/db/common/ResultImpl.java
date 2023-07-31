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

  private final ImmutableMap<String, Column<?>> columns;
  private final TableMetadata metadata;

  public ResultImpl(Map<String, Column<?>> columns, TableMetadata metadata) {
    this.columns = ImmutableMap.copyOf(Objects.requireNonNull(columns));
    this.metadata = Objects.requireNonNull(metadata);
  }

  /** @deprecated As of release 3.8.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<Key> getPartitionKey() {
    return getKey(metadata.getPartitionKeyNames());
  }

  /** @deprecated As of release 3.8.0. Will be removed in release 5.0.0 */
  @Deprecated
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
        logger.warn("Full key doesn't seem to be projected into the result");
        return Optional.empty();
      }
      builder.add(column);
    }
    return Optional.of(builder.build());
  }

  @Override
  public boolean isNull(String columnName) {
    checkIfExists(columnName);
    Column<?> column = columns.get(columnName);
    assert column != null;
    return column.hasNullValue();
  }

  @Override
  public boolean getBoolean(String columnName) {
    checkIfExists(columnName);
    Column<?> column = columns.get(columnName);
    assert column != null;
    return column.getBooleanValue();
  }

  @Override
  public int getInt(String columnName) {
    checkIfExists(columnName);
    Column<?> column = columns.get(columnName);
    assert column != null;
    return column.getIntValue();
  }

  @Override
  public long getBigInt(String columnName) {
    checkIfExists(columnName);
    Column<?> column = columns.get(columnName);
    assert column != null;
    return column.getBigIntValue();
  }

  @Override
  public float getFloat(String columnName) {
    checkIfExists(columnName);
    Column<?> column = columns.get(columnName);
    assert column != null;
    return column.getFloatValue();
  }

  @Override
  public double getDouble(String columnName) {
    checkIfExists(columnName);
    Column<?> column = columns.get(columnName);
    assert column != null;
    return column.getDoubleValue();
  }

  @Nullable
  @Override
  public String getText(String columnName) {
    checkIfExists(columnName);
    Column<?> column = columns.get(columnName);
    assert column != null;
    return column.getTextValue();
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(String columnName) {
    checkIfExists(columnName);
    Column<?> column = columns.get(columnName);
    assert column != null;
    return column.getBlobValueAsByteBuffer();
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(String columnName) {
    checkIfExists(columnName);
    Column<?> column = columns.get(columnName);
    assert column != null;
    return column.getBlobValueAsBytes();
  }

  @Nullable
  @Override
  public Object getAsObject(String columnName) {
    checkIfExists(columnName);
    Column<?> column = columns.get(columnName);
    assert column != null;
    return column.getValueAsObject();
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
