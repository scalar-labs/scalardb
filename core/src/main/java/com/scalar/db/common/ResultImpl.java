package com.scalar.db.common;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.util.ScalarDbUtils;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ResultImpl extends AbstractResult {

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
    return Optional.of(ScalarDbUtils.getPartitionKey(this, metadata));
  }

  /** @deprecated As of release 3.8.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<Key> getClusteringKey() {
    return ScalarDbUtils.getClusteringKey(this, metadata);
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
  public LocalDate getDate(String columnName) {
    checkIfExists(columnName);
    Column<?> column = columns.get(columnName);
    assert column != null;
    return column.getDateValue();
  }

  @Nullable
  @Override
  public LocalTime getTime(String columnName) {
    checkIfExists(columnName);
    Column<?> column = columns.get(columnName);
    assert column != null;
    return column.getTimeValue();
  }

  @Nullable
  @Override
  public LocalDateTime getTimestamp(String columnName) {
    checkIfExists(columnName);
    Column<?> column = columns.get(columnName);
    assert column != null;
    return column.getTimestampValue();
  }

  @Nullable
  @Override
  public Instant getTimestampTZ(String columnName) {
    checkIfExists(columnName);
    Column<?> column = columns.get(columnName);
    assert column != null;
    return column.getTimestampTZValue();
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
