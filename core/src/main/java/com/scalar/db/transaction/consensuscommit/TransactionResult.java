package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Result;
import com.scalar.db.api.TransactionState;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.util.AbstractResult;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class TransactionResult extends AbstractResult {
  private final Result result;

  public TransactionResult(Result result) {
    // assume that all the values are projected to the result
    this.result = checkNotNull(result);
  }

  @Override
  public Optional<Key> getPartitionKey() {
    return result.getPartitionKey();
  }

  @Override
  public Optional<Key> getClusteringKey() {
    return result.getClusteringKey();
  }

  @Override
  public Optional<Value<?>> getValue(String name) {
    return result.getValue(name);
  }

  @Override
  @Nonnull
  public Map<String, Value<?>> getValues() {
    return result.getValues();
  }

  @Override
  public boolean isNull(String name) {
    return result.isNull(name);
  }

  @Override
  public boolean getBoolean(String name) {
    return result.getBoolean(name);
  }

  @Override
  public int getInt(String name) {
    return result.getInt(name);
  }

  @Override
  public long getBigInt(String name) {
    return result.getBigInt(name);
  }

  @Override
  public float getFloat(String name) {
    return result.getFloat(name);
  }

  @Override
  public double getDouble(String name) {
    return result.getDouble(name);
  }

  @Nullable
  @Override
  public String getText(String name) {
    return result.getText(name);
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(String name) {
    return result.getBlobAsByteBuffer(name);
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(String name) {
    return result.getBlobAsBytes(name);
  }

  @Nullable
  @Override
  public Object getObject(String name) {
    return result.getObject(name);
  }

  @Override
  public boolean contains(String name) {
    return result.contains(name);
  }

  @Override
  public Set<String> getContainedColumnNames() {
    return result.getContainedColumnNames();
  }

  public String getId() {
    return getText(Attribute.ID);
  }

  public TransactionState getState() {
    return TransactionState.getInstance(getInt(Attribute.STATE));
  }

  public int getVersion() {
    return getInt(Attribute.VERSION);
  }

  public long getPreparedAt() {
    return getBigInt(Attribute.PREPARED_AT);
  }

  public long getCommittedAt() {
    return getBigInt(Attribute.COMMITTED_AT);
  }

  public boolean isCommitted() {
    return getState().equals(TransactionState.COMMITTED);
  }
}
