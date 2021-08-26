package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import com.scalar.db.api.TransactionState;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public class TransactionResult implements Result {
  private final Result result;
  private final Map<String, Value<?>> values;

  public TransactionResult(Result result) {
    // assume that all the values are projected to the result
    this.result = checkNotNull(result);
    this.values = result.getValues();
  }

  @Override
  public Optional<Key> getPartitionKey() {
    if (result != null) {
      return result.getPartitionKey();
    }
    return Optional.empty();
  }

  @Override
  public Optional<Key> getClusteringKey() {
    if (result != null) {
      return result.getClusteringKey();
    }
    return Optional.empty();
  }

  @Override
  public Optional<Value<?>> getValue(String name) {
    return Optional.ofNullable(values.get(name));
  }

  @Override
  @Nonnull
  public ImmutableMap<String, Value<?>> getValues() {
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
    if (!(o instanceof TransactionResult)) {
      return false;
    }
    TransactionResult other = (TransactionResult) o;
    return this.values.equals(other.values);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("values", values).toString();
  }

  public String getId() {
    return getValue(Attribute.ID).get().getAsString().get();
  }

  public TransactionState getState() {
    return TransactionState.getInstance(getValue(Attribute.STATE).get().getAsInt());
  }

  public int getVersion() {
    return getValue(Attribute.VERSION).get().getAsInt();
  }

  public long getPreparedAt() {
    return getValue(Attribute.PREPARED_AT).get().getAsLong();
  }

  public long getCommittedAt() {
    return getValue(Attribute.COMMITTED_AT).get().getAsLong();
  }

  public boolean isCommitted() {
    return getState().equals(TransactionState.COMMITTED);
  }
}
