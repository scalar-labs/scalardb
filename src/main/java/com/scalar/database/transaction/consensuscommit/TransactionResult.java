package com.scalar.database.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.scalar.database.api.Result;
import com.scalar.database.api.TransactionState;
import com.scalar.database.io.BigIntValue;
import com.scalar.database.io.IntValue;
import com.scalar.database.io.Key;
import com.scalar.database.io.TextValue;
import com.scalar.database.io.Value;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/** A result retrieved from storage. */
@Immutable
public class TransactionResult implements Result {
  private final Result result;
  private final Map<String, Value> values;

  /**
   * Constructs a {@code TransactionResult} from the specified {@link Result}
   *
   * @param result
   */
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
  public Optional<Value> getValue(String name) {
    return Optional.ofNullable(values.get(name));
  }

  @Override
  @Nonnull
  public ImmutableMap<String, Value> getValues() {
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
    if (this.values.equals(other.values)) {
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("values", values).toString();
  }

  public String getId() {
    return ((TextValue) getValue(Attribute.ID).get()).getString().get();
  }

  public TransactionState getState() {
    return TransactionState.getInstance(((IntValue) getValue(Attribute.STATE).get()).get());
  }

  public int getVersion() {
    return ((IntValue) getValue(Attribute.VERSION).get()).get();
  }

  public long getPreparedAt() {
    return ((BigIntValue) getValue(Attribute.PREPARED_AT).get()).get();
  }

  public long getCommittedAt() {
    return ((BigIntValue) getValue(Attribute.COMMITTED_AT).get()).get();
  }

  public boolean isCommitted() {
    if (getState().equals(TransactionState.COMMITTED)) {
      return true;
    }
    return false;
  }
}
