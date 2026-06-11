package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Result;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.AbstractResult;
import com.scalar.db.io.Column;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class TransactionResult extends AbstractResult {
  private final Result result;

  public TransactionResult(Result result) {
    // assume that all the columns are projected to the result
    this.result = checkNotNull(result);
  }

  /** @deprecated As of release 3.8.0. Will be removed in release 4.0.0 */
  @Deprecated
  @Override
  public Optional<Key> getPartitionKey() {
    return result.getPartitionKey();
  }

  /** @deprecated As of release 3.8.0. Will be removed in release 4.0.0 */
  @Deprecated
  @Override
  public Optional<Key> getClusteringKey() {
    return result.getClusteringKey();
  }

  @Override
  public boolean isNull(String columnName) {
    return result.isNull(columnName);
  }

  @Override
  public boolean getBoolean(String columnName) {
    return result.getBoolean(columnName);
  }

  @Override
  public int getInt(String columnName) {
    return result.getInt(columnName);
  }

  @Override
  public long getBigInt(String columnName) {
    return result.getBigInt(columnName);
  }

  @Override
  public float getFloat(String columnName) {
    return result.getFloat(columnName);
  }

  @Override
  public double getDouble(String columnName) {
    return result.getDouble(columnName);
  }

  @Nullable
  @Override
  public String getText(String columnName) {
    return result.getText(columnName);
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(String columnName) {
    return result.getBlobAsByteBuffer(columnName);
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(String columnName) {
    return result.getBlobAsBytes(columnName);
  }

  @Nullable
  @Override
  public LocalDate getDate(String columnName) {
    return result.getDate(columnName);
  }

  @Nullable
  @Override
  public LocalTime getTime(String columnName) {
    return result.getTime(columnName);
  }

  @Nullable
  @Override
  public LocalDateTime getTimestamp(String columnName) {
    return result.getTimestamp(columnName);
  }

  @Nullable
  @Override
  public Instant getTimestampTZ(String columnName) {
    return result.getTimestampTZ(columnName);
  }

  @Nullable
  @Override
  public Object getAsObject(String columnName) {
    return result.getAsObject(columnName);
  }

  @Override
  public boolean contains(String columnName) {
    return result.contains(columnName);
  }

  @Override
  public Set<String> getContainedColumnNames() {
    return result.getContainedColumnNames();
  }

  @Override
  public Map<String, Column<?>> getColumns() {
    return result.getColumns();
  }

  @Nullable
  public String getId() {
    return getText(Attribute.ID);
  }

  /**
   * Returns the transaction state ({@code tx_state}) of this record.
   *
   * <p>When {@code tx_state} is null, the record is deemed as committed and {@code COMMITTED} is
   * returned: a record imported into a table initially has no transaction metadata, which is a
   * legitimate state (see {@link #isCommitted()} and {@link #isDeemedAsCommitted()}).
   *
   * @return the transaction state, or {@code COMMITTED} when this record has no transaction
   *     metadata
   */
  public TransactionState getState() {
    if (isNull(Attribute.STATE)) {
      // To handle existing databases that do not have transaction metadata, the record is deemed as
      // committed if the state is NULL.
      return TransactionState.COMMITTED;
    }
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

  /**
   * Returns whether this record is in the committed state.
   *
   * <p>This returns {@code true} when the transaction state ({@code tx_state}) is {@code
   * COMMITTED}. It also returns {@code true} when the record has no transaction metadata (i.e.
   * {@code tx_state} is null): when a table is imported, its existing records initially have no
   * transaction metadata. That is a legitimate state and such a record is deemed as committed (see
   * {@link #getState()}).
   *
   * @return {@code true} if this record is committed, including records that are deemed as
   *     committed
   */
  public boolean isCommitted() {
    return getState().equals(TransactionState.COMMITTED);
  }

  /**
   * Returns whether this record is deemed as committed because it carries no transaction metadata.
   *
   * <p>When a table is imported, its existing records initially have no transaction metadata. This
   * is a legitimate state, and such a record is deemed as committed.
   *
   * <p>Both this method and {@link #isCommitted()} capture the "deemed as committed" notion of a
   * record without transaction metadata, but inspect different columns: this method keys off the
   * absence of the transaction ID ({@code tx_id}), while {@code isCommitted()} keys off the
   * transaction state ({@code tx_state}, via {@link #getState()}). They agree for a record without
   * transaction metadata because such a record has both {@code tx_id} and {@code tx_state} absent —
   * a record written by ScalarDB always carries both together.
   *
   * @return {@code true} if this record has no transaction ID and is therefore deemed as committed
   */
  public boolean isDeemedAsCommitted() {
    return getId() == null;
  }

  public boolean isMergedResult() {
    return result instanceof MergedResult;
  }

  public boolean hasBeforeImage() {
    // We need to check not only before_id but also before_version to determine if the record has
    // the before image or not since we set before_version to 0 for the prepared record when
    // updating the record deemed as the committed state (cf. PrepareMutationComposer).
    return !getBeforeIdColumn().hasNullValue() || !getBeforeVersionColumn().hasNullValue();
  }

  private TextColumn getBeforeIdColumn() {
    return (TextColumn) result.getColumns().get(Attribute.BEFORE_ID);
  }

  private IntColumn getBeforeVersionColumn() {
    return (IntColumn) result.getColumns().get(Attribute.BEFORE_VERSION);
  }
}
