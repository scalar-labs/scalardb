package com.scalar.db.transaction.consensuscommit.cbrl;

import com.scalar.db.transaction.consensuscommit.proto.v1.Column;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The replayed state of one record. Mirrors SSR's per-record fields: the cursor {@code currentTxId}
 * (the tx that last wrote the record, {@code null} if absent), the {@code deleted} tombstone flag,
 * the merged non-key {@code columns}, and {@code insertTxIds} — the set of already-applied INSERT
 * tx ids, needed to dedup INSERT roots on a re-run (idempotency), since roots have no inbound chain
 * link to dedup against.
 *
 * <p>A record is observably present when it has a current tx and is not deleted. Two states are
 * observably equal when both are absent, or both present with equal columns — what a restore writes
 * back. {@code insertTxIds}/{@code currentTxId} are internal bookkeeping and not part of that.
 */
final class RecordState {
  @Nullable private final String currentTxId;
  private final boolean deleted;
  private final Map<String, Column> columns;
  private final Set<String> insertTxIds;

  private RecordState(
      @Nullable String currentTxId,
      boolean deleted,
      Map<String, Column> columns,
      Set<String> insertTxIds) {
    this.currentTxId = currentTxId;
    this.deleted = deleted;
    this.columns = columns;
    this.insertTxIds = insertTxIds;
  }

  /** The state of a key with no record (never inserted, or fully deleted and rolled forward). */
  static RecordState absent() {
    return new RecordState(null, false, Collections.emptyMap(), Collections.emptySet());
  }

  static RecordState of(
      @Nullable String currentTxId,
      boolean deleted,
      Map<String, Column> columns,
      Set<String> insertTxIds) {
    return new RecordState(currentTxId, deleted, columns, insertTxIds);
  }

  @Nullable
  String currentTxId() {
    return currentTxId;
  }

  boolean present() {
    return currentTxId != null && !deleted;
  }

  Map<String, Column> columns() {
    return columns;
  }

  /** Mutable working copy used while replaying a key's chain. */
  Builder toBuilder() {
    return new Builder(currentTxId, deleted, columns, insertTxIds);
  }

  static final class Builder {
    @Nullable private String currentTxId;
    private boolean deleted;
    private final Map<String, Column> columns;
    private final Set<String> insertTxIds;

    private Builder(
        @Nullable String currentTxId,
        boolean deleted,
        Map<String, Column> columns,
        Set<String> insertTxIds) {
      this.currentTxId = currentTxId;
      this.deleted = deleted;
      this.columns = new LinkedHashMap<>(columns);
      this.insertTxIds = new HashSet<>(insertTxIds);
    }

    /** INSERT: the record is (re)created, so its columns become exactly the insert's. */
    void applyInsert(Iterable<Column> insertColumns) {
      columns.clear();
      for (Column column : insertColumns) {
        columns.put(column.getName(), column);
      }
      deleted = false;
    }

    /** UPDATE: partial — merge the changed columns onto the existing ones. */
    void applyUpdate(Iterable<Column> updatedColumns) {
      for (Column column : updatedColumns) {
        columns.put(column.getName(), column);
      }
      deleted = false;
    }

    /** DELETE: logical tombstone — drop the columns and mark deleted. */
    void applyDelete() {
      columns.clear();
      deleted = true;
    }

    void markInsertApplied(String txId) {
      insertTxIds.add(txId);
    }

    boolean isInsertApplied(String txId) {
      return insertTxIds.contains(txId);
    }

    void advanceCursor(@Nullable String txId) {
      currentTxId = txId;
    }

    @Nullable
    String currentTxId() {
      return currentTxId;
    }

    boolean deleted() {
      return deleted;
    }

    RecordState build() {
      return new RecordState(currentTxId, deleted, columns, insertTxIds);
    }
  }

  /** Observable equality: both absent, or both present with equal merged columns. */
  boolean observablyEquals(RecordState other) {
    if (present() != other.present()) {
      return false;
    }
    return !present() || columns.equals(other.columns);
  }

  @Override
  public String toString() {
    if (!present()) {
      return "RecordState{absent" + (deleted ? "(deleted)" : "") + ", cur=" + currentTxId + '}';
    }
    return "RecordState{present, cur=" + currentTxId + ", columns=" + columns.keySet() + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RecordState)) {
      return false;
    }
    RecordState other = (RecordState) o;
    return deleted == other.deleted
        && Objects.equals(currentTxId, other.currentTxId)
        && columns.equals(other.columns)
        && insertTxIds.equals(other.insertTxIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(currentTxId, deleted, columns, insertTxIds);
  }
}
