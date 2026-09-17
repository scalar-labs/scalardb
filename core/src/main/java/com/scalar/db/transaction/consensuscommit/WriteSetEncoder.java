package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.ColumnVisitor;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column.BigIntValue;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column.BlobValue;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column.BooleanValue;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column.DateValue;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column.DoubleValue;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column.FloatValue;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column.IntValue;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column.TextValue;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column.TimeValue;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column.TimestampTZValue;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column.TimestampValue;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup;
import com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroups;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import com.scalar.db.util.TimeRelatedColumnEncodingUtils;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Encodes the proto {@link WriteSet} / {@link EntryGroup} payload persisted in the Coordinator
 * table's {@code tx_write_set} column.
 *
 * <p>An {@link Entry} currently records only a record's identity — namespace, table, partition key,
 * optional clustering key — plus the owning participant on the multi-participant TwoPhaseCommit
 * path. The written column values are not recorded, and neither is whether the record was put or
 * deleted (that is recoverable from the record's own {@code tx_state}). The proto's {@code
 * schema_version} comment states which of those can be added without bumping the version.
 */
final class WriteSetEncoder {
  /**
   * The {@code schema_version} stamped on every persisted {@link WriteSet}. A single definition
   * governs both write paths — the single-group encoding ({@link #encodeSingleGroupWriteSet}) and
   * the normal-group parent-row merge ({@link #mergeChildWriteSets}) — so they cannot drift.
   */
  private static final int SCHEMA_VERSION = 1;

  private WriteSetEncoder() {}

  /**
   * Encodes a {@link WriteSet} for a single-group transaction (non-group-commit, or a delayed group
   * commit that contains only this transaction).
   *
   * <p>For a transaction with writes/deletes, the returned WriteSet carries a single populated
   * {@link EntryGroup}. For a read-only transaction, it carries an {@link EntryGroups} with an
   * empty {@code entry_groups} list — the payload oneof is always set, explicitly recording that
   * the transaction had nothing to write rather than leaving the payload absent. {@code
   * schema_version} is always set so that the persisted BLOB is unambiguously non-null on every
   * storage backend, distinguishing it from a NULL column (which indicates "no info" for
   * lazy-recovery aborts or pre-feature rows).
   *
   * @param context the transaction context
   * @return the encoded {@link WriteSet}
   */
  static WriteSet encodeSingleGroupWriteSet(TransactionContext context) {
    EntryGroups.Builder entryGroups = EntryGroups.newBuilder();
    if (context.snapshot.hasWritesOrDeletes()) {
      entryGroups.addEntryGroups(encodeEntryGroup(context.snapshot, null));
    }
    return WriteSet.newBuilder()
        .setSchemaVersion(SCHEMA_VERSION)
        .setEntryGroups(entryGroups)
        .build();
  }

  /**
   * Encodes an {@link EntryGroup} from the {@link Snapshot}'s write/delete sets.
   *
   * <p>Visible for testing only — production callers go through {@link
   * #encodeSingleGroupWriteSet(TransactionContext)}.
   *
   * @param snapshot the snapshot of the transaction
   * @param childId the child id within a group commit, or {@code null} for non-group-commit
   * @return the encoded {@link EntryGroup}
   */
  @VisibleForTesting
  static EntryGroup encodeEntryGroup(Snapshot snapshot, @Nullable String childId) {
    EntryGroup.Builder builder = EntryGroup.newBuilder();
    if (childId != null) {
      builder.setChildId(childId);
    }
    for (Map.Entry<Snapshot.Key, Put> e : snapshot.getWriteSet()) {
      builder.addEntries(encodeEntry(e.getValue()));
    }
    for (Map.Entry<Snapshot.Key, Delete> e : snapshot.getDeleteSet()) {
      builder.addEntries(encodeEntry(e.getValue()));
    }
    return builder.build();
  }

  private static Entry encodeEntry(Mutation mutation) {
    Entry.Builder builder = Entry.newBuilder();
    mutation.forNamespace().ifPresent(builder::setNamespaceName);
    mutation.forTable().ifPresent(builder::setTableName);
    builder.setPartitionKey(encodeKey(mutation.getPartitionKey()));
    mutation.getClusteringKey().ifPresent(ck -> builder.setClusteringKey(encodeKey(ck)));
    return builder.build();
  }

  private static com.scalar.db.transaction.consensuscommit.proto.v1.Key encodeKey(Key key) {
    com.scalar.db.transaction.consensuscommit.proto.v1.Key.Builder builder =
        com.scalar.db.transaction.consensuscommit.proto.v1.Key.newBuilder();
    for (Column<?> column : key.getColumns()) {
      builder.addColumns(encodeColumn(column));
    }
    return builder.build();
  }

  private static com.scalar.db.transaction.consensuscommit.proto.v1.Column encodeColumn(
      Column<?> column) {
    com.scalar.db.transaction.consensuscommit.proto.v1.Column.Builder builder =
        com.scalar.db.transaction.consensuscommit.proto.v1.Column.newBuilder()
            .setName(column.getName());
    column.accept(new ColumnEncodingVisitor(builder));
    return builder.build();
  }

  /** Visitor that fills the proto Column builder with the value oneof. */
  private static final class ColumnEncodingVisitor implements ColumnVisitor {
    private final com.scalar.db.transaction.consensuscommit.proto.v1.Column.Builder builder;

    ColumnEncodingVisitor(
        com.scalar.db.transaction.consensuscommit.proto.v1.Column.Builder builder) {
      this.builder = builder;
    }

    @Override
    public void visit(BooleanColumn column) {
      BooleanValue.Builder valueBuilder = BooleanValue.newBuilder();
      if (!column.hasNullValue()) {
        valueBuilder.setValue(column.getBooleanValue());
      }
      builder.setBooleanValue(valueBuilder);
    }

    @Override
    public void visit(IntColumn column) {
      IntValue.Builder valueBuilder = IntValue.newBuilder();
      if (!column.hasNullValue()) {
        valueBuilder.setValue(column.getIntValue());
      }
      builder.setIntValue(valueBuilder);
    }

    @Override
    public void visit(BigIntColumn column) {
      BigIntValue.Builder valueBuilder = BigIntValue.newBuilder();
      if (!column.hasNullValue()) {
        valueBuilder.setValue(column.getBigIntValue());
      }
      builder.setBigintValue(valueBuilder);
    }

    @Override
    public void visit(FloatColumn column) {
      FloatValue.Builder valueBuilder = FloatValue.newBuilder();
      if (!column.hasNullValue()) {
        valueBuilder.setValue(column.getFloatValue());
      }
      builder.setFloatValue(valueBuilder);
    }

    @Override
    public void visit(DoubleColumn column) {
      DoubleValue.Builder valueBuilder = DoubleValue.newBuilder();
      if (!column.hasNullValue()) {
        valueBuilder.setValue(column.getDoubleValue());
      }
      builder.setDoubleValue(valueBuilder);
    }

    @Override
    public void visit(TextColumn column) {
      TextValue.Builder valueBuilder = TextValue.newBuilder();
      if (!column.hasNullValue()) {
        valueBuilder.setValue(column.getTextValue());
      }
      builder.setTextValue(valueBuilder);
    }

    @Override
    public void visit(BlobColumn column) {
      BlobValue.Builder valueBuilder = BlobValue.newBuilder();
      if (!column.hasNullValue()) {
        ByteBuffer blobValue = column.getBlobValue();
        assert blobValue != null;
        valueBuilder.setValue(ByteString.copyFrom(blobValue));
      }
      builder.setBlobValue(valueBuilder);
    }

    @Override
    public void visit(DateColumn column) {
      DateValue.Builder valueBuilder = DateValue.newBuilder();
      if (!column.hasNullValue()) {
        valueBuilder.setValue(TimeRelatedColumnEncodingUtils.encode(column));
      }
      builder.setDateValue(valueBuilder);
    }

    @Override
    public void visit(TimeColumn column) {
      TimeValue.Builder valueBuilder = TimeValue.newBuilder();
      if (!column.hasNullValue()) {
        valueBuilder.setValue(TimeRelatedColumnEncodingUtils.encode(column));
      }
      builder.setTimeValue(valueBuilder);
    }

    @Override
    public void visit(TimestampColumn column) {
      TimestampValue.Builder valueBuilder = TimestampValue.newBuilder();
      if (!column.hasNullValue()) {
        valueBuilder.setValue(TimeRelatedColumnEncodingUtils.encode(column));
      }
      builder.setTimestampValue(valueBuilder);
    }

    @Override
    public void visit(TimestampTZColumn column) {
      TimestampTZValue.Builder valueBuilder = TimestampTZValue.newBuilder();
      if (!column.hasNullValue()) {
        valueBuilder.setValue(TimeRelatedColumnEncodingUtils.encode(column));
      }
      builder.setTimestamptzValue(valueBuilder);
    }
  }

  /**
   * Encodes a {@link WriteSet} from the per-participant write sets aggregated across a
   * multi-participant transaction.
   *
   * <p>One {@link EntryGroup} is emitted per participant (in the map's iteration order), and every
   * {@link Entry} it produces is stamped with the owning participant's id. Participants with no
   * writes are skipped. {@code child_id} is not used in this path (it is reserved for the existing
   * group-commit grouping).
   *
   * @param writeSetsByParticipant the write set produced by each participant, keyed by participant
   *     ID
   * @return the encoded {@link WriteSet}
   */
  static WriteSet encodeFromWriteSetEntries(
      Map<String, List<TwoPhaseCommitParticipant.WriteSetEntry>> writeSetsByParticipant) {
    EntryGroups.Builder entryGroups = EntryGroups.newBuilder();
    for (Map.Entry<String, List<TwoPhaseCommitParticipant.WriteSetEntry>> e :
        writeSetsByParticipant.entrySet()) {
      String participantId = checkNotNull(e.getKey());
      List<TwoPhaseCommitParticipant.WriteSetEntry> entries = e.getValue();
      if (entries.isEmpty()) {
        continue;
      }
      EntryGroup.Builder groupBuilder = EntryGroup.newBuilder();
      for (TwoPhaseCommitParticipant.WriteSetEntry entry : entries) {
        groupBuilder.addEntries(encodeWriteSetEntry(entry, participantId));
      }
      entryGroups.addEntryGroups(groupBuilder.build());
    }
    return WriteSet.newBuilder()
        .setSchemaVersion(SCHEMA_VERSION)
        .setEntryGroups(entryGroups)
        .build();
  }

  private static Entry encodeWriteSetEntry(
      TwoPhaseCommitParticipant.WriteSetEntry entry, String participantId) {
    Entry.Builder builder =
        Entry.newBuilder()
            .setNamespaceName(entry.getNamespaceName())
            .setTableName(entry.getTableName())
            .setPartitionKey(encodeKey(entry.getPartitionKey()))
            .setParticipantId(participantId);
    entry.getClusteringKey().ifPresent(ck -> builder.setClusteringKey(encodeKey(ck)));
    return builder.build();
  }

  /**
   * Merges the per-child, already-encoded write sets of a normal group commit into one parent-row
   * {@link WriteSet}, stamping every {@link EntryGroup} with its owning child id.
   *
   * <p>This is pure proto assembly over pre-encoded inputs: it neither reads a {@link Snapshot} nor
   * derives child ids (the caller resolves those), so it carries no dependency on the group-commit
   * key manipulator. {@code schema_version} is always set, matching {@link
   * #encodeSingleGroupWriteSet}, so a normal-group parent row and a delayed-group single row share
   * one schema version. Read-only children (a {@code null} or empty write set) contribute no entry
   * group.
   *
   * @param children each child's id paired with its pre-encoded write set, in emit order
   * @return the merged parent-row {@link WriteSet}
   */
  static WriteSet mergeChildWriteSets(List<ChildWriteSet> children) {
    EntryGroups.Builder entryGroups = EntryGroups.newBuilder();
    for (ChildWriteSet child : children) {
      if (child.writeSet == null) {
        continue;
      }
      // Every child is encoded in-process by this same encoder, so the payload case is always
      // ENTRY_GROUPS here. Unlike the reader in ConsensusCommitManager#finishTransaction, this can
      // never meet a row written by a newer binary; the assertion would only fail if the encoder
      // itself started emitting a second payload case, and then getEntryGroups would return an
      // empty default instance and drop that child's entries from the parent row in silence.
      assert child.writeSet.getPayloadCase() == WriteSet.PayloadCase.ENTRY_GROUPS;
      for (EntryGroup entryGroup : child.writeSet.getEntryGroups().getEntryGroupsList()) {
        entryGroups.addEntryGroups(entryGroup.toBuilder().setChildId(child.childId).build());
      }
    }
    return WriteSet.newBuilder()
        .setSchemaVersion(SCHEMA_VERSION)
        .setEntryGroups(entryGroups)
        .build();
  }

  /**
   * A child's id paired with its pre-encoded {@link WriteSet}, for {@link #mergeChildWriteSets}.
   */
  static final class ChildWriteSet {
    final String childId;
    @Nullable final WriteSet writeSet;

    ChildWriteSet(String childId, @Nullable WriteSet writeSet) {
      this.childId = childId;
      this.writeSet = writeSet;
    }
  }
}
