package com.scalar.db.transaction.consensuscommit.cbrl;

import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import javax.annotation.Nullable;

/**
 * One record-level write in the redo stream: a single {@link Entry} tagged with the transaction
 * that produced it ({@code txId}). The replay core consumes only {@code RedoOp}s and never how they
 * were sourced — the integration test builds them from real coordinator {@code tx_write_set} rows,
 * the property tests from a generator.
 *
 * <p>Operation type is derived (decision A in the PoC plan): a {@code WRITE} entry with no {@code
 * prev_tx_id} is an INSERT (chain root); with a {@code prev_tx_id} it is an UPDATE; a {@code
 * DELETE} entry is a delete. This rests on the encoder invariant that {@code prev_tx_id} is set iff
 * the record had a committed before-image.
 */
public final class RedoOp {
  private final RecordKey key;
  private final String txId;
  @Nullable private final String prevTxId;
  private final Entry entry;

  public RedoOp(String txId, Entry entry) {
    this.key = RecordKey.from(entry);
    this.txId = txId;
    this.prevTxId = entry.hasPrevTxId() ? entry.getPrevTxId() : null;
    this.entry = entry;
  }

  RecordKey key() {
    return key;
  }

  String txId() {
    return txId;
  }

  @Nullable
  String prevTxId() {
    return prevTxId;
  }

  Entry entry() {
    return entry;
  }

  boolean isDelete() {
    return entry.getEntryType() == Entry.EntryType.ENTRY_TYPE_DELETE;
  }

  boolean isInsert() {
    return !isDelete() && prevTxId == null;
  }

  boolean isUpdate() {
    return !isDelete() && prevTxId != null;
  }

  @Override
  public String toString() {
    return "RedoOp{"
        + (isDelete() ? "DELETE" : isInsert() ? "INSERT" : "UPDATE")
        + " tx="
        + txId
        + " prev="
        + prevTxId
        + " "
        + key
        + '}';
  }
}
