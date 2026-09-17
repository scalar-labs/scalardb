package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * The value buffered in a Coordinator group-commit slot: a transaction's full id and its
 * pre-encoded {@link WriteSet}.
 *
 * <p>The {@link com.scalar.db.util.groupcommit.Emittable} callback receives only the buffered
 * values, not the slot keys, so {@code fullId} is carried here to let the emitter derive each
 * member's child id.
 */
@Immutable
public class CoordinatorGroupCommitValue {
  public final String fullId;
  @Nullable public final WriteSet writeSet;

  public CoordinatorGroupCommitValue(String fullId, @Nullable WriteSet writeSet) {
    this.fullId = fullId;
    this.writeSet = writeSet;
  }
}
