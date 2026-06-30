package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.InvalidProtocolBufferException;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import com.scalar.db.util.groupcommit.GroupCommitKeyManipulator.Keys;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the Coordinator table, the single source of truth for each transaction's final state
 * ({@link TransactionState#COMMITTED} or {@link TransactionState#ABORTED}). Every coordinator-state
 * write in the system funnels through this class, so the conflict-safety of the whole protocol is
 * decided here. All writes are conditional (put-if-not-exists on the {@code tx_id} partition key),
 * so two writers targeting the same key race and exactly one wins with a {@link
 * CoordinatorConflictException}; the loser then reads the persisted state and follows it.
 *
 * <h3>Key scheme</h3>
 *
 * The {@code tx_id} partition key is either a plain transaction ID or, when coordinator group
 * commit is enabled, a group-commit key distinguished by {@code KEY_MANIPULATOR.isFullKey(id)}:
 *
 * <ul>
 *   <li><b>parent ID</b> — the key for a <i>normal group</i>: one record under the parent ID lists
 *       the child IDs of all transactions committed together (written by {@code
 *       CommitHandlerWithGroupCommit.Emitter#emitNormalGroup} via {@link #putState}).
 *   <li><b>full ID</b> ({@code parent + child}) — the key for a transaction committed on its own: a
 *       <i>delayed</i> group commit, or a transaction whose lifecycle abort writes under its own
 *       full ID.
 * </ul>
 *
 * <h3>Write paths (non-group-commit)</h3>
 *
 * When group commit is disabled, every {@code tx_id} is a plain ID and there is only one possible
 * key per transaction, so a single {@link #putState} is always safe:
 *
 * <ul>
 *   <li>commit / abort during a transaction's own lifecycle — {@code CommitHandler.commitState} /
 *       {@code abortState} write {@code putState(txId, COMMITTED|ABORTED)}.
 *   <li>lazy recovery and manager-level rollback/abort by ID — {@link #forceAbort} takes the
 *       non-full branch and writes {@code putState(txId, ABORTED)}.
 * </ul>
 *
 * <h3>Write paths (group commit)</h3>
 *
 * With group commit enabled a transaction may be committed under <i>either</i> key, so an abort
 * must be careful not to write under a key that does not conflict with the commit:
 *
 * <ul>
 *   <li><b>normal group commit</b> — {@code CommitHandlerWithGroupCommit.Emitter#emitNormalGroup}
 *       writes {@code COMMITTED} under the parent ID via {@link #putState}.
 *   <li><b>delayed group commit</b> — {@code CommitHandlerWithGroupCommit.Emitter#emitDelayedGroup}
 *       writes {@code putState(fullId, COMMITTED)}.
 *   <li><b>lifecycle abort</b> ({@link CommitHandlerWithGroupCommit#abortState}) — writes {@code
 *       putState(fullId, ABORTED)}. Safe because the caller first removes the slot from the group
 *       committer (so a normal group commit can no longer list this child under the parent ID), and
 *       against a delayed commit it conflicts on the same full ID.
 *   <li><b>lazy recovery / manager-level rollback-or-abort by ID</b> — {@link #forceAbort} runs the
 *       two-step protocol: write the parent-ID conflict marker (empty {@code tx_child_ids}) first,
 *       then the full-ID {@code ABORTED} record. Needed because the caller does <i>not</i> own the
 *       slot and cannot remove it, so an in-flight normal group commit could still write {@code
 *       COMMITTED} under the parent ID. See {@link #forceAbort} for the full case analysis.
 * </ul>
 *
 * <h3>Invariant for a plain single-{@code putState} abort</h3>
 *
 * Writing {@code ABORTED} with one {@link #putState} is correct only when no concurrent group
 * commit can write {@code COMMITTED} under the parent ID for that transaction — i.e. <i>either</i>
 * the ID is not a group-commit full key (group commit disabled, or the two-phase-commit interface,
 * which forbids group commit), <i>or</i> the caller has already removed the slot from the group
 * committer (the lifecycle-abort path above). When neither holds (a manager-level abort/rollback by
 * ID), the two-step {@link #forceAbort} must be used instead.
 */
@ThreadSafe
public class Coordinator {
  public static final String NAMESPACE = "coordinator";
  public static final String TABLE = "state";
  public static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(Attribute.ID, DataType.TEXT)
          .addColumn(Attribute.CHILD_IDS, DataType.TEXT)
          .addColumn(Attribute.WRITE_SET, DataType.BLOB)
          .addColumn(Attribute.STATE, DataType.INT)
          .addColumn(Attribute.CREATED_AT, DataType.BIGINT)
          .addPartitionKey(Attribute.ID)
          .build();

  private static final int MAX_RETRY_COUNT = 5;
  private static final long SLEEP_BASE_MILLIS = 50;
  private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
  private static final CoordinatorGroupCommitKeyManipulator KEY_MANIPULATOR =
      new CoordinatorGroupCommitKeyManipulator();

  private final DistributedStorage storage;
  private final String coordinatorNamespace;

  /**
   * @param storage a storage
   * @deprecated As of release 3.3.0. Will be removed in release 4.0.0
   */
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Deprecated
  public Coordinator(DistributedStorage storage) {
    this.storage = storage;
    coordinatorNamespace = NAMESPACE;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public Coordinator(DistributedStorage storage, ConsensusCommitConfig config) {
    this.storage = storage;
    coordinatorNamespace = config.getCoordinatorNamespace().orElse(NAMESPACE);
  }

  /**
   * Gets the coordinator state by ID. If the ID is a full ID for the coordinator group commit, it
   * will look up the state using the parent ID and the child ID. Otherwise, it will look up the
   * state only by ID.
   *
   * @param id the ID of the coordinator state
   * @return the coordinator state
   * @throws CoordinatorException if the coordinator state cannot be retrieved
   */
  public Optional<Coordinator.State> getState(String id) throws CoordinatorException {
    if (KEY_MANIPULATOR.isFullKey(id)) {
      return getStateForGroupCommit(id);
    }

    return getStateInternal(id);
  }

  /**
   * Gets the coordinator state for a group commit by ID. It first looks up the state using the
   * parent ID and then checks if the child ID is contained in the state. If the child ID is not
   * found, it will look up the state using the full ID.
   *
   * @param fullId the full ID for the coordinator group commit
   * @return the coordinator state
   * @throws CoordinatorException if the coordinator state cannot be retrieved
   */
  @VisibleForTesting
  Optional<Coordinator.State> getStateForGroupCommit(String fullId) throws CoordinatorException {
    // Scan with the parent ID for a normal group that contains multiple transactions.
    Keys<String, String, String> idForGroupCommit = KEY_MANIPULATOR.keysFromFullKey(fullId);

    String parentId = idForGroupCommit.parentKey;
    String childId = idForGroupCommit.childKey;
    Optional<State> state = getStateInternal(parentId);
    // The current implementation is optimized for cases where most transactions are
    // group-committed. It first looks up a transaction state using the parent ID with a single read
    // operation. If no matching transaction state is found (i.e., the transaction was delayed and
    // committed individually), it issues an additional read operation using the full ID.
    Optional<State> stateContainingTargetTxId =
        state.flatMap(
            s -> {
              if (s.getChildIds().contains(childId)) {
                return state;
              }
              return Optional.empty();
            });
    if (stateContainingTargetTxId.isPresent()) {
      return stateContainingTargetTxId;
    }

    return getStateInternal(fullId);
  }

  private Optional<Coordinator.State> getStateInternal(String id) throws CoordinatorException {
    Get get = createGetWith(id);
    return get(get, id);
  }

  public void putState(Coordinator.State state) throws CoordinatorException {
    Put put = createPutWith(state);
    put(put, state.getId());
  }

  public void forceAbort(String id) throws CoordinatorException {
    if (KEY_MANIPULATOR.isFullKey(id)) {
      forceAbortForGroupCommit(id);
      return;
    }

    putState(new Coordinator.State(id, TransactionState.ABORTED, System.currentTimeMillis()));
  }

  private void forceAbortForGroupCommit(String id) throws CoordinatorException {
    // Lazy recoveries don't know which the transaction that created the PREPARE record is using, a
    // parent ID or a full ID as `tx_id` partition key.
    //
    // Case a) If a transaction becomes "ready for commit" in time, it'll be committed in a group
    // with `tx_id: <parent tx ID>`.
    // Case b) If a transaction is delayed, it'll be committed in an isolated group with a full ID
    // as `tx_id: <full tx ID>`.
    //
    // If lazy recoveries only insert a record with `tx_id: <full tx ID>` to abort the transaction,
    // it will not conflict with the group commit using `tx_id: <parent tx ID>` in case #a.
    // Therefore, lazy recoveries first need to insert a record with `tx_id: <parent tx ID>` and
    // empty `tx_child_ids` to the Coordinator table. We'll call this insertion
    // `lazy-recovery-abort-with-parent-id`. This record is intended to conflict with a potential
    // group commit considering case#1, even though it doesn't help in finding the coordinator state
    // since `tx_child_ids` is empty.
    //
    // Once the record insertion with `tx_id: <parent tx ID>` succeeds, the lazy recovery will
    // insert another record with `tx_id: <full tx ID>`. We'll call this insertion
    // `lazy-recovery-abort-with-full-id`. This record insertion is needed to conflict with a
    // potential delayed group commit that has `tx_id: <full tx ID>` in case #b, and indicates the
    // transaction is aborted.
    //
    // Let's walk through all the cases.
    //
    // A. The original commit with `tx_id: <parent tx ID>` succeeds in case #a, and then lazy
    // recovery happens
    // - The original commit with `tx_id: <parent tx ID>` succeeds
    // - `lazy-recovery-abort-with-parent-id` fails
    // - The transaction is treated as committed since the commit's `tx_child_ids` contains the
    // transaction child ID
    //
    // B. The original commit with `tx_id: <parent tx ID>` is in-progress in case #a, and lazy
    // recovery happens first
    // - `lazy-recovery-abort-with-parent-id` succeeds
    // - The original commit with `tx_id: <parent tx ID>` fails
    // - (If the lazy recovery crashes here, another lazy recovery will insert the below
    // `lazy-recovery-abort-with-full-id` later)
    // - `lazy-recovery-abort-with-full-id` succeeds
    // - The transaction is treated as aborted because of `lazy-recovery-abort-with-full-id`
    //
    // C. The original commit with `tx_id: <full tx ID>` is done in case #b, and then lazy recovery
    // happens
    // - The original commit with `tx_id: <full tx ID>` succeeds
    // - `lazy-recovery-abort-with-parent-id` succeeds
    // - `lazy-recovery-abort-with-full-id` fails
    // - The transaction is treated as committed since the commit `tx_id` is the transaction full ID
    //
    // D. The original commit with `tx_id: <full tx ID>` is in-progress in case #b, and lazy
    // recovery happens first
    // - `lazy-recovery-abort-with-parent-id` succeeds
    // - (If the lazy recovery crashes here and the original commit happens, the situation will be
    // the same as C)
    // - `lazy-recovery-abort-with-full-id` succeeds
    // - The original commit with `tx_id: <full tx ID>` fails
    // - The transaction is treated as aborted because of `lazy-recovery-abort-with-full-id`
    Keys<String, String, String> keys = KEY_MANIPULATOR.keysFromFullKey(id);
    try {
      // This record is to prevent a group commit that has the same parent ID considering case #a
      // regardless if the transaction is actually in a group commit (case #a) or a delayed commit
      // (case #b). The parent-id row carries no childIds because lazy recovery has no membership
      // info to record; it only needs the put-if-not-exists conflict against an in-flight normal
      // group commit.
      putState(new State(keys.parentKey, TransactionState.ABORTED, System.currentTimeMillis()));
    } catch (CoordinatorConflictException e) {
      // The group commit finished already, although there may be ongoing delayed groups.

      // If the group commit contains the transaction, follow the state.
      // Otherwise, continue to insert a record with the full ID.
      //
      // The parent row can also be absent here: the Coordinator state cleanup process removed an
      // already-finished group-commit row in the small window after our insert conflicted (the row
      // existed at insert time, which is why the insert conflicted). This is no longer an invariant
      // violation, so we fall through to insert the full-ID record -- the same path taken when the
      // parent row is present but does not contain this child. If the transaction actually
      // committed via a full-ID record (a delayed, standalone commit), that insert conflicts and
      // the caller re-reads the committed outcome instead of treating the transaction as aborted.
      // If instead it committed via the parent row (a normal group commit) whose row was then
      // cleaned up, no full-ID record exists, so the insert succeeds and writes a now-spurious
      // full-ID ABORTED. This is non-corrupting -- the record was already finalized to its
      // committed value, so a subsequent rollback is a conditional no-op -- and is the same
      // accepted residual as the read path's pre-abort/abort cleanup race (the spurious ABORTED is
      // reclaimed later by the Coordinator state cleanup process). getState for such a finished,
      // cleaned-up transaction is already documented as indeterminate.
      Optional<State> optState = getState(keys.parentKey);
      if (optState.isPresent()) {
        State state = optState.get();
        if (state.getChildIds().contains(keys.childKey)) {
          if (state.getState() == TransactionState.ABORTED) {
            return;
          } else {
            // Conflicted.
            throw e;
          }
        }
      }
    }

    // This record is to intend the transaction is aborted.
    putState(new Coordinator.State(id, TransactionState.ABORTED, System.currentTimeMillis()));
  }

  @VisibleForTesting
  Get createGetWith(String id) {
    return Get.newBuilder()
        .namespace(coordinatorNamespace)
        .table(TABLE)
        .partitionKey(Key.ofText(Attribute.ID, id))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private Optional<Coordinator.State> get(Get get, String id) throws CoordinatorException {
    int counter = 0;
    Exception exception = null;
    while (true) {
      try {
        Optional<Result> result = storage.get(get);
        if (result.isPresent()) {
          return Optional.of(new State(result.get()));
        } else {
          return Optional.empty();
        }
      } catch (Exception e) {
        if (exception == null) {
          exception = e;
        } else {
          exception.addSuppressed(e);
        }

        if (counter + 1 >= MAX_RETRY_COUNT) {
          throw new CoordinatorException("Can't get coordinator state", exception);
        }

        logger.warn(
            "Can't get coordinator state. Retrying... Attempt: {}; Transaction ID: {}",
            counter,
            id,
            e);

        exponentialBackoff(counter++);
      }
    }
  }

  @VisibleForTesting
  Put createPutWith(Coordinator.State state) {
    String childIds = state.getChildIdsAsString();
    PutBuilder.Buildable builder =
        Put.newBuilder()
            .namespace(coordinatorNamespace)
            .table(TABLE)
            .partitionKey(Key.ofText(Attribute.ID, state.getId()))
            .intValue(Attribute.STATE, state.getState().get())
            .bigIntValue(Attribute.CREATED_AT, state.getCreatedAt())
            .consistency(Consistency.LINEARIZABLE)
            .condition(ConditionBuilder.putIfNotExists());

    if (!childIds.isEmpty()) {
      builder = builder.textValue(Attribute.CHILD_IDS, childIds);
    }
    if (state.getWriteSet().isPresent()) {
      builder = builder.blobValue(Attribute.WRITE_SET, state.getWriteSet().get().toByteArray());
    }
    return builder.build();
  }

  private void put(Put put, String id) throws CoordinatorException {
    int counter = 0;
    Exception exception = null;
    while (true) {
      try {
        storage.put(put);
        break;
      } catch (NoMutationException e) {
        throw new CoordinatorConflictException("Mutation seems applied already", e);
      } catch (Exception e) {
        if (exception == null) {
          exception = e;
        } else {
          exception.addSuppressed(e);
        }

        if (counter + 1 >= MAX_RETRY_COUNT) {
          throw new CoordinatorException("Couldn't put coordinator state", exception);
        }

        logger.warn(
            "Putting state in coordinator failed. Retrying... Attempt: {}; Transaction ID: {}",
            counter,
            id,
            e);

        exponentialBackoff(counter++);
      }
    }
  }

  /**
   * Deletes the coordinator state row for the given transaction id.
   *
   * <p>If {@code id} is a group-commit full key, the actual row may live under the parent key (when
   * the transaction was committed in a normal group) or under the full key (when it was
   * delayed-committed or had been forced-aborted under the full key). To stay symmetric with {@link
   * #getState(String)}, this method internally resolves the row location: when {@code id} is a full
   * key, it looks up the actual row via {@code getState} and deletes whichever key {@link
   * State#getId} returns. Non-full ids are deleted at their literal partition key.
   *
   * <p>The delete is unconditional — if the row is already gone (concurrent cleanup, no row ever
   * existed) this is a benign no-op at the storage layer and no exception is propagated. Storage
   * errors surface as {@link CoordinatorException}; the caller is itself a retryable API, so no
   * internal retry is performed here.
   *
   * @param id the transaction id whose state row should be deleted
   * @throws CoordinatorException if the storage layer fails to delete the row
   */
  public void deleteState(String id) throws CoordinatorException {
    String actualKey;
    if (KEY_MANIPULATOR.isFullKey(id)) {
      // For a full key, resolve to whichever row actually holds this transaction's state. When the
      // tx was committed in a normal group, state.getId() is the parent key; when delayed or
      // forced-aborted under the full key, it is the full key. If no row exists, fall through and
      // attempt the literal delete (which is a no-op against storage).
      Optional<State> state = getState(id);
      actualKey = state.map(State::getId).orElse(id);
    } else {
      actualKey = id;
    }
    Delete delete =
        Delete.newBuilder()
            .namespace(coordinatorNamespace)
            .table(TABLE)
            .partitionKey(Key.ofText(Attribute.ID, actualKey))
            .consistency(Consistency.LINEARIZABLE)
            .build();
    try {
      storage.delete(delete);
    } catch (ExecutionException e) {
      throw new CoordinatorException("Couldn't delete coordinator state", e);
    }
  }

  private void exponentialBackoff(int counter) {
    Uninterruptibles.sleepUninterruptibly(
        (long) Math.pow(2, counter) * SLEEP_BASE_MILLIS, TimeUnit.MILLISECONDS);
  }

  @ThreadSafe
  public static class State {
    private static final List<String> EMPTY_CHILD_IDS = Collections.emptyList();
    private static final String CHILD_IDS_DELIMITER = ",";
    private final String id;
    private final List<String> childIds;
    @Nullable private final WriteSet writeSet;
    private final TransactionState state;
    private final long createdAt;

    public State(Result result) throws CoordinatorException {
      checkNotMissingRequired(result);
      id = result.getText(Attribute.ID);
      String childIdsStr = result.getText(Attribute.CHILD_IDS);
      if (childIdsStr != null) {
        childIds = Splitter.on(CHILD_IDS_DELIMITER).omitEmptyStrings().splitToList(childIdsStr);
      } else {
        childIds = EMPTY_CHILD_IDS;
      }
      writeSet = parseWriteSet(result);
      state = TransactionState.getInstance(result.getInt(Attribute.STATE));
      createdAt = result.getBigInt(Attribute.CREATED_AT);
    }

    public State(String id, TransactionState state, long createdAt) {
      this(id, EMPTY_CHILD_IDS, null, state, createdAt);
    }

    public State(String id, @Nullable WriteSet writeSet, TransactionState state, long createdAt) {
      this(id, EMPTY_CHILD_IDS, writeSet, state, createdAt);
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public State(
        String id,
        List<String> childIds,
        @Nullable WriteSet writeSet,
        TransactionState state,
        long createdAt) {
      this.id = checkNotNull(id);
      for (String childId : childIds) {
        if (childId.contains(CHILD_IDS_DELIMITER)) {
          throw new IllegalArgumentException(
              String.format(
                  "This child transaction ID itself contains the delimiter. ChildTransactionID: %s, Delimiter: %s",
                  childId, CHILD_IDS_DELIMITER));
        }
      }
      this.childIds = childIds;
      this.writeSet = writeSet;
      this.state = checkNotNull(state);
      this.createdAt = createdAt;
    }

    @Nonnull
    public String getId() {
      return id;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    @Nonnull
    public List<String> getChildIds() {
      return childIds;
    }

    @VisibleForTesting
    String getChildIdsAsString() {
      return Joiner.on(CHILD_IDS_DELIMITER).join(childIds);
    }

    /**
     * Returns the write set persisted in the {@code tx_write_set} column, if any.
     *
     * <p>Returns {@link Optional#empty()} for rows written before the {@code tx_write_set} feature
     * was introduced and for ABORTED rows written by the lazy-recovery / manager-level rollback
     * paths (which do not have access to the original transaction's writes). For read-only
     * transactions, returns an {@code Optional} containing a {@link WriteSet} with {@code
     * schema_version = 1} and an empty {@code entry_groups} list — this is distinct from {@link
     * Optional#empty()} and lets callers distinguish "no info recorded" from "transaction had no
     * writes".
     *
     * @return the persisted write set, or empty if none is recorded for this row
     */
    @Nonnull
    public Optional<WriteSet> getWriteSet() {
      return Optional.ofNullable(writeSet);
    }

    @Nonnull
    public TransactionState getState() {
      return state;
    }

    public long getCreatedAt() {
      return createdAt;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof State)) {
        return false;
      }
      State other = (State) o;
      return Objects.equals(id, other.id)
          && Objects.equals(childIds, other.childIds)
          && Objects.equals(writeSet, other.writeSet)
          && state == other.state
          && createdAt == other.createdAt;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, childIds, writeSet, state, createdAt);
    }

    @Nullable
    private static WriteSet parseWriteSet(Result result) throws CoordinatorException {
      if (result.isNull(Attribute.WRITE_SET)) {
        // The column is not set: the row was written before this feature, by the lazy-recovery
        // abort path, or on a deployment where the feature is disabled. The reader treats this as
        // "no info" and falls back to per-record lazy recovery.
        return null;
      }
      // The column is set. The bytes always contain at least the schema_version field, so an
      // empty (no entry_groups) WriteSet still parses as a non-null proto and is distinguishable
      // from the NULL case above.
      try {
        return WriteSet.parseFrom(result.getBlobAsBytes(Attribute.WRITE_SET));
      } catch (InvalidProtocolBufferException e) {
        throw new CoordinatorException(
            "Failed to parse the write set. Transaction ID: " + result.getText(Attribute.ID), e);
      }
    }

    private static void checkNotMissingRequired(Result result) throws CoordinatorException {
      if (result.isNull(Attribute.ID) || result.getText(Attribute.ID) == null) {
        throw new CoordinatorException("id is missing in the coordinator state");
      }
      if (result.isNull(Attribute.STATE) || result.getInt(Attribute.STATE) == 0) {
        throw new CoordinatorException("state is missing in the coordinator state");
      }
      if (result.isNull(Attribute.CREATED_AT) || result.getBigInt(Attribute.CREATED_AT) == 0) {
        throw new CoordinatorException("created_at is missing in the coordinator state");
      }
    }
  }
}
