package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.util.groupcommit.GroupCommitKeyManipulator.Keys;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class Coordinator {
  public static final String NAMESPACE = "coordinator";
  public static final String TABLE = "state";
  public static final TableMetadata TABLE_METADATA_WITH_GROUP_COMMIT_DISABLED =
      TableMetadata.newBuilder()
          .addColumn(Attribute.ID, DataType.TEXT)
          .addColumn(Attribute.STATE, DataType.INT)
          .addColumn(Attribute.CREATED_AT, DataType.BIGINT)
          .addPartitionKey(Attribute.ID)
          .build();
  public static final TableMetadata TABLE_METADATA_WITH_GROUP_COMMIT_ENABLED =
      TableMetadata.newBuilder()
          .addColumn(Attribute.ID, DataType.TEXT)
          .addColumn(Attribute.CHILD_IDS, DataType.TEXT)
          .addColumn(Attribute.STATE, DataType.INT)
          .addColumn(Attribute.CREATED_AT, DataType.BIGINT)
          .addPartitionKey(Attribute.ID)
          .build();

  private static final int MAX_RETRY_COUNT = 5;
  private static final long SLEEP_BASE_MILLIS = 50;
  private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
  private final DistributedStorage storage;
  private final String coordinatorNamespace;
  private final CoordinatorGroupCommitKeyManipulator keyManipulator;

  /**
   * @param storage a storage
   * @deprecated As of release 3.3.0. Will be removed in release 5.0.0
   */
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Deprecated
  public Coordinator(DistributedStorage storage) {
    this.storage = storage;
    coordinatorNamespace = NAMESPACE;
    keyManipulator = new CoordinatorGroupCommitKeyManipulator();
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public Coordinator(DistributedStorage storage, ConsensusCommitConfig config) {
    this.storage = storage;
    coordinatorNamespace = config.getCoordinatorNamespace().orElse(NAMESPACE);
    keyManipulator = new CoordinatorGroupCommitKeyManipulator();
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
    if (keyManipulator.isFullKey(id)) {
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
    Keys<String, String, String> idForGroupCommit = keyManipulator.keysFromFullKey(fullId);

    String parentId = idForGroupCommit.parentKey;
    String childId = idForGroupCommit.childKey;
    Optional<State> state = getStateByParentId(parentId);
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

    return getStateByFullId(fullId);
  }

  private Optional<Coordinator.State> getStateInternal(String id) throws CoordinatorException {
    Get get = createGetWith(id);
    return get(get, id);
  }

  /**
   * Gets the coordinator state by the parent ID for the coordinator group commit. Note: The scope
   * of this method has public visibility, but is intended for internal use. Also, the method only
   * calls {@link #getStateInternal(String)} with the parent ID, but it exists as a separate method
   * for clarifying this specific use case.
   *
   * @param parentId the parent ID of the coordinator state for the coordinator group commit
   * @return the coordinator state
   * @throws CoordinatorException if the coordinator state cannot be retrieved
   */
  public Optional<Coordinator.State> getStateByParentId(String parentId)
      throws CoordinatorException {
    return getStateInternal(parentId);
  }

  /**
   * Gets the coordinator state by the full ID for the coordinator group commit. Note: The scope of
   * this method has public visibility, but is intended for internal use. Also, the method only
   * calls {@link #getStateInternal(String)} with the parent ID, but it exists as a separate method
   * for clarifying this specific use case.
   *
   * @param fullId the parent ID of the coordinator state for the coordinator group commit
   * @return the coordinator state
   * @throws CoordinatorException if the coordinator state cannot be retrieved
   */
  public Optional<Coordinator.State> getStateByFullId(String fullId) throws CoordinatorException {
    return getStateInternal(fullId);
  }

  public void putState(Coordinator.State state) throws CoordinatorException {
    Put put = createPutWith(state);
    put(put, state.getId());
  }

  void putStateForGroupCommit(
      String parentId, List<String> fullIds, TransactionState transactionState, long createdAt)
      throws CoordinatorException {

    if (keyManipulator.isFullKey(parentId)) {
      throw new AssertionError(
          "This method is only for normal group commits that use a parent ID as the key");
    }

    // Put the state that contains a parent ID as the key and multiple child transaction IDs.
    List<String> childIds = new ArrayList<>(fullIds.size());
    for (String fullId : fullIds) {
      Keys<String, String, String> keys = keyManipulator.keysFromFullKey(fullId);
      childIds.add(keys.childKey);
    }
    State state = new State(parentId, childIds, transactionState, createdAt);

    Put put = createPutWith(state);
    put(put, state.getId());
  }

  public void putStateForLazyRecoveryRollback(String id) throws CoordinatorException {
    if (keyManipulator.isFullKey(id)) {
      putStateForLazyRecoveryRollbackForGroupCommit(id);
      return;
    }

    putState(new Coordinator.State(id, TransactionState.ABORTED));
  }

  private void putStateForLazyRecoveryRollbackForGroupCommit(String id)
      throws CoordinatorException {
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
    // - The transaction is treated as committed since the commit `tx_id` is the transaction full
    // ID
    //
    // D. The original commit with `tx_id: <full tx ID>` is in-progress in case #b, and lazy
    // recovery happens first
    // - `lazy-recovery-abort-with-parent-id` succeeds
    // - (If the lazy recovery crashes here and the original commit happens, the situation will be
    // the same as C)
    // - `lazy-recovery-abort-with-full-id` succeeds
    // - The original commit with `tx_id: <full tx ID>` fails
    // - The transaction is treated as aborted because of `lazy-recovery-abort-with-full-id`
    Keys<String, String, String> keys = keyManipulator.keysFromFullKey(id);
    try {
      // This record is to prevent a group commit that has the same parent ID considering case #a
      // regardless if the transaction is actually in a group commit (case #a) or a delayed commit
      // (case #b).
      putStateForGroupCommit(
          keys.parentKey,
          Collections.emptyList(),
          TransactionState.ABORTED,
          System.currentTimeMillis());
    } catch (CoordinatorConflictException e) {
      // The group commit finished already, although there may be ongoing delayed groups.

      // If the group commit contains the transaction, follow the state.
      // Otherwise, continue to insert a record with the full ID.
      Optional<State> optState = getState(keys.parentKey);
      if (!optState.isPresent()) {
        throw new AssertionError();
      }
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
    // This record is to intend the transaction is aborted.
    putState(new Coordinator.State(id, TransactionState.ABORTED));
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

  private void exponentialBackoff(int counter) {
    Uninterruptibles.sleepUninterruptibly(
        (long) Math.pow(2, counter) * SLEEP_BASE_MILLIS, TimeUnit.MILLISECONDS);
  }

  @ThreadSafe
  public static class State {
    private static final List<String> EMPTY_CHILD_IDS = Collections.emptyList();
    private static final String CHILD_IDS_DELIMITER = ",";
    private final String id;
    private final TransactionState state;
    private final long createdAt;
    private final List<String> childIds;

    public State(Result result) throws CoordinatorException {
      checkNotMissingRequired(result);
      id = result.getText(Attribute.ID);
      state = TransactionState.getInstance(result.getInt(Attribute.STATE));
      createdAt = result.getBigInt(Attribute.CREATED_AT);

      if (result.contains(Attribute.CHILD_IDS)) {
        String childIdsStr = result.getText(Attribute.CHILD_IDS);
        if (childIdsStr != null) {
          childIds = Splitter.on(CHILD_IDS_DELIMITER).omitEmptyStrings().splitToList(childIdsStr);
        } else {
          childIds = EMPTY_CHILD_IDS;
        }
      } else {
        childIds = EMPTY_CHILD_IDS;
      }
    }

    public State(String id, TransactionState state) {
      this(id, state, System.currentTimeMillis());
    }

    public State(String id, List<String> childIds, TransactionState state) {
      this(id, childIds, state, System.currentTimeMillis());
    }

    @VisibleForTesting
    State(String id, List<String> childIds, TransactionState state, long createdAt) {
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
      this.state = checkNotNull(state);
      this.createdAt = createdAt;
    }

    @VisibleForTesting
    State(String id, TransactionState state, long createdAt) {
      this(id, EMPTY_CHILD_IDS, state, createdAt);
    }

    @Nonnull
    public String getId() {
      return id;
    }

    @Nonnull
    public TransactionState getState() {
      return state;
    }

    public long getCreatedAt() {
      return createdAt;
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

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof State)) {
        return false;
      }
      State other = (State) o;
      // NOTICE: createdAt is not taken into account
      return Objects.equals(id, other.id)
          && state == other.state
          && Objects.equals(childIds, other.childIds);
    }

    @Override
    public int hashCode() {
      // NOTICE: createdAt is not taken into account
      return Objects.hash(id, state, childIds);
    }

    private void checkNotMissingRequired(Result result) throws CoordinatorException {
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
