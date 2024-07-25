package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.util.groupcommit.KeyManipulator.Keys;
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
  public static final TableMetadata TABLE_METADATA =
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

  public Optional<Coordinator.State> getState(String id) throws CoordinatorException {
    if (keyManipulator.isFullKey(id)) {
      return getStateForGroupCommit(id);
    }

    Get get = createGetWith(id);
    return get(get);
  }

  @VisibleForTesting
  Optional<Coordinator.State> getStateForGroupCommit(String fullId) throws CoordinatorException {
    // Reading a coordinator state is likely to occur during lazy recovery, as follows:
    // 1. Transaction T1 starts and creates PREPARED state records but hasn't committed or aborted
    //    yet.
    // 2. Transaction T2 starts and reads the PREPARED state records created by T1.
    // 3. T2 reads the coordinator table record for T1 to decide whether to roll back or roll
    //    forward.
    //
    // The likelihood of step 2 would increase if T1 is delayed.
    //
    // With the group commit feature enabled, delayed transactions are isolated from a normal group
    // that is looked up by a parent ID into a delayed group that is looked up by a full ID.
    // Therefore, looking up with the full transaction ID should be tried first to minimize read
    // operations as much as possible.

    // Scan with the full ID for a delayed group that contains only a single transaction.
    // The normal lookup logic can be used as is.
    Optional<State> stateOfDelayedTxn = get(createGetWith(fullId));
    if (stateOfDelayedTxn.isPresent()) {
      return stateOfDelayedTxn;
    }

    // Scan with the parent ID for a normal group that contains multiple transactions.
    Keys<String, String, String> idForGroupCommit = keyManipulator.keysFromFullKey(fullId);

    String parentId = idForGroupCommit.parentKey;
    String childId = idForGroupCommit.childKey;
    Get get = createGetWith(parentId);
    Optional<State> state = get(get);
    return state.flatMap(
        s -> {
          if (s.getChildIds().contains(childId)) {
            return state;
          }
          return Optional.empty();
        });
  }

  public void putState(Coordinator.State state) throws CoordinatorException {
    Put put = createPutWith(state);
    put(put);
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
    put(put);
  }

  public void putStateForLazyRecoveryRollback(String id) throws CoordinatorException {
    if (!keyManipulator.isFullKey(id)) {
      putState(new Coordinator.State(id, TransactionState.ABORTED));
      return;
    }

    Keys<String, String, String> keys = keyManipulator.keysFromFullKey(id);
    // This record is to prevent a group commit that has the same parent ID regardless if the
    // transaction is group committed or committed alone.
    putStateForGroupCommit(
        keys.parentKey,
        Collections.emptyList(),
        TransactionState.ABORTED,
        System.currentTimeMillis());
    // This record is to clarify the transaction is aborted.
    putState(new Coordinator.State(id, TransactionState.ABORTED));
  }

  private Get createGetWith(String id) {
    return new Get(new Key(Attribute.toIdValue(id)))
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(coordinatorNamespace)
        .forTable(TABLE);
  }

  private Optional<Coordinator.State> get(Get get) throws CoordinatorException {
    int counter = 0;
    while (true) {
      if (counter >= MAX_RETRY_COUNT) {
        throw new CoordinatorException("Can't get coordinator state");
      }
      try {
        Optional<Result> result = storage.get(get);
        if (result.isPresent()) {
          return Optional.of(new State(result.get()));
        } else {
          return Optional.empty();
        }
      } catch (ExecutionException e) {
        logger.warn("Can't get coordinator state", e);
      }
      exponentialBackoff(counter++);
    }
  }

  @VisibleForTesting
  Put createPutWith(Coordinator.State state) {
    Put put = new Put(new Key(Attribute.toIdValue(state.getId())));
    String childIds = state.getChildIdsAsString();
    if (!childIds.isEmpty()) {
      put.withValue(Attribute.toChildIdsValue(childIds));
    }
    return put.withValue(Attribute.toStateValue(state.getState()))
        .withValue(Attribute.toCreatedAtValue(state.getCreatedAt()))
        .withConsistency(Consistency.LINEARIZABLE)
        .withCondition(new PutIfNotExists())
        .forNamespace(coordinatorNamespace)
        .forTable(TABLE);
  }

  private void put(Put put) throws CoordinatorException {
    int counter = 0;
    while (true) {
      if (counter >= MAX_RETRY_COUNT) {
        throw new CoordinatorException("Couldn't put coordinator state");
      }
      try {
        storage.put(put);
        break;
      } catch (NoMutationException e) {
        throw new CoordinatorConflictException("Mutation seems applied already", e);
      } catch (ExecutionException e) {
        logger.warn("Putting state in coordinator failed", e);
      }
      exponentialBackoff(counter++);
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
      id = result.getValue(Attribute.ID).get().getAsString().get();
      state = TransactionState.getInstance(result.getValue(Attribute.STATE).get().getAsInt());
      createdAt = result.getValue(Attribute.CREATED_AT).get().getAsLong();
      Optional<Value<?>> childIdsOpt = result.getValue(Attribute.CHILD_IDS);
      Optional<String> childIdsStrOpt;
      if (childIdsOpt.isPresent()) {
        childIdsStrOpt = childIdsOpt.get().getAsString();
      } else {
        childIdsStrOpt = Optional.empty();
      }
      childIds =
          childIdsStrOpt
              .map(s -> Splitter.on(CHILD_IDS_DELIMITER).omitEmptyStrings().splitToList(s))
              .orElse(EMPTY_CHILD_IDS);
    }

    public State(String id, TransactionState state) {
      this(id, state, System.currentTimeMillis());
    }

    // For the SpotBugs warning CT_CONSTRUCTOR_THROW
    @Override
    protected final void finalize() {}

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

    @VisibleForTesting
    List<String> getChildIds() {
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
      if (!result.getValue(Attribute.ID).isPresent()
          || !result.getValue(Attribute.ID).get().getAsString().isPresent()) {
        throw new CoordinatorException("id is missing in the coordinator state");
      }
      if (!result.getValue(Attribute.STATE).isPresent()
          || result.getValue(Attribute.STATE).get().getAsInt() == 0) {
        throw new CoordinatorException("state is missing in the coordinator state");
      }
      if (!result.getValue(Attribute.CREATED_AT).isPresent()
          || result.getValue(Attribute.CREATED_AT).get().getAsLong() == 0) {
        throw new CoordinatorException("created_at is missing in the coordinator state");
      }
    }
  }
}
