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
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.util.groupcommit.KeyManipulator.Keys;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
  private final CoordinatorGroupCommitKeyManipulator coordinatorGroupCommitKeyManipulator;

  /**
   * @param storage a storage
   * @deprecated As of release 3.3.0. Will be removed in release 5.0.0
   */
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Deprecated
  public Coordinator(DistributedStorage storage) {
    this.storage = storage;
    coordinatorNamespace = NAMESPACE;
    coordinatorGroupCommitKeyManipulator = new CoordinatorGroupCommitKeyManipulator();
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public Coordinator(DistributedStorage storage, ConsensusCommitConfig config) {
    this.storage = storage;
    coordinatorNamespace = config.getCoordinatorNamespace().orElse(NAMESPACE);
    coordinatorGroupCommitKeyManipulator = new CoordinatorGroupCommitKeyManipulator();
  }

  public Optional<Coordinator.State> getState(String id) throws CoordinatorException {
    if (coordinatorGroupCommitKeyManipulator.isFullKey(id)) {
      // When dealing with transaction IDs for group commit, the process of checking two transaction
      // ID formats is executed non-atomically.
      // Consequently, there is a possibility of transaction ID insertion occurring between the two
      // read operations.
      // However, this scenario arises only when the checking of the two transaction ID formats
      // and the insertion of the transaction ID overlap with different transactions, ensuring it
      // does not violate linearizability.

      // Scan with the parent ID for group committed record.
      Optional<State> state = getStateForGroupCommit(id);
      if (state.isPresent()) {
        return state;
      }
      // Scan with the full ID for single transaction record.
      return get(createGetWith(id));
    }

    Get get = createGetWith(id);
    return get(get);
  }

  @VisibleForTesting
  Optional<Coordinator.State> getStateForGroupCommit(String id) throws CoordinatorException {
    if (!coordinatorGroupCommitKeyManipulator.isFullKey(id)) {
      throw new IllegalArgumentException("This id format isn't for group commit. Id:" + id);
    }
    Keys<String, String, String> idForGroupCommit =
        coordinatorGroupCommitKeyManipulator.keysFromFullKey(id);

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
      String id, List<String> fullIds, TransactionState transactionState, long createdAt)
      throws CoordinatorException {
    State state;
    if (coordinatorGroupCommitKeyManipulator.isFullKey(id)) {
      // In this case, this is same as normal commit and child_ids isn't needed.
      state = new State(id, Collections.emptyList(), transactionState, createdAt);
    } else {
      // Group commit with child_ids.
      state = new State(id, fullIds, transactionState, createdAt);
    }
    Put put = createPutWith(state);
    put(put);
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
    return new Put(new Key(Attribute.toIdValue(state.getId())))
        .withValue(Attribute.toChildIdsValue(Joiner.on(',').join(state.getChildIds())))
        .withValue(Attribute.toStateValue(state.getState()))
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
    private final String id;
    private final TransactionState state;
    private final long createdAt;
    private final List<String> childIds;

    public State(Result result) throws CoordinatorException {
      checkNotMissingRequired(result);
      id = result.getValue(Attribute.ID).get().getAsString().get();
      state = TransactionState.getInstance(result.getValue(Attribute.STATE).get().getAsInt());
      createdAt = result.getValue(Attribute.CREATED_AT).get().getAsLong();
      Optional<String> childIdsOpt = result.getValue(Attribute.CHILD_IDS).get().getAsString();
      childIds =
          childIdsOpt
              .map(s -> Splitter.on(',').omitEmptyStrings().splitToList(s))
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

    public List<String> getChildIds() {
      return childIds;
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
