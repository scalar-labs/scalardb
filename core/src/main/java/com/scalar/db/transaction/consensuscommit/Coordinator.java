package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
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
import com.scalar.db.io.TextValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.Collection;
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
          .addColumn(Attribute.STATE, DataType.INT)
          .addColumn(Attribute.CREATED_AT, DataType.BIGINT)
          // For PoC
          .addColumn("child_ids", DataType.TEXT)
          .addPartitionKey(Attribute.ID)
          .build();

  private static final int MAX_RETRY_COUNT = 5;
  private static final long SLEEP_BASE_MILLIS = 50;
  private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
  private final DistributedStorage storage;
  private final String coordinatorNamespace;

  /**
   * @param storage a storage
   * @deprecated As of release 3.3.0. Will be removed in release 5.0.0
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

  private static class IdForGroupCommit {
    public final String parentId;
    public final String childId;

    public IdForGroupCommit(String parentId, String childId) {
      this.parentId = parentId;
      this.childId = childId;
    }
  }

  private boolean isIdForGroupCommit(String id) {
    // TODO: Move these logics to an expert class
    return id.contains(":");
  }

  private IdForGroupCommit idForGroupCommit(String id) {
    if (!isIdForGroupCommit(id)) {
      throw new IllegalArgumentException("'id' isn't for group commit. id:" + id);
    }
    String[] parts = id.split(":");
    return new IdForGroupCommit(parts[0], parts[1]);
  }

  public Optional<Coordinator.State> getState(String id) throws CoordinatorException {
    if (isIdForGroupCommit(id)) {
      Get get = createGetWith(id);
      Optional<State> state = get(get);
      if (state.isPresent()) {
        return state;
      }
      return getStateForGroupCommit(id);
    }

    Get get = createGetWith(id);
    return get(get);
  }

  public Optional<Coordinator.State> getStateForGroupCommit(String id) throws CoordinatorException {
    IdForGroupCommit idForGroupCommit = idForGroupCommit(id);

    String parentId = idForGroupCommit.parentId;
    String childId = idForGroupCommit.childId;
    Get get = createGetWith(parentId);
    Optional<State> state = get(get);
    return state.flatMap(
        s -> {
          if (!s.getState().equals(TransactionState.COMMITTED)) {
            return state;
          }

          boolean isTheChildIdContained = Arrays.asList(s.childIds).contains(childId);
          if (isTheChildIdContained) {
            return state;
          } else {
            State partiallyCommittedState = state.get();
            return Optional.of(
                new State(
                    partiallyCommittedState.getId(),
                    TransactionState.ABORTED,
                    partiallyCommittedState.getCreatedAt()));
          }
        });
  }

  public void putState(Coordinator.State state) throws CoordinatorException {
    Put put = createPutWith(state);
    put(put);
  }

  // For PoC
  public void putStateForGroupCommit(
      String parentId, Collection<String> ids, TransactionState transactionState, long createdAt)
      throws CoordinatorException {
    boolean isFirst = true;
    StringBuilder sb = new StringBuilder();
    for (String id : ids) {
      IdForGroupCommit idForGroupCommit = idForGroupCommit(id);
      // TODO: Verify the parentId
      if (isFirst) {
        isFirst = false;
      } else {
        sb.append(",");
      }
      sb.append(idForGroupCommit.childId);
    }

    Put put =
        new Put(new Key(Attribute.toIdValue(parentId)))
            .withValue(Attribute.toStateValue(transactionState))
            .withValue(Attribute.toCreatedAtValue(createdAt))
            .withValue(new TextValue("child_ids", sb.toString()))
            .withConsistency(Consistency.LINEARIZABLE)
            .withCondition(new PutIfNotExists())
            .forNamespace(coordinatorNamespace)
            .forTable(TABLE);
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
    private final String id;
    private final TransactionState state;
    private final long createdAt;
    private final String[] childIds;

    public State(Result result) throws CoordinatorException {
      checkNotMissingRequired(result);
      id = result.getValue(Attribute.ID).get().getAsString().get();
      state = TransactionState.getInstance(result.getValue(Attribute.STATE).get().getAsInt());
      createdAt = result.getValue(Attribute.CREATED_AT).get().getAsLong();
      // For PoC
      Optional<String> childIdsOpt = result.getValue("child_ids").get().getAsString();
      childIds = childIdsOpt.map(s -> s.split(",")).orElse(null);
    }

    public State(String id, TransactionState state) {
      this(id, state, System.currentTimeMillis());
    }

    // For the SpotBugs warning CT_CONSTRUCTOR_THROW
    @Override
    protected final void finalize() {}

    @VisibleForTesting
    State(String id, TransactionState state, long createdAt) {
      this.id = checkNotNull(id);
      this.state = checkNotNull(state);
      this.createdAt = createdAt;
      this.childIds = new String[] {};
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

    public String[] childIds() {
      return childIds;
    }

    /// FIXME: DEBUG
    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("id", id)
          .add("state", state)
          .add("createdAt", createdAt)
          .add("childIds", childIds)
          .toString();
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
      return (getId().equals(other.getId()) && getState().equals(other.getState()));
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, state);
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
