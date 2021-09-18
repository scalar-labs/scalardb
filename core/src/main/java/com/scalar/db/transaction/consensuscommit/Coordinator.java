package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
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
import com.scalar.db.exception.transaction.CoordinatorException;
import com.scalar.db.exception.transaction.RequiredValueMissingException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
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
          .addColumn(Attribute.METADATA, DataType.TEXT)
          .addPartitionKey(Attribute.ID)
          .build();

  private static final int MAX_RETRY_COUNT = 5;
  private static final long SLEEP_BASE_MILLIS = 50;
  private static final Logger LOGGER = LoggerFactory.getLogger(Coordinator.class);
  private final DistributedStorage storage;

  public Coordinator(DistributedStorage storage) {
    this.storage = storage;
  }

  public Optional<Coordinator.State> getState(String id) throws CoordinatorException {
    Get get = createGetWith(id);
    return get(get);
  }

  public void putState(Coordinator.State state) throws CoordinatorException {
    Put put = createPutWith(state);
    put(put);
  }

  private Get createGetWith(String id) {
    return new Get(new Key(Attribute.toIdValue(id)))
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(NAMESPACE)
        .forTable(TABLE);
  }

  private Optional<Coordinator.State> get(Get get) throws CoordinatorException {
    int counter = 0;
    while (true) {
      if (counter >= MAX_RETRY_COUNT) {
        throw new CoordinatorException("can't get coordinator state.");
      }
      try {
        return storage.get(get).map(State::new);
      } catch (ExecutionException e) {
        LOGGER.warn("can't get coordinator state.", e);
      }
      exponentialBackoff(counter++);
    }
  }

  @VisibleForTesting
  Put createPutWith(Coordinator.State state) {
    Put put =
        new Put(new Key(Attribute.toIdValue(state.getId())))
            .withValue(Attribute.toStateValue(state.getState()))
            .withValue(Attribute.toCreatedAtValue(state.getCreatedAt()))
            .withConsistency(Consistency.LINEARIZABLE)
            .withCondition(new PutIfNotExists())
            .forNamespace(NAMESPACE)
            .forTable(TABLE);

    state.getMetadata().ifPresent(m -> put.withValue(Attribute.METADATA, m));
    return put;
  }

  private void put(Put put) throws CoordinatorException {
    int counter = 0;
    while (true) {
      if (counter >= MAX_RETRY_COUNT) {
        throw new CoordinatorException("couldn't put coordinator state.");
      }
      try {
        storage.put(put);
        break;
      } catch (NoMutationException e) {
        LOGGER.warn("mutation seems applied already", e);
        throw new CoordinatorException("mutation seems applied already.", e);
      } catch (ExecutionException e) {
        LOGGER.warn("putting state in coordinator failed.", e);
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
    private String metadata;

    public State(Result result) {
      checkNotMissingRequired(result);
      id = result.getValue(Attribute.ID).get().getAsString().get();
      state = TransactionState.getInstance(result.getValue(Attribute.STATE).get().getAsInt());
      createdAt = result.getValue(Attribute.CREATED_AT).get().getAsLong();
    }

    public State(String id, TransactionState state) {
      this(id, state, System.currentTimeMillis());
    }

    @VisibleForTesting
    State(String id, TransactionState state, long createdAt) {
      this.id = checkNotNull(id);
      this.state = checkNotNull(state);
      this.createdAt = createdAt;
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

    @Nonnull
    public Optional<String> getMetadata() {
      return Optional.ofNullable(metadata);
    }

    public void setMetadata(String metadata) {
      this.metadata = metadata;
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
      // NOTICE: createdAt and metadata are not taken into account
      return (getId().equals(other.getId()) && getState().equals(other.getState()));
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, state);
    }

    private void checkNotMissingRequired(Result result) {
      if (!result.getValue(Attribute.ID).isPresent()
          || !result.getValue(Attribute.ID).get().getAsString().isPresent()) {
        throw new RequiredValueMissingException("id is missing in the coordinator state");
      }
      if (!result.getValue(Attribute.STATE).isPresent()
          || result.getValue(Attribute.STATE).get().getAsInt() == 0) {
        throw new RequiredValueMissingException("state is missing in the coordinator state");
      }
      if (!result.getValue(Attribute.CREATED_AT).isPresent()
          || result.getValue(Attribute.CREATED_AT).get().getAsLong() == 0) {
        throw new RequiredValueMissingException("created_at is missing in the coordinator state");
      }
    }
  }
}
