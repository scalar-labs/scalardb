package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.api.ConditionalExpression.Operator;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.toIdValue;
import static com.scalar.db.transaction.consensuscommit.Attribute.toStateValue;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TransactionState;
import com.scalar.db.io.Key;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class CommitMutationComposer extends AbstractMutationComposer {
  private static final Logger logger = LoggerFactory.getLogger(CommitMutationComposer.class);

  public CommitMutationComposer(String id) {
    super(id);
  }

  @VisibleForTesting
  CommitMutationComposer(String id, long current) {
    super(id, current);
  }

  @Override
  public void add(Operation base, @Nullable TransactionResult result) {
    if (base instanceof Put) {
      // for usual commit
      add((Put) base, result);
    } else if (base instanceof Delete) {
      // for usual commit
      add((Delete) base, result);
    } else { // Selection
      add((Selection) base, result);
    }
  }

  private void add(Put base, @Nullable TransactionResult result) {
    mutations.add(composePut(base, result));
  }

  private void add(Delete base, @Nullable TransactionResult result) {
    mutations.add(composeDelete(base, result));
  }

  private void add(Selection base, @Nullable TransactionResult result) {
    if (result == null) {
      // for deleting non-existing record that was prepared with DELETED for Serializable with
      // Extra-write
      mutations.add(composeDelete(base, null));
    } else if (result.getState().equals(TransactionState.PREPARED)) {
      // for rollforward in lazy recovery
      mutations.add(composePut(base, result));
    } else if (result.getState().equals(TransactionState.DELETED)) {
      // for rollforward in lazy recovery
      mutations.add(composeDelete(base, result));
    } else {
      logger.debug(
          "The record was committed by the originated one "
              + "or rolled forward by another transaction: {}",
          result);
    }
  }

  private Put composePut(Operation base, @Nullable TransactionResult result) {
    return new Put(getPartitionKey(base, result), getClusteringKey(base, result).orElse(null))
        .forNamespace(base.forNamespace().get())
        .forTable(base.forTable().get())
        .withConsistency(Consistency.LINEARIZABLE)
        .withCondition(
            new PutIf(
                new ConditionalExpression(ID, toIdValue(id), Operator.EQ),
                new ConditionalExpression(
                    STATE, toStateValue(TransactionState.PREPARED), Operator.EQ)))
        .withValue(Attribute.toCommittedAtValue(current))
        .withValue(Attribute.toStateValue(TransactionState.COMMITTED));
  }

  private Delete composeDelete(Operation base, @Nullable TransactionResult result) {
    return new Delete(getPartitionKey(base, result), getClusteringKey(base, result).orElse(null))
        .forNamespace(base.forNamespace().get())
        .forTable(base.forTable().get())
        .withConsistency(Consistency.LINEARIZABLE)
        .withCondition(
            new DeleteIf(
                new ConditionalExpression(ID, toIdValue(id), Operator.EQ),
                new ConditionalExpression(
                    STATE, toStateValue(TransactionState.DELETED), Operator.EQ)));
  }

  private Key getPartitionKey(Operation base, @Nullable TransactionResult result) {
    if (base instanceof Mutation) {
      // for usual commit
      return base.getPartitionKey();
    } else {
      assert base instanceof Selection;
      if (result != null) {
        // for rollforward in lazy recovery
        return result.getPartitionKey().get();
      } else {
        // for deleting non-existing record that was prepared with DELETED for Serializable with
        // Extra-write
        assert base instanceof Get;
        return base.getPartitionKey();
      }
    }
  }

  private Optional<Key> getClusteringKey(Operation base, @Nullable TransactionResult result) {
    if (base instanceof Mutation) {
      // for usual commit
      return base.getClusteringKey();
    } else {
      assert base instanceof Selection;
      if (result != null) {
        // for rollforward in lazy recovery
        return result.getClusteringKey();
      } else {
        // for deleting non-existing record that was prepared with DELETED for Serializable with
        // Extra-write
        assert base instanceof Get;
        return base.getClusteringKey();
      }
    }
  }
}
