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
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TransactionState;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class CommitMutationComposer extends AbstractMutationComposer {
  private static final Logger LOGGER = LoggerFactory.getLogger(CommitMutationComposer.class);

  public CommitMutationComposer(String id) {
    super(id);
  }

  @VisibleForTesting
  CommitMutationComposer(String id, List<Mutation> mutations, long current) {
    super(id, mutations, current);
  }

  @Override
  public void add(Operation base, TransactionResult result) {
    if (base instanceof Put) {
      // for usual commit
      add((Put) base, result);
    } else if (base instanceof Delete) {
      // for usual commit
      add((Delete) base, result);
    } else { // Selection
      // for rollforward
      add((Selection) base, result);
    }
  }

  private void add(Put base, TransactionResult result) {
    mutations.add(composePut(base, result));
  }

  private void add(Delete base, TransactionResult result) {
    mutations.add(composeDelete(base, result));
  }

  // for rollforward
  private void add(Selection base, TransactionResult result) {
    if (result == null) {
      // delete non-existing record that was prepared with DELETED for Serializable with Extra-write
      mutations.add(composeDelete(base, null));
    } else if (result.getState().equals(TransactionState.PREPARED)) {
      mutations.add(composePut(base, result));
    } else if (result.getState().equals(TransactionState.DELETED)) {
      mutations.add(composeDelete(base, result));
    } else {
      LOGGER.debug(
          "the record was committed by the originated one "
              + "or rolled forward by another transaction: {}",
          result);
    }
  }

  private Put composePut(Operation base, TransactionResult result) {
    return new Put(base.getPartitionKey(), getClusteringKey(base, result).orElse(null))
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

  private Delete composeDelete(Operation base, TransactionResult result) {
    return new Delete(base.getPartitionKey(), getClusteringKey(base, result).orElse(null))
        .forNamespace(base.forNamespace().get())
        .forTable(base.forTable().get())
        .withConsistency(Consistency.LINEARIZABLE)
        .withCondition(
            new DeleteIf(
                new ConditionalExpression(ID, toIdValue(id), Operator.EQ),
                new ConditionalExpression(
                    STATE, toStateValue(TransactionState.DELETED), Operator.EQ)));
  }
}
