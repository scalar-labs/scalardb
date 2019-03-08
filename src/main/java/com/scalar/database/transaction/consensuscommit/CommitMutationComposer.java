package com.scalar.database.transaction.consensuscommit;

import static com.scalar.database.api.ConditionalExpression.Operator;
import static com.scalar.database.transaction.consensuscommit.Attribute.*;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.database.api.ConditionalExpression;
import com.scalar.database.api.Consistency;
import com.scalar.database.api.Delete;
import com.scalar.database.api.DeleteIf;
import com.scalar.database.api.Mutation;
import com.scalar.database.api.Operation;
import com.scalar.database.api.Put;
import com.scalar.database.api.PutIf;
import com.scalar.database.api.Selection;
import com.scalar.database.api.TransactionState;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keep track of mutations that need to be committed to {@link
 * com.scalar.database.api.DistributedStorage}
 */
@NotThreadSafe
public class CommitMutationComposer extends AbstractMutationComposer {
  private static final Logger LOGGER = LoggerFactory.getLogger(CommitMutationComposer.class);

  /**
   * Constructs a {@code CommitMutationComposer} with specified id
   *
   * @param id a String
   */
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
      // for rollforward in recovery
      add((Selection) base, result);
    }
  }

  private void add(Put base, TransactionResult result) {
    mutations.add(composePut(base, result));
  }

  private void add(Delete base, TransactionResult result) {
    mutations.add(composeDelete(base, result));
  }

  // for rollforward in recovery
  private void add(Selection base, TransactionResult result) {
    if (result.getState().equals(TransactionState.PREPARED)) {
      mutations.add(composePut(base, result));
    } else if (result.getState().equals(TransactionState.DELETED)) {
      mutations.add(composeDelete(base, result));
    } else {
      LOGGER.info(
          "the record was committed by the originated one "
              + "or rollforwarded by another transaction");
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
