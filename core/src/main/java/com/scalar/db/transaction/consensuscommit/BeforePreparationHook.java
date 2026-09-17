package com.scalar.db.transaction.consensuscommit;

import java.util.concurrent.Future;

/**
 * A hook invoked by {@link CommitHandler#commit} before the prepare phase begins.
 *
 * <p>This hook is invoked only on the two-phase commit path (prepare, validate, commit-state). It
 * is not invoked when the one-phase commit optimization path is taken.
 */
public interface BeforePreparationHook {
  Future<Void> handle(TransactionContext context);
}
