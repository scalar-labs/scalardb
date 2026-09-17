package com.scalar.db.api;

import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;

/**
 * The handle for one branch of a global transaction, obtained via {@link
 * GlobalTransactionManager#beginBranch(String)}.
 *
 * <p>A branch performs CRUD (inherited from {@link TransactionCrudOperable}) against its own
 * portion of the data. The transaction's overall outcome — commit or rollback — is driven by the
 * owning {@link GlobalTransaction}, not by the branch.
 *
 * <p>The branch lifecycle places one obligation on each side:
 *
 * <ul>
 *   <li><b>On the process running the branch</b> — ensure it is ended on every path with {@link
 *       #end(Status)}, declaring the outcome of its work. Ending is what releases whatever that
 *       process holds for the branch, so it applies whichever way the transaction goes. A process
 *       that also drives the outcome is not exempt: the owner's commit or rollback only ever
 *       reaches branches begun on the same manager instance, so it still ends the branches it
 *       began.
 *   <li><b>On whoever drives the outcome</b> — do not {@linkplain GlobalTransaction#commit()
 *       commit} until every branch has been ended with {@link Status#SUCCESS}; a branch ended with
 *       {@link Status#FAILURE} obliges you to {@linkplain GlobalTransaction#rollback() roll back}
 *       instead. <b>Nothing enforces this.</b> No check detects a commit that follows a branch
 *       ended with {@code FAILURE}, and committing while a branch is still working may commit that
 *       branch's partial work.
 * </ul>
 *
 * <p>The two often fall to different processes. One that runs a branch but holds no {@link
 * GlobalTransaction} carries the first and cannot carry the second; one that holds both carries
 * each in turn, for the branches it began.
 *
 * <p>Close the scanners a branch handed out <em>before</em> ending it. {@link Status#FAILURE} will
 * not close them for you, and closing one afterwards is worse than leaving it open: a scanner's
 * {@code close()} participates in the transaction's read-set bookkeeping, so performing it as part
 * of a branch's cleanup can affect the outcome of the transaction as a whole. A scanner left open
 * is closed when the owning transaction is rolled back.
 *
 * <p>Ending a branch grants it no lifetime guarantee. Ending is a client-side operation, so it does
 * not keep the transaction alive; a branch that ends early while waiting for slower branches may
 * still have its state reclaimed by expiry before the owner commits. "Every branch has ended" is
 * necessary for a safe commit, not sufficient.
 *
 * <p>A branch handle is not thread-safe, and {@link #end(Status)} is no exception. Ending a branch
 * from a timeout or cancellation handler on another thread while CRUD is in flight is a data race
 * that the implementation does not guard against; the caller must coordinate.
 *
 * <p>Putting that together, a joining process:
 *
 * <pre>{@code
 * // In a method that declares throws TransactionException (or wider).
 * BranchTransaction branch = manager.beginBranch(transactionId);
 * try {
 *   branch.insert(insert);
 *   branch.end(BranchTransaction.Status.SUCCESS);
 * } catch (Exception e) {
 *   try {
 *     branch.end(BranchTransaction.Status.FAILURE);
 *   } catch (RuntimeException | CrudException suppressed) {
 *     e.addSuppressed(suppressed);
 *   }
 *   throw e;
 * }
 * }</pre>
 *
 * <p>The cleanup call is guarded so that a failure while ending cannot displace the failure that
 * caused it. Where the enclosing method cannot declare a checked exception, convert instead:
 *
 * <pre>{@code
 * } catch (Exception e) {
 *   try {
 *     branch.end(BranchTransaction.Status.FAILURE);
 *   } catch (RuntimeException | CrudException suppressed) {
 *     e.addSuppressed(suppressed);
 *   }
 *   throw e instanceof RuntimeException ? (RuntimeException) e : new IllegalStateException(e);
 * }
 * }</pre>
 *
 * <p>A process that also drives the outcome ends its own branch the same way, then drives the
 * transaction. Declaring {@code FAILURE} after a successful {@code SUCCESS} is a no-op, so a
 * failure arising after the branch was ended needs no extra guard:
 *
 * <pre>{@code
 * // In a method that declares throws TransactionException.
 * GlobalTransaction global = manager.begin();
 * BranchTransaction branch = manager.beginBranch(global.getId());
 * try {
 *   branch.insert(insert);
 *   branch.end(BranchTransaction.Status.SUCCESS);
 *   global.commit();
 * } catch (Exception e) {
 *   try {
 *     branch.end(BranchTransaction.Status.FAILURE);
 *   } catch (RuntimeException | CrudException suppressed) {
 *     e.addSuppressed(suppressed);
 *   }
 *   try {
 *     global.rollback();
 *   } catch (RollbackException suppressed) {
 *     e.addSuppressed(suppressed);
 *   }
 *   throw e;
 * }
 * }</pre>
 *
 * <p>Both cleanup calls are guarded, for the same reason: neither ending the branch nor rolling
 * back may displace the failure that caused them.
 */
public interface BranchTransaction extends TransactionCrudOperable {

  /**
   * Returns the ID of the global transaction this branch belongs to.
   *
   * @return the global transaction ID
   */
  String getId();

  /**
   * Ends the branch, declaring the outcome of its work. This does not commit the transaction; the
   * outcome of the transaction as a whole is still driven by the owning global transaction.
   *
   * <p>Issuing CRUD on a branch that has been ended is not allowed and is rejected with {@link
   * IllegalStateException}, whichever outcome was declared.
   *
   * <p>{@link Status#SUCCESS} rejects a branch that has already been ended, and requires that every
   * scanner obtained from this branch has been closed. {@link Status#FAILURE} is lenient so that it
   * can be called from a failure path without masking the original failure: an already-ended branch
   * is a no-op, and an open scanner is accepted rather than refused.
   *
   * @param status the outcome of this branch's work
   * @throws NullPointerException if {@code status} is null. The branch is left un-ended
   * @throws IllegalStateException if {@code status} is {@link Status#SUCCESS} and the branch has
   *     already been ended, or a scanner obtained from this branch has not been closed
   * @throws CrudConflictException if ending the branch fails due to transient faults (e.g., a
   *     conflict). You can retry the transaction from the beginning
   * @throws CrudException if ending the branch fails due to transient or nontransient faults
   */
  void end(Status status) throws CrudConflictException, CrudException;

  /** The outcome a branch declares when it is ended. */
  enum Status {

    /** The branch's work succeeded. */
    SUCCESS,

    /** The branch's work failed. */
    FAILURE
  }
}
