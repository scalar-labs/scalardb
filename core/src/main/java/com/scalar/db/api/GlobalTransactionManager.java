package com.scalar.db.api;

import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import java.util.Collections;
import java.util.Map;

/**
 * A manager of global transactions — distributed transactions that span multiple branches, each
 * performing its own work against its own data and coordinated into a single transaction that
 * commits or rolls back as a whole. The branches typically run in separate processes (for example,
 * one per microservice).
 *
 * <p>A global transaction has two kinds of handle:
 *
 * <ul>
 *   <li>a {@link GlobalTransaction} — the overall handle that drives the commit/rollback outcome,
 *       obtained from {@link #begin()};
 *   <li>one {@link BranchTransaction} per branch — the CRUD handle for that branch, obtained from
 *       {@link #beginBranch(String)}.
 * </ul>
 *
 * <p>A typical flow: an initiator begins the global transaction with {@link #begin()}, reads the
 * transaction ID with {@link GlobalTransaction#getId()}, and shares it; each branch is begun by
 * calling {@link #beginBranch(String)} with that ID, performs its CRUD on the returned {@link
 * BranchTransaction}, and is ended with {@link BranchTransaction#end(BranchTransaction.Status)}
 * declaring that branch's outcome; finally the initiator calls {@link GlobalTransaction#commit()}
 * (or {@link GlobalTransaction#rollback()}). Every branch is ended, including one the initiator
 * began itself, and on the rollback path as well as the commit path.
 *
 * <p>The {@code beginReadOnly} variants begin a read-only transaction, which the implementation may
 * optimize. The {@code startXxx} methods are aliases of the corresponding {@code beginXxx} methods.
 */
public interface GlobalTransactionManager extends AutoCloseable {

  /**
   * Begins a new global transaction.
   *
   * @return the {@link GlobalTransaction} handle used to drive the transaction's outcome
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction from the beginning
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults
   */
  default GlobalTransaction begin() throws TransactionNotFoundException, TransactionException {
    return begin(Collections.emptyMap());
  }

  /**
   * Begins a new global transaction with the specified transaction-scoped attributes.
   *
   * @param attributes implementation-specific transaction attributes (may be empty)
   * @return the {@link GlobalTransaction} handle used to drive the transaction's outcome
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction from the beginning
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults
   */
  GlobalTransaction begin(Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException;

  /**
   * Begins a new read-only global transaction. The implementation may optimize for a transaction
   * that will not write.
   *
   * @return the {@link GlobalTransaction} handle used to drive the transaction's outcome
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction from the beginning
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults
   */
  default GlobalTransaction beginReadOnly()
      throws TransactionNotFoundException, TransactionException {
    return beginReadOnly(Collections.emptyMap());
  }

  /**
   * Begins a new read-only global transaction with the specified transaction-scoped attributes. The
   * implementation may optimize for a transaction that will not write.
   *
   * @param attributes implementation-specific transaction attributes (may be empty)
   * @return the {@link GlobalTransaction} handle used to drive the transaction's outcome
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction from the beginning
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults
   */
  GlobalTransaction beginReadOnly(Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException;

  /**
   * Begins a new global transaction. This method is an alias of {@link #begin()}.
   *
   * @return the {@link GlobalTransaction} handle used to drive the transaction's outcome
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction from the beginning
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults
   */
  default GlobalTransaction start() throws TransactionNotFoundException, TransactionException {
    return begin();
  }

  /**
   * Begins a new global transaction with attributes. This method is an alias of {@link
   * #begin(Map)}.
   *
   * @param attributes implementation-specific transaction attributes (may be empty)
   * @return the {@link GlobalTransaction} handle used to drive the transaction's outcome
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction from the beginning
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults
   */
  default GlobalTransaction start(Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException {
    return begin(attributes);
  }

  /**
   * Begins a new read-only global transaction. This method is an alias of {@link #beginReadOnly()}.
   *
   * @return the {@link GlobalTransaction} handle used to drive the transaction's outcome
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction from the beginning
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults
   */
  default GlobalTransaction startReadOnly()
      throws TransactionNotFoundException, TransactionException {
    return beginReadOnly();
  }

  /**
   * Begins a new read-only global transaction with attributes. This method is an alias of {@link
   * #beginReadOnly(Map)}.
   *
   * @param attributes implementation-specific transaction attributes (may be empty)
   * @return the {@link GlobalTransaction} handle used to drive the transaction's outcome
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction from the beginning
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults
   */
  default GlobalTransaction startReadOnly(Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException {
    return beginReadOnly(attributes);
  }

  /**
   * Begins a branch of the global transaction with the specified ID.
   *
   * @param transactionId the ID of the global transaction this branch belongs to, as returned by
   *     {@link GlobalTransaction#getId()}
   * @return the {@link BranchTransaction} CRUD handle for this branch
   * @throws TransactionNotFoundException if the branch fails to begin due to transient faults. You
   *     can retry the transaction from the beginning
   * @throws TransactionException if the branch fails to begin due to transient or nontransient
   *     faults
   */
  default BranchTransaction beginBranch(String transactionId)
      throws TransactionNotFoundException, TransactionException {
    return beginBranch(transactionId, Collections.emptyMap());
  }

  /**
   * Begins a branch of the global transaction with the specified ID. The given per-branch
   * attributes are attached to every operation issued on the branch (an attribute set directly on
   * an operation takes precedence).
   *
   * @param transactionId the ID of the global transaction this branch belongs to, as returned by
   *     {@link GlobalTransaction#getId()}
   * @param attributes per-branch, implementation-specific attributes attached to each operation
   *     issued on the branch (may be empty)
   * @return the {@link BranchTransaction} CRUD handle for this branch
   * @throws TransactionNotFoundException if the branch fails to begin due to transient faults. You
   *     can retry the transaction from the beginning
   * @throws TransactionException if the branch fails to begin due to transient or nontransient
   *     faults
   */
  BranchTransaction beginBranch(String transactionId, Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException;

  /**
   * Begins a branch of the global transaction with the specified ID. This method is an alias of
   * {@link #beginBranch(String)}.
   *
   * @param transactionId the ID of the global transaction this branch belongs to, as returned by
   *     {@link GlobalTransaction#getId()}
   * @return the {@link BranchTransaction} CRUD handle for this branch
   * @throws TransactionNotFoundException if the branch fails to begin due to transient faults. You
   *     can retry the transaction from the beginning
   * @throws TransactionException if the branch fails to begin due to transient or nontransient
   *     faults
   */
  default BranchTransaction startBranch(String transactionId)
      throws TransactionNotFoundException, TransactionException {
    return beginBranch(transactionId);
  }

  /**
   * Begins a branch of the global transaction with the specified ID and attributes. This method is
   * an alias of {@link #beginBranch(String, Map)}.
   *
   * @param transactionId the ID of the global transaction this branch belongs to, as returned by
   *     {@link GlobalTransaction#getId()}
   * @param attributes per-branch, implementation-specific attributes attached to each operation
   *     issued on the branch (may be empty)
   * @return the {@link BranchTransaction} CRUD handle for this branch
   * @throws TransactionNotFoundException if the branch fails to begin due to transient faults. You
   *     can retry the transaction from the beginning
   * @throws TransactionException if the branch fails to begin due to transient or nontransient
   *     faults
   */
  default BranchTransaction startBranch(String transactionId, Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException {
    return beginBranch(transactionId, attributes);
  }

  /** Closes the manager and releases any resources it holds. */
  @Override
  void close();
}
