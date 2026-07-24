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
 *       obtained from {@link #beginGlobal()};
 *   <li>one {@link BranchTransaction} per branch — the CRUD handle for that branch, obtained from
 *       {@link #beginBranch(String)}.
 * </ul>
 *
 * <p>A typical flow: an initiator begins the global transaction with {@link #beginGlobal()}, reads
 * the transaction ID with {@link GlobalTransaction#getId()}, and shares it; each branch joins the
 * transaction by calling {@link #beginBranch(String)} with that ID, performs CRUD on the returned
 * {@link BranchTransaction}, and calls {@link BranchTransaction#end()}; finally the initiator calls
 * {@link GlobalTransaction#commit()} (or {@link GlobalTransaction#rollback()}).
 *
 * <p>The {@code beginGlobalReadOnly} variants begin a read-only transaction, which the
 * implementation may optimize. The {@code startXxx} methods are aliases of the corresponding {@code
 * beginXxx} methods.
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
  default GlobalTransaction beginGlobal()
      throws TransactionNotFoundException, TransactionException {
    return beginGlobal(Collections.emptyMap());
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
  GlobalTransaction beginGlobal(Map<String, String> attributes)
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
  default GlobalTransaction beginGlobalReadOnly()
      throws TransactionNotFoundException, TransactionException {
    return beginGlobalReadOnly(Collections.emptyMap());
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
  GlobalTransaction beginGlobalReadOnly(Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException;

  /**
   * Begins a new global transaction. This method is an alias of {@link #beginGlobal()}.
   *
   * @return the {@link GlobalTransaction} handle used to drive the transaction's outcome
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction from the beginning
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults
   */
  default GlobalTransaction startGlobal()
      throws TransactionNotFoundException, TransactionException {
    return beginGlobal();
  }

  /**
   * Begins a new global transaction with attributes. This method is an alias of {@link
   * #beginGlobal(Map)}.
   *
   * @param attributes implementation-specific transaction attributes (may be empty)
   * @return the {@link GlobalTransaction} handle used to drive the transaction's outcome
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction from the beginning
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults
   */
  default GlobalTransaction startGlobal(Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException {
    return beginGlobal(attributes);
  }

  /**
   * Begins a new read-only global transaction. This method is an alias of {@link
   * #beginGlobalReadOnly()}.
   *
   * @return the {@link GlobalTransaction} handle used to drive the transaction's outcome
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction from the beginning
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults
   */
  default GlobalTransaction startGlobalReadOnly()
      throws TransactionNotFoundException, TransactionException {
    return beginGlobalReadOnly();
  }

  /**
   * Begins a new read-only global transaction with attributes. This method is an alias of {@link
   * #beginGlobalReadOnly(Map)}.
   *
   * @param attributes implementation-specific transaction attributes (may be empty)
   * @return the {@link GlobalTransaction} handle used to drive the transaction's outcome
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction from the beginning
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults
   */
  default GlobalTransaction startGlobalReadOnly(Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException {
    return beginGlobalReadOnly(attributes);
  }

  /**
   * Begins a branch of the global transaction with the specified ID, joining it as a participant.
   *
   * @param transactionId the ID of the global transaction to join, as returned by {@link
   *     GlobalTransaction#getId()}
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
   * Begins a branch of the global transaction with the specified ID, joining it as a participant.
   * The given per-branch attributes are attached to every operation issued on the branch (an
   * attribute set directly on an operation takes precedence).
   *
   * @param transactionId the ID of the global transaction to join, as returned by {@link
   *     GlobalTransaction#getId()}
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
   * @param transactionId the ID of the global transaction to join, as returned by {@link
   *     GlobalTransaction#getId()}
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
   * @param transactionId the ID of the global transaction to join, as returned by {@link
   *     GlobalTransaction#getId()}
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
