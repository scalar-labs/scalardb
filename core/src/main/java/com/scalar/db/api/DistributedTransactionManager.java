package com.scalar.db.api;

import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public interface DistributedTransactionManager
    extends TransactionManagerCrudOperable, AutoCloseable {

  /**
   * Sets the specified namespace and the table name as default values in the instance.
   *
   * @param namespace default namespace to operate for
   * @param tableName default table name to operate for
   * @deprecated As of release 3.6.0. Will be removed in release 4.0.0
   */
  @Deprecated
  void with(String namespace, String tableName);

  /**
   * Sets the specified namespace as a default value in the instance.
   *
   * @param namespace default namespace to operate for
   * @deprecated As of release 3.6.0. Will be removed in release 4.0.0
   */
  @Deprecated
  void withNamespace(String namespace);

  /**
   * Returns the namespace.
   *
   * @return an {@code Optional} with the namespace
   * @deprecated As of release 3.6.0. Will be removed in release 4.0.0
   */
  @Deprecated
  Optional<String> getNamespace();

  /**
   * Sets the specified table name as a default value in the instance.
   *
   * @param tableName default table name to operate for
   * @deprecated As of release 3.6.0. Will be removed in release 4.0.0
   */
  @Deprecated
  void withTable(String tableName);

  /**
   * Returns the table name.
   *
   * @return an {@code Optional} with the table name
   * @deprecated As of release 3.6.0. Will be removed in release 4.0.0
   */
  @Deprecated
  Optional<String> getTable();

  /**
   * Begins a new transaction.
   *
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to begin the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction begin() throws TransactionNotFoundException, TransactionException {
    return begin(UUID.randomUUID().toString());
  }

  /**
   * Begins a new transaction with the specified transaction ID. It is users' responsibility to
   * guarantee uniqueness of the ID, so it is not recommended to use this method unless you know
   * exactly what you are doing.
   *
   * @param txId a user-provided unique transaction ID
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to begin the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction begin(String txId)
      throws TransactionNotFoundException, TransactionException {
    return begin(txId, Collections.emptyMap());
  }

  /**
   * Begins a new transaction with the specified attributes.
   *
   * @param attributes attributes for the transaction
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to begin the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction begin(Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException {
    return begin(UUID.randomUUID().toString(), attributes);
  }

  /**
   * Begins a new transaction with the specified transaction ID and attributes. It is users'
   * responsibility to guarantee uniqueness of the ID, so it is not recommended to use this method
   * unless you know exactly what you are doing.
   *
   * @param txId a user-provided unique transaction ID
   * @param attributes attributes for the transaction
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to begin the
   *     transaction due to nontransient faults
   */
  DistributedTransaction begin(String txId, Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException;

  /**
   * Begins a new transaction in read-only mode.
   *
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to begin the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction beginReadOnly()
      throws TransactionNotFoundException, TransactionException {
    return beginReadOnly(UUID.randomUUID().toString());
  }

  /**
   * Begins a new transaction with the specified transaction ID in read-only mode. It is users'
   * responsibility to guarantee uniqueness of the ID, so it is not recommended to use this method
   * unless you know exactly what you are doing.
   *
   * @param txId a user-provided unique transaction ID
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to begin the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction beginReadOnly(String txId)
      throws TransactionNotFoundException, TransactionException {
    return beginReadOnly(txId, Collections.emptyMap());
  }

  /**
   * Begins a new transaction in read-only mode with the specified attributes.
   *
   * @param attributes attributes for the transaction
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to begin the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction beginReadOnly(Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException {
    return beginReadOnly(UUID.randomUUID().toString(), attributes);
  }

  /**
   * Begins a new transaction with the specified transaction ID in read-only mode with the specified
   * attributes. It is users' responsibility to guarantee uniqueness of the ID, so it is not
   * recommended to use this method unless you know exactly what you are doing.
   *
   * @param txId a user-provided unique transaction ID
   * @param attributes attributes for the transaction
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to begin the
   *     transaction due to nontransient faults
   */
  DistributedTransaction beginReadOnly(String txId, Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException;

  /**
   * Starts a new transaction. This method is an alias of {@link #begin()}.
   *
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to start due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to start due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to start the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction start() throws TransactionNotFoundException, TransactionException {
    return begin();
  }

  /**
   * Starts a new transaction with the specified transaction ID. This method is an alias of {@link
   * #begin(String)}.
   *
   * @param txId a user-provided unique transaction ID
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to start due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to start due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to start the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction start(String txId)
      throws TransactionNotFoundException, TransactionException {
    return begin(txId);
  }

  /**
   * Starts a new transaction with the specified attributes. This method is an alias of {@link
   * #begin(Map)}.
   *
   * @param attributes attributes for the transaction
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to start due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to start due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to start the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction start(Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException {
    return begin(attributes);
  }

  /**
   * Starts a new transaction with the specified transaction ID and attributes. This method is an
   * alias of {@link #begin(String, Map)}.
   *
   * @param txId a user-provided unique transaction ID
   * @param attributes attributes for the transaction
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to start due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to start due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to start the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction start(String txId, Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException {
    return begin(txId, attributes);
  }

  /**
   * Starts a new transaction in read-only mode. This method is an alias of {@link
   * #beginReadOnly()}.
   *
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to start due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to start due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to start the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction startReadOnly()
      throws TransactionNotFoundException, TransactionException {
    return beginReadOnly();
  }

  /**
   * Starts a new transaction with the specified transaction ID in read-only mode. This method is an
   * alias of {@link #beginReadOnly(String)}.
   *
   * @param txId a user-provided unique transaction ID
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to start due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to start due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to start the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction startReadOnly(String txId)
      throws TransactionNotFoundException, TransactionException {
    return beginReadOnly(txId);
  }

  /**
   * Starts a new transaction in read-only mode with the specified attributes. This method is an
   * alias of {@link #beginReadOnly(Map)}.
   *
   * @param attributes attributes for the transaction
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to start due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to start due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to start the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction startReadOnly(Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException {
    return beginReadOnly(attributes);
  }

  /**
   * Starts a new transaction with the specified transaction ID in read-only mode with the specified
   * attributes. This method is an alias of {@link #beginReadOnly(String, Map)}.
   *
   * @param txId a user-provided unique transaction ID
   * @param attributes attributes for the transaction
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to start due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to start due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to start the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction startReadOnly(String txId, Map<String, String> attributes)
      throws TransactionNotFoundException, TransactionException {
    return beginReadOnly(txId, attributes);
  }

  /**
   * Starts a new transaction with the specified {@link Isolation} level.
   *
   * @param isolation an isolation level
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction fails
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(Isolation isolation) throws TransactionException;

  /**
   * Starts a new transaction with the specified transaction ID and {@link Isolation} level. It is
   * users' responsibility to guarantee uniqueness of the ID, so it is not recommended to use this
   * method unless you know exactly what you are doing.
   *
   * @param txId a user-provided unique transaction ID
   * @param isolation an isolation level
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction fails
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(String txId, Isolation isolation) throws TransactionException;

  /**
   * Starts a new transaction with the specified {@link Isolation} level and {@link
   * SerializableStrategy}. If the isolation is not SERIALIZABLE, the serializable strategy is
   * ignored.
   *
   * @param isolation an isolation level
   * @param strategy a serializable strategy
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction fails
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException;

  /**
   * Starts a new transaction with Serializable isolation level and the specified {@link
   * SerializableStrategy}.
   *
   * @param strategy a serializable strategy
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction fails
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(SerializableStrategy strategy) throws TransactionException;

  /**
   * Starts a new transaction with the specified transaction ID, Serializable isolation level and
   * the specified {@link SerializableStrategy}. It is users' responsibility to guarantee uniqueness
   * of the ID, so it is not recommended to use this method unless you know exactly what you are
   * doing.
   *
   * @param txId a user-provided unique transaction ID
   * @param strategy a serializable strategy
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction fails
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException;

  /**
   * Starts a new transaction with the specified transaction ID, {@link Isolation} level and {@link
   * SerializableStrategy}. It is users' responsibility to guarantee uniqueness of the ID, so it is
   * not recommended to use this method unless you know exactly what you are doing. If the isolation
   * is not SERIALIZABLE, the serializable strategy is ignored.
   *
   * @param txId a user-provided unique transaction ID
   * @param isolation an isolation level
   * @param strategy a serializable strategy
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction fails
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(String txId, Isolation isolation, SerializableStrategy strategy)
      throws TransactionException;

  /**
   * Joins an ongoing transaction associated with the specified transaction ID.
   *
   * @param txId the transaction ID
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction associated with the specified
   *     transaction ID is not found. You can retry the transaction from the beginning
   */
  default DistributedTransaction join(String txId) throws TransactionNotFoundException {
    return resume(txId);
  }

  /**
   * Resumes an ongoing transaction associated with the specified transaction ID.
   *
   * @param txId the transaction ID
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction associated with the specified
   *     transaction ID is not found. You can retry the transaction from the beginning
   */
  DistributedTransaction resume(String txId) throws TransactionNotFoundException;

  /**
   * Returns the state of a given transaction.
   *
   * <p><b>Note:</b> This is a low-level operational API specific to the Consensus Commit
   * transaction manager. Most applications should not call it directly — it is intended for
   * advanced use cases. Callers are expected to understand the underlying transaction lifecycle and
   * the implications of invoking this method directly.
   *
   * @param txId a transaction ID
   * @return {@link TransactionState}
   * @throws TransactionException if getting the state of a given transaction fails
   * @throws UnsupportedOperationException if the underlying transaction manager does not support
   *     getting a transaction state
   */
  TransactionState getState(String txId) throws TransactionException;

  /**
   * Rolls back a given transaction.
   *
   * <p><b>Note:</b> This is a low-level operational API specific to the Consensus Commit
   * transaction manager. Most applications should not call it directly — it is intended for
   * advanced use cases. Callers are expected to understand the underlying transaction lifecycle and
   * the implications of invoking this method directly.
   *
   * @param txId a transaction ID
   * @return {@link TransactionState}
   * @throws TransactionException if rolling back the given transaction fails
   * @throws UnsupportedOperationException if the underlying transaction manager does not support
   *     rolling back a transaction
   */
  TransactionState rollback(String txId) throws TransactionException;

  /**
   * Aborts a given transaction. This method is an alias of {@link #rollback(String)}.
   *
   * <p><b>Note:</b> This is a low-level operational API specific to the Consensus Commit
   * transaction manager. Most applications should not call it directly — it is intended for
   * advanced use cases. Callers are expected to understand the underlying transaction lifecycle and
   * the implications of invoking this method directly.
   *
   * @param txId a transaction ID
   * @return {@link TransactionState}
   * @throws TransactionException if aborting the given transaction fails
   * @throws UnsupportedOperationException if the underlying transaction manager does not support
   *     aborting a transaction
   */
  default TransactionState abort(String txId) throws TransactionException {
    return rollback(txId);
  }

  /**
   * Finishes a given terminated transaction by performing any remaining post-termination work and
   * cleaning up the Coordinator state row. The transaction must already be in a terminal state
   * ({@code COMMITTED} or {@code ABORTED}); this method completes the per-record work that was
   * otherwise deferred to lazy recovery — rolling forward {@code PREPARED} or {@code DELETED}
   * records of a committed transaction, or rolling back {@code PREPARED} or {@code DELETED} records
   * of an aborted one — and then removes the Coordinator state row.
   *
   * <p>This is a best-effort, retryable cleanup API intended to be called after a transaction
   * terminates so that ScalarDB can complete per-record post-termination work eagerly and reclaim
   * the Coordinator state row instead of leaving it for lazy recovery.
   *
   * <p><b>Note:</b> This is a low-level operational API specific to the Consensus Commit
   * transaction manager. Most applications should not call it directly — it is intended for
   * advanced use cases. Callers are expected to understand the underlying transaction lifecycle and
   * the implications of invoking this method directly.
   *
   * <p><b>Applicability and return value:</b> only transactions terminated via {@link
   * DistributedTransaction#commit()} are eligible — they are the ones that persist a write set
   * alongside the Coordinator state row, regardless of whether the commit succeeded ({@code
   * COMMITTED}) or failed via a conflict during preparation ({@code ABORTED}). For an eligible
   * transaction, this method completes the cleanup and returns {@code true}. Transactions that did
   * not go through {@link DistributedTransaction#commit()} (for example, transactions terminated
   * via {@link #rollback(String)} or {@link #abort(String)}, transactions aborted by lazy recovery,
   * or transactions originated from older binaries that pre-date the write-set column) do not carry
   * a write set; they are not applicable to this method, and calling it on their transaction ID
   * returns {@code false} without doing any work. This is an expected outcome rather than an error
   * — retrying with the same transaction ID would never succeed — and those state rows are left for
   * lazy recovery to handle.
   *
   * <p><b>Idempotency:</b> calling this method on a transaction ID whose state row is absent
   * (already finished, never started, or already cleaned up by a concurrent caller) returns {@code
   * true}. Callers may safely re-invoke this method on the same transaction ID.
   *
   * <p><b>Group commit:</b> when the transaction ID belongs to a child of a group commit, the call
   * processes the write sets of all sibling children in a single pass and then deletes the shared
   * parent state row. Subsequent calls with sibling transaction IDs return {@code true} per the
   * idempotency contract above.
   *
   * @param txId a transaction ID
   * @return {@code true} if the transaction was finished (or was already finished), or {@code
   *     false} if the transaction is not applicable because it carries no write set
   * @throws TransactionException if finishing the given transaction fails
   * @throws UnsupportedOperationException if the underlying transaction manager does not support
   *     coordinator-level cleanup
   */
  boolean finishTransaction(String txId) throws TransactionException;

  /**
   * Closes connections to the cluster. The connections are shared among multiple services such as
   * StorageService and TransactionService, thus this should only be used when closing applications.
   */
  @Override
  void close();
}
