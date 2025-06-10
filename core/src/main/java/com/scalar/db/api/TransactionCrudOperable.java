package com.scalar.db.api;

import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import java.util.List;
import java.util.Optional;

/** An interface for transactional CRUD operations for transactions. */
public interface TransactionCrudOperable extends CrudOperable<CrudException> {

  /**
   * {@inheritDoc}
   *
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontransient
   */
  @Override
  Optional<Result> get(Get get) throws CrudConflictException, CrudException;

  /**
   * {@inheritDoc}
   *
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontransient
   */
  @Override
  List<Result> scan(Scan scan) throws CrudConflictException, CrudException;

  /**
   * {@inheritDoc}
   *
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontransient
   */
  @Override
  Scanner getScanner(Scan scan) throws CrudConflictException, CrudException;

  /**
   * {@inheritDoc}
   *
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontransient
   * @throws UnsatisfiedConditionException if a condition is specified, and if the condition is not
   *     satisfied or the entry does not exist
   * @deprecated As of release 3.13.0. Will be removed in release 5.0.0.
   */
  @Deprecated
  @Override
  void put(Put put) throws CrudConflictException, CrudException, UnsatisfiedConditionException;

  /**
   * {@inheritDoc}
   *
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontransient
   * @throws UnsatisfiedConditionException if a condition is specified, and if the condition is not
   *     satisfied or the entry does not exist
   * @deprecated As of release 3.13.0. Will be removed in release 5.0.0. Use {@link #mutate(List)}
   *     instead.
   */
  @Deprecated
  @Override
  void put(List<Put> puts)
      throws CrudConflictException, CrudException, UnsatisfiedConditionException;

  /**
   * {@inheritDoc}
   *
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontransient
   */
  @Override
  void insert(Insert insert) throws CrudConflictException, CrudException;

  /**
   * {@inheritDoc}
   *
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontransient
   */
  @Override
  void upsert(Upsert upsert) throws CrudConflictException, CrudException;

  /**
   * {@inheritDoc}
   *
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontransient
   * @throws UnsatisfiedConditionException if a condition is specified, and if the condition is not
   *     satisfied or the entry does not exist
   */
  @Override
  void update(Update update)
      throws CrudConflictException, CrudException, UnsatisfiedConditionException;

  /**
   * {@inheritDoc}
   *
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontransient
   * @throws UnsatisfiedConditionException if a condition is specified, and if the condition is not
   *     satisfied or the entry does not exist
   */
  @Override
  void delete(Delete delete)
      throws CrudConflictException, CrudException, UnsatisfiedConditionException;

  /**
   * {@inheritDoc}
   *
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontransient
   * @throws UnsatisfiedConditionException if a condition is specified, and if the condition is not
   *     satisfied or the entry does not exist
   * @deprecated As of release 3.13.0. Will be removed in release 5.0.0. Use {@link #mutate(List)}
   *     instead.
   */
  @Deprecated
  @Override
  void delete(List<Delete> deletes)
      throws CrudConflictException, CrudException, UnsatisfiedConditionException;

  /**
   * {@inheritDoc}
   *
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontransient
   * @throws UnsatisfiedConditionException if a condition is specified in a {@link Put}, {@link
   *     Delete}, or {@link Update} command, and if the condition is not satisfied or the entry does
   *     not exist
   */
  @Override
  void mutate(List<? extends Mutation> mutations)
      throws CrudConflictException, CrudException, UnsatisfiedConditionException;

  interface Scanner extends CrudOperable.Scanner<CrudException> {
    /**
     * {@inheritDoc}
     *
     * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
     *     (e.g., a conflict error). You can retry the transaction from the beginning
     * @throws CrudException if the transaction CRUD operation fails due to transient or
     *     nontransient faults. You can try retrying the transaction from the beginning, but the
     *     transaction may still fail if the cause is nontransient
     */
    @Override
    Optional<Result> one() throws CrudConflictException, CrudException;

    /**
     * {@inheritDoc}
     *
     * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
     *     (e.g., a conflict error). You can retry the transaction from the beginning
     * @throws CrudException if the transaction CRUD operation fails due to transient or
     *     nontransient faults. You can try retrying the transaction from the beginning, but the
     *     transaction may still fail if the cause is nontransient
     */
    @Override
    List<Result> all() throws CrudConflictException, CrudException;

    /**
     * {@inheritDoc}
     *
     * @throws CrudException if closing the scanner fails
     */
    @Override
    void close() throws CrudException;
  }
}
