package com.scalar.db.api;

import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.RecordNotFoundException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import java.util.List;
import java.util.Optional;

/**
 * An interface for transactional CRUD operations. Note that the LINEARIZABLE consistency level is
 * always used in transactional CRUD operations, so {@link Consistency} specified for CRUD
 * operations is ignored.
 */
public interface TransactionCrudOperable {

  /**
   * Retrieves a result from the storage through a transaction with the specified {@link Get}
   * command with a primary key and returns the result.
   *
   * @param get a {@code Get} command
   * @return an {@code Optional} with the returned result
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontranient
   */
  Optional<Result> get(Get get) throws CrudConflictException, CrudException;

  /**
   * Retrieves results from the storage through a transaction with the specified {@link Scan}
   * command with a partition key and returns a list of {@link Result}. Results can be filtered by
   * specifying a range of clustering keys.
   *
   * @param scan a {@code Scan} command
   * @return a list of {@link Result}
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontranient
   */
  List<Result> scan(Scan scan) throws CrudConflictException, CrudException;

  /**
   * Inserts/Updates an entry to the storage through a transaction with the specified {@link Put}
   * command. If a condition is specified in the {@link Put} command, and if the condition is not
   * satisfied or the entry does not exist, it throws {@link UnsatisfiedConditionException}.
   *
   * @param put a {@code Put} command
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontranient
   * @throws UnsatisfiedConditionException if a condition is specified, and if the condition is not
   *     satisfied or the entry does not exist
   * @deprecated As of release 3.13.0. Will be removed in release 5.0.0.
   */
  @Deprecated
  void put(Put put) throws CrudConflictException, CrudException, UnsatisfiedConditionException;

  /**
   * Inserts/Updates multiple entries to the storage through a transaction with the specified list
   * of {@link Put} commands. If a condition is specified in the {@link Put} command, and if the
   * condition is not satisfied or the entry does not exist, it throws {@link
   * UnsatisfiedConditionException}.
   *
   * @param puts a list of {@code Put} commands
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontranient
   * @throws UnsatisfiedConditionException if a condition is specified, and if the condition is not
   *     satisfied or the entry does not exist
   * @deprecated As of release 3.13.0. Will be removed in release 5.0.0.
   */
  @Deprecated
  void put(List<Put> puts)
      throws CrudConflictException, CrudException, UnsatisfiedConditionException;

  /**
   * Deletes an entry from the storage through a transaction with the specified {@link Delete}
   * command. If a condition is specified in the {@link Delete} command, and if the condition is not
   * satisfied or the entry does not exist, it throws {@link UnsatisfiedConditionException}.
   *
   * @param delete a {@code Delete} command
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontranient
   * @throws UnsatisfiedConditionException if a condition is specified, and if the condition is not
   *     satisfied or the entry does not exist
   */
  void delete(Delete delete)
      throws CrudConflictException, CrudException, UnsatisfiedConditionException;

  /**
   * Deletes entries from the storage through a transaction with the specified list of {@link
   * Delete} commands. If a condition is specified in the {@link Delete} command, and if the
   * condition is not satisfied or the entry does not exist, it throws {@link
   * UnsatisfiedConditionException}.
   *
   * @param deletes a list of {@code Delete} commands
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontranient
   * @throws UnsatisfiedConditionException if a condition is specified, and if the condition is not
   *     satisfied or the entry does not exist
   * @deprecated As of release 3.13.0. Will be removed in release 5.0.0.
   */
  @Deprecated
  void delete(List<Delete> deletes)
      throws CrudConflictException, CrudException, UnsatisfiedConditionException;

  /**
   * Inserts an entry into the storage through a transaction with the specified {@link Insert}
   * command.
   *
   * <p>If the entry already exists, a conflict error occurs. Note that the location where the
   * conflict error is thrown depends on the implementation of the transaction manager. This method
   * may throw {@link CrudConflictException}. Alternatively, {@link DistributedTransaction#commit()}
   * or {@link TwoPhaseCommitTransaction#prepare()} may throw {@link CommitConflictException} or
   * {@link PreparationConflictException} respectively in case of a conflict error.
   *
   * @param insert a {@code Insert} command
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontranient
   */
  void insert(Insert insert) throws CrudConflictException, CrudException;

  /**
   * Inserts or updates an entry in the storage through a transaction with the specified {@link
   * Upsert} command. If the entry already exists, it is updated; otherwise, it is inserted.
   *
   * @param upsert a {@code Upsert} command
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontranient
   */
  void upsert(Upsert upsert) throws CrudConflictException, CrudException;

  /**
   * Updates an entry in the storage through a transaction with the specified {@link Update}
   * command. If a condition is specified in the {@link Update} command, and if the condition is not
   * satisfied or the entry does not exist, it throws {@link UnsatisfiedConditionException}. If no
   * condition is specified in the {@link Update} command and the entry does not exist, it throws
   * {@link RecordNotFoundException}.
   *
   * @param update an {@code Update} command
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontranient
   * @throws UnsatisfiedConditionException if a condition is specified, and if the condition is not
   *     satisfied or the entry does not exist
   * @throws RecordNotFoundException if no condition is specified and the entry does not exist
   */
  void update(Update update)
      throws CrudConflictException, CrudException, UnsatisfiedConditionException,
          RecordNotFoundException;

  /**
   * Mutates entries of the storage through a transaction with the specified list of {@link
   * Mutation} commands.
   *
   * @param mutations a list of {@code Mutation} commands
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontranient
   * @throws UnsatisfiedConditionException if a condition is specified in a {@link Put}, {@link
   *     Delete}, or {@link Update} command, and if the condition is not satisfied or the entry does
   *     not exist
   * @throws RecordNotFoundException if no condition is specified in an {@link Update} command and
   *     the entry does not exist
   */
  void mutate(List<? extends Mutation> mutations)
      throws CrudConflictException, CrudException, UnsatisfiedConditionException;
}
