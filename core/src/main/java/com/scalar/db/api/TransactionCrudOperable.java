package com.scalar.db.api;

import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
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
   * command.
   *
   * @param put a {@code Put} command
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontranient
   * @throws UnsatisfiedConditionException if the mutation condition is not satisfied
   */
  void put(Put put) throws CrudConflictException, CrudException, UnsatisfiedConditionException;

  /**
   * Inserts/Updates multiple entries to the storage through a transaction with the specified list
   * of {@link Put} commands.
   *
   * @param puts a list of {@code Put} commands
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontranient
   * @throws UnsatisfiedConditionException if the mutation condition is not satisfied
   */
  void put(List<Put> puts)
      throws CrudConflictException, CrudException, UnsatisfiedConditionException;

  /**
   * Deletes an entry from the storage through a transaction with the specified {@link Delete}
   * command.
   *
   * @param delete a {@code Delete} command
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontranient
   * @throws UnsatisfiedConditionException if the mutation condition is not satisfied
   */
  void delete(Delete delete)
      throws CrudConflictException, CrudException, UnsatisfiedConditionException;

  /**
   * Deletes entries from the storage through a transaction with the specified list of {@link
   * Delete} commands.
   *
   * @param deletes a list of {@code Delete} commands
   * @throws CrudConflictException if the transaction CRUD operation fails due to transient faults
   *     (e.g., a conflict error). You can retry the transaction from the beginning
   * @throws CrudException if the transaction CRUD operation fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontranient
   * @throws UnsatisfiedConditionException if the mutation condition is not satisfied
   */
  void delete(List<Delete> deletes)
      throws CrudConflictException, CrudException, UnsatisfiedConditionException;

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
   * @throws UnsatisfiedConditionException if the mutation condition is not satisfied
   */
  void mutate(List<? extends Mutation> mutations)
      throws CrudConflictException, CrudException, UnsatisfiedConditionException;
}
