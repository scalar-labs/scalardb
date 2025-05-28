package com.scalar.db.api;

import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnsatisfiedConditionException;
import java.util.List;
import java.util.Optional;

/**
 * An interface for transactional CRUD operations. Note that the LINEARIZABLE consistency level is
 * always used in transactional CRUD operations, so {@link Consistency} specified for CRUD
 * operations is ignored.
 *
 * @param <E> the type of {@link TransactionException} that the implementation throws if the
 *     operation fails
 */
public interface CrudOperable<E extends TransactionException> {

  /**
   * Retrieves a result from the storage through a transaction with the specified {@link Get}
   * command with a primary key and returns the result.
   *
   * @param get a {@code Get} command
   * @return an {@code Optional} with the returned result
   * @throws E if the transaction CRUD operation fails
   */
  Optional<Result> get(Get get) throws E;

  /**
   * Retrieves results from the storage through a transaction with the specified {@link Scan} or
   * {@link ScanAll} or {@link ScanWithIndex} command with a partition key and returns a list of
   * {@link Result}. Results can be filtered by specifying a range of clustering keys.
   *
   * <ul>
   *   <li>{@link Scan} : by specifying a partition key, it will return results within the
   *       partition. Results can be filtered by specifying a range of clustering keys.
   *   <li>{@link ScanAll} : for a given table, it will return all its records even if they span
   *       several partitions.
   *   <li>{@link ScanWithIndex} : by specifying an index key, it will return results within the
   *       index.
   * </ul>
   *
   * @param scan a {@code Scan} command
   * @return a list of {@link Result}
   * @throws E if the transaction CRUD operation fails
   */
  List<Result> scan(Scan scan) throws E;

  /**
   * Retrieves results from the storage through a transaction with the specified {@link Scan} or
   * {@link ScanAll} or {@link ScanWithIndex} command with a partition key and returns a {@link
   * Scanner} to iterate over the results. Results can be filtered by specifying a range of
   * clustering keys.
   *
   * @param scan a {@code Scan} command
   * @return a {@code Scanner} to iterate over the results
   * @throws E if the transaction CRUD operation fails
   */
  Scanner<E> getScanner(Scan scan) throws E;

  /**
   * Inserts an entry into or updates an entry in the underlying storage through a transaction with
   * the specified {@link Put} command. If a condition is specified in the {@link Put} command, and
   * if the condition is not satisfied or the entry does not exist, it throws {@link
   * UnsatisfiedConditionException}.
   *
   * @param put a {@code Put} command
   * @throws E if the transaction CRUD operation fails
   * @deprecated As of release 3.13.0. Will be removed in release 5.0.0.
   */
  @Deprecated
  void put(Put put) throws E;

  /**
   * Inserts multiple entries into or updates multiple entries in the underlying storage through a
   * transaction with the specified list of {@link Put} commands. If a condition is specified in the
   * {@link Put} command, and if the condition is not satisfied or the entry does not exist, it
   * throws {@link UnsatisfiedConditionException}.
   *
   * @param puts a list of {@code Put} commands
   * @throws E if the transaction CRUD operation fails
   * @deprecated As of release 3.13.0. Will be removed in release 5.0.0. Use {@link #mutate(List)}
   *     instead.
   */
  @Deprecated
  void put(List<Put> puts) throws E;

  /**
   * Inserts an entry into the underlying storage through a transaction with the specified {@link
   * Insert} command. If the entry already exists, a conflict error occurs. Note that the location
   * where the conflict error is thrown depends on the implementation of the transaction manager.
   * This method may throw {@link CrudConflictException}. Alternatively, {@link
   * DistributedTransaction#commit()} or {@link TwoPhaseCommitTransaction#prepare()} may throw
   * {@link CommitConflictException} or {@link PreparationConflictException} respectively in case of
   * a conflict error.
   *
   * @param insert a {@code Insert} command
   * @throws E if the transaction CRUD operation fails
   */
  void insert(Insert insert) throws E;

  /**
   * Inserts an entry into or updates an entry in the underlying storage through a transaction with
   * the specified {@link Upsert} command. If the entry already exists, it is updated; otherwise, it
   * is inserted.
   *
   * @param upsert a {@code Upsert} command
   * @throws E if the transaction CRUD operation fails
   */
  void upsert(Upsert upsert) throws E;

  /**
   * Updates an entry in the underlying storage through a transaction with the specified {@link
   * Update} command. If the entry does not exist, it does nothing. If a condition is specified in
   * the {@link Update} command, and if the condition is not satisfied or the entry does not exist,
   * it throws {@link UnsatisfiedConditionException}.
   *
   * @param update an {@code Update} command
   * @throws E if the transaction CRUD operation fails
   */
  void update(Update update) throws E;

  /**
   * Deletes an entry from the underlying storage through a transaction with the specified {@link
   * Delete} command. If a condition is specified in the {@link Delete} command, and if the
   * condition is not satisfied or the entry does not exist, it throws {@link
   * UnsatisfiedConditionException}.
   *
   * @param delete a {@code Delete} command
   * @throws E if the transaction CRUD operation fails
   */
  void delete(Delete delete) throws E;

  /**
   * Deletes entries from the underlying storage through a transaction with the specified list of
   * {@link Delete} commands. If a condition is specified in the {@link Delete} command, and if the
   * condition is not satisfied or the entry does not exist, it throws {@link
   * UnsatisfiedConditionException}.
   *
   * @param deletes a list of {@code Delete} commands
   * @throws E if the transaction CRUD operation fails
   * @deprecated As of release 3.13.0. Will be removed in release 5.0.0. Use {@link #mutate(List)}
   *     instead.
   */
  @Deprecated
  void delete(List<Delete> deletes) throws E;

  /**
   * Mutates entries of the underlying storage through a transaction with the specified list of
   * {@link Mutation} commands.
   *
   * @param mutations a list of {@code Mutation} commands
   * @throws E if the transaction CRUD operation fails
   */
  void mutate(List<? extends Mutation> mutations) throws E;

  /** A scanner abstraction for iterating results. */
  interface Scanner<E extends TransactionException> extends AutoCloseable, Iterable<Result> {
    /**
     * Returns the next result.
     *
     * @return an {@code Optional} containing the next result if available, or empty if no more
     *     results
     * @throws E if the operation fails
     */
    Optional<Result> one() throws E;

    /**
     * Returns all remaining results.
     *
     * @return a {@code List} containing all remaining results
     * @throws E if the operation fails
     */
    List<Result> all() throws E;

    /**
     * Closes the scanner.
     *
     * @throws E if closing the scanner fails
     */
    @Override
    void close() throws E;
  }
}
