package com.scalar.db.api;

import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
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
   * @throws CrudConflictException if conflicts happened. You can retry the transaction in this case
   * @throws CrudException if the operation fails
   */
  Optional<Result> get(Get get) throws CrudConflictException, CrudException;

  /**
   * Retrieves results from the storage through a transaction with the specified {@link Scan}
   * command with a partition key and returns a list of {@link Result}. Results can be filtered by
   * specifying a range of clustering keys.
   *
   * @param scan a {@code Scan} command
   * @return a list of {@link Result}
   * @throws CrudConflictException if conflicts happened. You can retry the transaction in this case
   * @throws CrudException if the operation fails
   */
  List<Result> scan(Scan scan) throws CrudConflictException, CrudException;

  /**
   * Inserts/Updates an entry to the storage through a transaction with the specified {@link Put}
   * command. Note that the conditions set in Put will be ignored. Please program such conditions in
   * a transaction if you want to implement conditional mutation.
   *
   * @param put a {@code Put} command
   * @throws CrudConflictException if conflicts happened. You can retry the transaction in this case
   * @throws CrudException if the operation fails
   */
  void put(Put put) throws CrudConflictException, CrudException;

  /**
   * Inserts/Updates multiple entries to the storage through a transaction with the specified list
   * of {@link Put} commands. Note that the conditions set in Put will be ignored. Please program
   * such conditions in a transaction if you want to implement conditional mutation.
   *
   * @param puts a list of {@code Put} commands
   * @throws CrudConflictException if conflicts happened. You can retry the transaction in this case
   * @throws CrudException if the operation fails
   */
  void put(List<Put> puts) throws CrudConflictException, CrudException;

  /**
   * Deletes an entry from the storage through a transaction with the specified {@link Delete}
   * command. Note that the conditions set in Delete will be ignored. Please program such conditions
   * in a transaction if you want to implement conditional mutation.
   *
   * @param delete a {@code Delete} command
   * @throws CrudConflictException if conflicts happened. You can retry the transaction in this case
   * @throws CrudException if the operation fails
   */
  void delete(Delete delete) throws CrudConflictException, CrudException;

  /**
   * Deletes entries from the storage through a transaction with the specified list of {@link
   * Delete} commands. Note that the conditions set in Delete will be ignored. Please program such
   * conditions in a transaction if you want to implement conditional mutation.
   *
   * @param deletes a list of {@code Delete} commands
   * @throws CrudConflictException if conflicts happened. You can retry the transaction in this case
   * @throws CrudException if the operation fails
   */
  void delete(List<Delete> deletes) throws CrudConflictException, CrudException;

  /**
   * Mutates entries of the storage through a transaction with the specified list of {@link
   * Mutation} commands. Note that the conditions set in Mutation will be ignored. Please program
   * such conditions in a transaction if you want to implement conditional mutation.
   *
   * @param mutations a list of {@code Mutation} commands
   * @throws CrudConflictException if conflicts happened. You can retry the transaction in this case
   * @throws CrudException if the operation fails
   */
  void mutate(List<? extends Mutation> mutations) throws CrudConflictException, CrudException;
}
