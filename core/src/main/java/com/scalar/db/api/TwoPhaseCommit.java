package com.scalar.db.api;

import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PrepareException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationException;
import java.util.List;
import java.util.Optional;

/**
 * A transaction abstraction based on a two-phase commit protocol for interacting with the
 * underlining storage and database implementations.
 */
public interface TwoPhaseCommit {

  /**
   * Returns the ID of a transaction.
   *
   * @return the ID of a transaction
   */
  String getId();

  /**
   * Sets the specified namespace and the table name as default values in the instance
   *
   * @param namespace default namespace to operate for
   * @param tableName default table name to operate for
   */
  void with(String namespace, String tableName);

  /**
   * Sets the specified namespace as a default value in the instance
   *
   * @param namespace default namespace to operate for
   */
  void withNamespace(String namespace);

  /**
   * Returns the namespace
   *
   * @return an {@code Optional} with the namespace
   */
  Optional<String> getNamespace();

  /**
   * Sets the specified table name as a default value in the instance
   *
   * @param tableName default table name to operate for
   */
  void withTable(String tableName);

  /**
   * Returns the table name
   *
   * @return an {@code Optional} with the table name
   */
  Optional<String> getTable();

  /**
   * Retrieves a result from the storage through a transaction with the specified {@link Get}
   * command with a primary key and returns the result.
   *
   * @param get a {@code Get} command
   * @return an {@code Optional} with the returned result
   * @throws CrudException if the operation failed
   */
  Optional<Result> get(Get get) throws CrudException;

  /**
   * Retrieves results from the storage through a transaction with the specified {@link Scan}
   * command with a partition key and returns a list of {@link Result}. Results can be filtered by
   * specifying a range of clustering keys.
   *
   * @param scan a {@code Scan} command
   * @return a list of {@link Result}
   * @throws CrudException if the operation failed
   */
  List<Result> scan(Scan scan) throws CrudException;

  /**
   * Inserts/Updates an entry to the storage through a transaction with the specified {@link Put}
   * command. Note that the conditions set in Put will be ignored. Please program such conditions in
   * a transaction if you want to implement conditional mutation.
   *
   * @param put a {@code Put} command
   * @throws CrudException if the operation failed
   */
  void put(Put put) throws CrudException;

  /**
   * Inserts/Updates multiple entries to the storage through a transaction with the specified list
   * of {@link Put} commands. Note that the conditions set in Put will be ignored. Please program
   * such conditions in a transaction if you want to implement conditional mutation.
   *
   * @param puts a list of {@code Put} commands
   * @throws CrudException if the operation failed
   */
  void put(List<Put> puts) throws CrudException;

  /**
   * Deletes an entry from the storage through a transaction with the specified {@link Delete}
   * command. Note that the conditions set in Delete will be ignored. Please program such conditions
   * in a transaction if you want to implement conditional mutation.
   *
   * @param delete a {@code Delete} command
   * @throws CrudException if the operation failed
   */
  void delete(Delete delete) throws CrudException;

  /**
   * Deletes entries from the storage through a transaction with the specified list of {@link
   * Delete} commands. Note that the conditions set in Delete will be ignored. Please program such
   * conditions in a transaction if you want to implement conditional mutation.
   *
   * @param deletes a list of {@code Delete} commands
   * @throws CrudException if the operation failed
   */
  void delete(List<Delete> deletes) throws CrudException;

  /**
   * Mutates entries of the storage through a transaction with the specified list of {@link
   * Mutation} commands.
   *
   * @param mutations a list of {@code Mutation} commands
   * @throws CrudException if the operation failed
   */
  void mutate(List<? extends Mutation> mutations) throws CrudException;

  /**
   * Prepares a transaction.
   *
   * @throws PrepareException if the operation fails
   */
  void prepare() throws PrepareException;

  /**
   * Validates a transaction. Depending on the concurrency control algorithm, you need a validation
   * phase for a transaction.
   *
   * @throws ValidationException if the operation fails
   */
  void validate() throws ValidationException;

  /**
   * Commits a transaction.
   *
   * @throws CommitException if the operation fails
   * @throws UnknownTransactionStatusException if the status of the commit is unknown
   */
  void commit() throws CommitException, UnknownTransactionStatusException;

  /**
   * Rolls back a transaction.
   *
   * @throws RollbackException if the operation fails
   */
  void rollback() throws RollbackException;
}
