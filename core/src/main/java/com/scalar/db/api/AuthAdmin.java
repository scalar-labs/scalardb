package com.scalar.db.api;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * An interface for administrative operations for authentication and authorization. This interface
 * is used for creating and dropping users, and granting and revoking privileges.
 */
public interface AuthAdmin {

  /**
   * Creates a user with the given username, password and user options. If the password is null, the
   * user is created without a password.
   *
   * @param username the username
   * @param password the password. If null, the user is created without a password
   * @param userOptions the user options
   * @throws IllegalArgumentException if the user already exists
   * @throws ExecutionException if the operation fails
   */
  default void createUser(String username, @Nullable String password, UserOption... userOptions)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.NOT_SUPPORTED_IN_COMMUNITY_EDITION.buildMessage());
  }

  /**
   * Alters a user with the given username, password and user options. If the password is null, the
   * password is not changed. If empty, the password is deleted.
   *
   * @param username the username
   * @param password the password. If null, the password is not changed. If empty, the password is
   *     deleted
   * @param userOptions the user options
   * @throws IllegalArgumentException if the user does not exist
   * @throws ExecutionException if the operation fails
   */
  default void alterUser(String username, @Nullable String password, UserOption... userOptions)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.NOT_SUPPORTED_IN_COMMUNITY_EDITION.buildMessage());
  }

  /**
   * Drops a user with the given username.
   *
   * @param username the username
   * @throws IllegalArgumentException if the user does not exist
   * @throws ExecutionException if the operation fails
   */
  default void dropUser(String username) throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.NOT_SUPPORTED_IN_COMMUNITY_EDITION.buildMessage());
  }

  /**
   * Grants privileges to a user for the given table.
   *
   * @param username the username
   * @param namespaceName the namespace name of the table
   * @param tableName the table name
   * @param privileges the privileges
   * @throws IllegalArgumentException if the user does not exist or the table does not exist
   * @throws ExecutionException if the operation fails
   */
  default void grant(
      String username, String namespaceName, String tableName, Privilege... privileges)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.NOT_SUPPORTED_IN_COMMUNITY_EDITION.buildMessage());
  }

  /**
   * Grants privileges to a user for all tables in the given namespace.
   *
   * @param username the username
   * @param namespaceName the namespace name
   * @param privileges the privileges
   * @throws IllegalArgumentException if the user does not exist or the namespace does not exist
   * @throws ExecutionException if the operation fails
   */
  default void grant(String username, String namespaceName, Privilege... privileges)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.NOT_SUPPORTED_IN_COMMUNITY_EDITION.buildMessage());
  }

  /**
   * Revokes privileges from a user for the given table.
   *
   * @param username the username
   * @param namespaceName the namespace name of the table
   * @param tableName the table name
   * @param privileges the privileges
   * @throws IllegalArgumentException if the user does not exist or the table does not exist
   * @throws ExecutionException if the operation fails
   */
  default void revoke(
      String username, String namespaceName, String tableName, Privilege... privileges)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.NOT_SUPPORTED_IN_COMMUNITY_EDITION.buildMessage());
  }

  /**
   * Revokes privileges from a user for all tables in the given namespace.
   *
   * @param username the username
   * @param namespaceName the namespace name
   * @param privileges the privileges
   * @throws IllegalArgumentException if the user does not exist or the namespace does not exist
   * @throws ExecutionException if the operation fails
   */
  default void revoke(String username, String namespaceName, Privilege... privileges)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.NOT_SUPPORTED_IN_COMMUNITY_EDITION.buildMessage());
  }

  /**
   * Retrieves a {@link User} for the given username.
   *
   * @param username the username
   * @return a {@link User} for the given username
   * @throws ExecutionException if the operation fails
   */
  default Optional<User> getUser(String username) throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.NOT_SUPPORTED_IN_COMMUNITY_EDITION.buildMessage());
  }

  /**
   * Retrieves a list of {@link User}s.
   *
   * @return a list of {@link User}s
   * @throws ExecutionException if the operation fails
   */
  default List<User> getUsers() throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.NOT_SUPPORTED_IN_COMMUNITY_EDITION.buildMessage());
  }

  /**
   * Retrieves privileges for the given table for the given user.
   *
   * @param username the username
   * @param namespaceName the namespace name of the table
   * @param tableName the table name
   * @return a set of privileges
   * @throws IllegalArgumentException if the user does not exist or the table does not exist
   * @throws ExecutionException if the operation fails
   */
  default Set<Privilege> getPrivileges(String username, String namespaceName, String tableName)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.NOT_SUPPORTED_IN_COMMUNITY_EDITION.buildMessage());
  }

  /**
   * Retrieves privileges for all tables in the given namespace for the given user.
   *
   * @param username the username
   * @param namespaceName the namespace name
   * @return a set of privileges
   * @throws IllegalArgumentException if the user does not exist or the namespace does not exist
   * @throws ExecutionException if the operation fails
   */
  default Set<Privilege> getPrivileges(String username, String namespaceName)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.NOT_SUPPORTED_IN_COMMUNITY_EDITION.buildMessage());
  }

  interface User {
    String getName();

    boolean isSuperuser();
  }

  /** The user options. */
  enum UserOption {
    /** If specified, the user is created as a superuser. */
    SUPERUSER,

    /**
     * If specified, the user is created as a non-superuser. If neither SUPERUSER nor NO_SUPERUSER
     * is specified, the user is created as a non-superuser.
     */
    NO_SUPERUSER,
  }

  /** The privileges for ScalarDB operations. */
  enum Privilege {
    /** The privilege for read (Get and Scan) operations. */
    READ,

    /** The privilege for write (Put, Insert, Upsert, and Update) operations. */
    WRITE,

    /** The privilege for delete (Delete) operations. */
    DELETE,

    /** The privilege for creating tables and indexes. */
    CREATE,

    /** The privilege for dropping tables and indexes. */
    DROP,

    /** The privilege for truncating tables. */
    TRUNCATE,

    /** The privilege for altering tables. */
    ALTER,

    /** The privilege for granting and revoking privileges on tables to other users. */
    GRANT
  }
}
