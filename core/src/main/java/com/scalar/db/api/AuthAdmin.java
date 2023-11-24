package com.scalar.db.api;

import com.scalar.db.exception.storage.ExecutionException;
import java.util.EnumSet;
import java.util.List;
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
   * @param userOption the user options
   * @throws ExecutionException if the operation fails
   */
  default void createUser(String username, @Nullable String password, UserOption... userOption)
      throws ExecutionException {
    throw new UnsupportedOperationException("Not supported in the community edition");
  }

  /**
   * Alters a user with the given username, password and user options. If the password is null, the
   * password is not changed.
   *
   * @param username the username
   * @param password the password. If null, the password is not changed
   * @param userOption the user options
   * @throws ExecutionException if the operation fails
   */
  default void alterUser(String username, @Nullable String password, UserOption... userOption)
      throws ExecutionException {
    throw new UnsupportedOperationException("Not supported in the community edition");
  }

  /**
   * Drops a user with the given username.
   *
   * @param username the username
   * @throws ExecutionException if the operation fails
   */
  default void dropUser(String username) throws ExecutionException {
    throw new UnsupportedOperationException("Not supported in the community edition");
  }

  /**
   * Grants privileges to a user for the given table.
   *
   * @param username the username
   * @param namespaceName the namespace name of the table
   * @param tableName the table name
   * @param privileges the privileges
   * @throws ExecutionException if the user does not exist or the operation fails
   */
  default void grant(
      String username, String namespaceName, String tableName, Privilege... privileges)
      throws ExecutionException {
    throw new UnsupportedOperationException("Not supported in the community edition");
  }

  /**
   * Grants privileges to a user for the given namespace.
   *
   * @param username the username
   * @param namespaceName the namespace name
   * @param privileges the privileges
   * @throws ExecutionException if the user does not exist or the operation fails
   */
  default void grant(String username, String namespaceName, Privilege... privileges)
      throws ExecutionException {
    throw new UnsupportedOperationException("Not supported in the community edition");
  }

  /**
   * Revokes privileges from a user for the given table.
   *
   * @param username the username
   * @param namespaceName the namespace name of the table
   * @param tableName the table name
   * @param privileges the privileges
   * @throws ExecutionException if the user does not exist or the operation fails
   */
  default void revoke(
      String username, String namespaceName, String tableName, Privilege... privileges)
      throws ExecutionException {
    throw new UnsupportedOperationException("Not supported in the community edition");
  }

  /**
   * Revokes a privilege from a user for the given namespace.
   *
   * @param username the username
   * @param namespaceName the namespace name
   * @param privileges the privileges
   * @throws ExecutionException if the user does not exist or the operation fails
   */
  default void revoke(String username, String namespaceName, Privilege... privileges)
      throws ExecutionException {
    throw new UnsupportedOperationException("Not supported in the community edition");
  }

  /**
   * Retrieve a list of usernames.
   *
   * @return a list of users
   * @throws ExecutionException if the operation fails
   */
  default List<User> getUsers() throws ExecutionException {
    throw new UnsupportedOperationException("Not supported in the community edition");
  }

  /**
   * Retrieve privileges for the given namespace for the given user.
   *
   * @param username the username
   * @param namespaceName the namespace name
   * @return a set of privileges
   * @throws ExecutionException if the user does not exist or the operation fails
   */
  default EnumSet<Privilege> getPrivileges(String username, String namespaceName)
      throws ExecutionException {
    throw new UnsupportedOperationException("Not supported in the community edition");
  }

  /**
   * Retrieve privileges for the given table for the given user.
   *
   * @param username the username
   * @param namespaceName the namespace name of the table
   * @param tableName the table name
   * @return a set of privileges
   * @throws ExecutionException if the user does not exist or the operation fails
   */
  default EnumSet<Privilege> getPrivileges(String username, String namespaceName, String tableName)
      throws ExecutionException {
    throw new UnsupportedOperationException("Not supported in the community edition");
  }

  interface User {
    String getUsername();

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

    /** The privilege for write (Put) operations. */
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

    /** The privilege for granting and revoking privileges. */
    GRANT
  }
}
