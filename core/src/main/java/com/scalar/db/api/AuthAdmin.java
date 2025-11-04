package com.scalar.db.api;

import com.scalar.db.common.CoreError;
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
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
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
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Drops a user with the given username.
   *
   * @param username the username
   * @throws IllegalArgumentException if the user does not exist
   * @throws ExecutionException if the operation fails
   */
  default void dropUser(String username) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
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
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
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
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
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
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
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
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves a {@link User} for the given username.
   *
   * @param username the username
   * @return a {@link User} for the given username
   * @throws ExecutionException if the operation fails
   */
  default Optional<User> getUser(String username) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves a list of {@link User}s.
   *
   * @return a list of {@link User}s
   * @throws ExecutionException if the operation fails
   */
  default List<User> getUsers() throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves the current logged-in user.
   *
   * @return the current logged-in user
   * @throws ExecutionException if the operation fails
   */
  default User getCurrentUser() throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
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
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
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
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Creates a role with the given role name.
   *
   * @param roleName the role name
   * @throws IllegalArgumentException if the role already exists
   * @throws ExecutionException if the operation fails
   */
  default void createRole(String roleName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Drops a role with the given role name.
   *
   * @param roleName the role name
   * @throws IllegalArgumentException if the role does not exist
   * @throws ExecutionException if the operation fails
   */
  default void dropRole(String roleName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves a list of {@link RoleDetail}s.
   *
   * @return a list of {@link RoleDetail}s
   * @throws ExecutionException if the operation fails
   */
  default List<RoleDetail> getRoles() throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves a list of {@link UserRoleDetail}s for the given user.
   *
   * @param username the username
   * @return a list of {@link UserRoleDetail}s for the given user
   * @throws ExecutionException if the operation fails
   */
  default List<UserRoleDetail> getRolesForUser(String username) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Grants a role to a user.
   *
   * @param username the username
   * @param roleName the role name
   * @param withAdminOption if true, the user can grant the role to other users or roles
   * @throws IllegalArgumentException if the user does not exist or the role does not exist
   * @throws ExecutionException if the operation fails
   */
  default void grantRoleToUser(String username, String roleName, boolean withAdminOption)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Revokes a role from a user.
   *
   * @param username the username
   * @param roleName the role name
   * @throws IllegalArgumentException if the user does not exist or the role does not exist
   * @throws ExecutionException if the operation fails
   */
  default void revokeRoleFromUser(String username, String roleName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Revokes admin option from a user for a role.
   *
   * @param username the username
   * @param roleName the role name
   * @throws IllegalArgumentException if the user does not exist or the role does not exist
   * @throws ExecutionException if the operation fails
   */
  default void revokeAdminOptionFromUser(String username, String roleName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves a list of {@link UserRole}s for the given role.
   *
   * @param roleName the role name
   * @return a list of {@link UserRole}s for the given role
   * @throws ExecutionException if the operation fails
   */
  default List<UserRole> getUsersForRole(String roleName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Grants a member role to a role. Users or roles that have the role will inherit all privileges
   * from the member role.
   *
   * @param roleName the role name
   * @param memberRoleName the member role name to be granted to the role
   * @param withAdminOption if true, users or roles that have the role can grant the member role to
   *     other users or roles
   * @throws IllegalArgumentException if the role does not exist or the member role does not exist
   * @throws ExecutionException if the operation fails
   */
  default void grantRoleToRole(String roleName, String memberRoleName, boolean withAdminOption)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Revokes a role from another role.
   *
   * @param roleName the role name
   * @param memberRoleName the member role name
   * @throws IllegalArgumentException if the role does not exist or the member role does not exist
   * @throws ExecutionException if the operation fails
   */
  default void revokeRoleFromRole(String roleName, String memberRoleName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Revokes admin option from a role for another role.
   *
   * @param roleName the role name
   * @param memberRoleName the member role name
   * @throws IllegalArgumentException if the role does not exist or the member role does not exist
   * @throws ExecutionException if the operation fails
   */
  default void revokeAdminOptionFromRole(String roleName, String memberRoleName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves privileges for the given role and namespace.
   *
   * @param roleName the role name
   * @param namespaceName the namespace name
   * @return a set of privileges for the given role and namespace
   * @throws ExecutionException if the operation fails
   */
  default Set<Privilege> getRolePrivileges(String roleName, String namespaceName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves privileges for the given role, namespace, and table.
   *
   * @param roleName the role name
   * @param namespaceName the namespace name
   * @param tableName the table name
   * @return a set of privileges for the given role, namespace, and table
   * @throws ExecutionException if the operation fails
   */
  default Set<Privilege> getRolePrivileges(String roleName, String namespaceName, String tableName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Grants privileges to a role for all tables in the given namespace.
   *
   * @param roleName the role name
   * @param namespaceName the namespace name
   * @param privileges the privileges
   * @throws IllegalArgumentException if the role does not exist or the namespace does not exist
   * @throws ExecutionException if the operation fails
   */
  default void grantPrivilegeToRole(String roleName, String namespaceName, Privilege... privileges)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Grants privileges to a role for the given table.
   *
   * @param roleName the role name
   * @param namespaceName the namespace name of the table
   * @param tableName the table name
   * @param privileges the privileges
   * @throws IllegalArgumentException if the role does not exist or the table does not exist
   * @throws ExecutionException if the operation fails
   */
  default void grantPrivilegeToRole(
      String roleName, String namespaceName, String tableName, Privilege... privileges)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Revokes privileges from a role for all tables in the given namespace.
   *
   * @param roleName the role name
   * @param namespaceName the namespace name
   * @param privileges the privileges
   * @throws IllegalArgumentException if the role does not exist or the namespace does not exist
   * @throws ExecutionException if the operation fails
   */
  default void revokePrivilegeFromRole(
      String roleName, String namespaceName, Privilege... privileges) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  /**
   * Revokes privileges from a role for the given table.
   *
   * @param roleName the role name
   * @param namespaceName the namespace name of the table
   * @param tableName the table name
   * @param privileges the privileges
   * @throws IllegalArgumentException if the role does not exist or the table does not exist
   * @throws ExecutionException if the operation fails
   */
  default void revokePrivilegeFromRole(
      String roleName, String namespaceName, String tableName, Privilege... privileges)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.AUTH_NOT_ENABLED.buildMessage());
  }

  interface User {
    String getName();

    boolean isSuperuser();
  }

  /** Represents a role. */
  interface Role {
    String getName();
  }

  /** Represents a role with its hierarchy information. */
  interface RoleDetail {
    Role getRole();

    List<RoleHierarchy> getRoleHierarchies();
  }

  /**
   * Represents a role detail for a specific user, including whether the user has admin option for
   * this role.
   */
  interface UserRoleDetail extends RoleDetail {
    /**
     * Returns whether the user has admin option for this role. This is distinct from the admin
     * option in role hierarchies, which applies to role-to-role grants.
     */
    boolean hasAdminOptionOnUser();
  }

  /** Represents a user-role assignment. */
  interface UserRole {
    String getUsername();

    String getRoleName();

    boolean hasAdminOption();
  }

  /** Represents a role hierarchy (role-to-role assignment). */
  interface RoleHierarchy {
    /** Returns the role name. */
    String getRoleName();

    /** Returns the member role name granted to the role. */
    String getMemberRoleName();

    /** Returns whether admin option is granted for this hierarchy. */
    boolean hasAdminOption();
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
