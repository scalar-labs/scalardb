package com.scalar.db.api;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/** An interface for administrative operations for attribute-based access control. */
public interface AbacAdmin {

  /**
   * Creates a policy with the given name and data tag column name.
   *
   * @param policyName the policy name
   * @param dataTagColumnName the data tag column name. If null, the default data tag column name is
   *     used
   * @throws ExecutionException if the operation fails
   */
  default void createPolicy(String policyName, @Nullable String dataTagColumnName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Enables a policy that has the given name.
   *
   * @param policyName the policy name
   * @throws ExecutionException if the operation fails
   */
  default void enablePolicy(String policyName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Disables a policy that has the given name.
   *
   * @param policyName the policy name
   * @throws ExecutionException if the operation fails
   */
  default void disablePolicy(String policyName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves a policy that has the given name.
   *
   * @param policyName the policy name
   * @return the policy
   * @throws ExecutionException if the operation fails
   */
  default Optional<Policy> getPolicy(String policyName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves all policies.
   *
   * @return the policies
   * @throws ExecutionException if the operation fails
   */
  default List<Policy> getPolicies() throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Creates a level with the given short name, long name and level number for the given policy.
   *
   * @param policyName the policy name
   * @param levelShortName the short name of the level
   * @param levelLongName the long name of the level
   * @param levelNumber the level number
   * @throws ExecutionException if the operation fails
   */
  default void createLevel(
      String policyName, String levelShortName, String levelLongName, int levelNumber)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Drops a level that has the given short name for the given policy.
   *
   * @param policyName the policy name
   * @param levelShortName the short name of the level
   * @throws ExecutionException if the operation fails
   */
  default void dropLevel(String policyName, String levelShortName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves a level that has the given short name for the given policy.
   *
   * @param policyName the policy name
   * @param levelShortName the short name of the level
   * @return the level
   * @throws ExecutionException if the operation fails
   */
  default Optional<Level> getLevel(String policyName, String levelShortName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves all levels for the given policy.
   *
   * @param policyName the policy name
   * @return the levels
   * @throws ExecutionException if the operation fails
   */
  default List<Level> getLevels(String policyName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Creates a compartment with the given short name and long name for the given policy.
   *
   * @param policyName the policy name
   * @param compartmentShortName the short name of the compartment
   * @param compartmentLongName the long name of the compartment
   * @throws ExecutionException if the operation fails
   */
  default void createCompartment(
      String policyName, String compartmentShortName, String compartmentLongName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Drops a compartment that has the given short name for the given policy.
   *
   * @param policyName the policy name
   * @param compartmentShortName the short name of the compartment
   * @throws ExecutionException if the operation fails
   */
  default void dropCompartment(String policyName, String compartmentShortName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves a compartment that has the given short name for the given policy.
   *
   * @param policyName the policy name
   * @param compartmentShortName the short name of the compartment
   * @return the compartment
   * @throws ExecutionException if the operation fails
   */
  default Optional<Compartment> getCompartment(String policyName, String compartmentShortName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves all compartments for the given policy.
   *
   * @param policyName the policy name
   * @return the compartments
   * @throws ExecutionException if the operation fails
   */
  default List<Compartment> getCompartments(String policyName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Creates a group with the given short name, long name, and the short name of the parent group
   * for the given policy.
   *
   * @param policyName the policy name
   * @param groupShortName the short name of the group
   * @param groupLongName the long name of the group
   * @param parentGroupShortName the short name of the parent group. If null, the group is a
   *     top-level group
   * @throws ExecutionException if the operation fails
   */
  default void createGroup(
      String policyName,
      String groupShortName,
      String groupLongName,
      @Nullable String parentGroupShortName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Drops a group that has the given short name for the given policy.
   *
   * @param policyName the policy name
   * @param groupShortName the short name of the group
   * @throws ExecutionException if the operation fails
   */
  default void dropGroup(String policyName, String groupShortName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves a group that has the given short name for the given policy.
   *
   * @param policyName the policy name
   * @param groupShortName the short name of the group
   * @return the group
   * @throws ExecutionException if the operation fails
   */
  default Optional<Group> getGroup(String policyName, String groupShortName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves all groups for the given policy.
   *
   * @param policyName the policy name
   * @return the groups
   * @throws ExecutionException if the operation fails
   */
  default List<Group> getGroups(String policyName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Sets the given levels of the given policy to a user that has the given username.
   *
   * @param policyName the policy name
   * @param username the username
   * @param levelShortName the short name of the level to set
   * @param defaultLevelShortName the short name of the default level. If null, the {@code
   *     levelShortName} will be used as the default level
   * @param rowLevelShortName the short name of the row level. If null, the {@code
   *     defaultLevelShortName} will be used as the row level
   * @throws ExecutionException if the operation fails
   */
  default void setLevelsToUser(
      String policyName,
      String username,
      String levelShortName,
      @Nullable String defaultLevelShortName,
      @Nullable String rowLevelShortName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Adds the given compartment of the given policy to a user that has the given username. Before
   * adding the compartment, levels must be set to the user.
   *
   * @param policyName the policy name
   * @param username the username
   * @param compartmentShortName the short name of the compartment to set
   * @param accessMode the access mode
   * @param defaultCompartment whether the compartment is the default compartment
   * @param rowCompartment whether the compartment is the row compartment
   * @throws ExecutionException if the operation fails
   */
  default void addCompartmentToUser(
      String policyName,
      String username,
      String compartmentShortName,
      AccessMode accessMode,
      boolean defaultCompartment,
      boolean rowCompartment)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Removes the given compartment of the given policy from a user that has the given username.
   *
   * @param policyName the policy name
   * @param username the username
   * @param compartmentShortName the short name of the compartment to remove
   * @throws ExecutionException if the operation fails
   */
  default void removeCompartmentFromUser(
      String policyName, String username, String compartmentShortName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Adds the given group of the given policy to a user that has the given username. Before adding
   * the group, levels must be set to the user.
   *
   * @param policyName the policy name
   * @param username the username
   * @param groupShortName the short name of the group to set
   * @param accessMode the access mode
   * @param defaultGroup whether the group is the default group
   * @param rowGroup whether the group is the row group
   * @throws ExecutionException if the operation fails
   */
  default void addGroupToUser(
      String policyName,
      String username,
      String groupShortName,
      AccessMode accessMode,
      boolean defaultGroup,
      boolean rowGroup)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Removes the given group of the given policy from a user that has the given username.
   *
   * @param policyName the policy name
   * @param username the username
   * @param groupShortName the short name of the group to remove
   * @throws ExecutionException if the operation fails
   */
  default void removeGroupFromUser(String policyName, String username, String groupShortName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Drops the user tag information of a user with the given username for the given policy.
   *
   * @param policyName the policy name
   * @param username the username
   * @throws ExecutionException if the operation fails
   */
  default void dropUserTagInfoFromUser(String policyName, String username)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves the user tag information of a user with the given username for the given policy.
   *
   * @param policyName the policy name
   * @param username the username
   * @return the user tag information. If the user tag information is not registered, returns an
   *     empty optional
   * @throws ExecutionException if the operation fails
   */
  default Optional<UserTagInfo> getUserTagInfo(String policyName, String username)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Applies the given policy to the given namespace.
   *
   * @param policyName the policy name
   * @param namespaceName the namespace name
   * @throws ExecutionException if the operation fails
   */
  default void applyPolicyToNamespace(String policyName, String namespaceName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Enables the given policy for the given namespace.
   *
   * @param policyName the policy name
   * @param namespaceName the namespace name
   * @throws ExecutionException if the operation fails
   */
  default void enableNamespacePolicy(String policyName, String namespaceName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Disables the given policy for the given namespace.
   *
   * @param policyName the policy name
   * @param namespaceName the namespace name
   * @throws ExecutionException if the operation fails
   */
  default void disableNamespacePolicy(String policyName, String namespaceName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves the namespace policy for the given namespace.
   *
   * @param policyName the policy name
   * @param namespaceName the namespace name
   * @return the namespace policy. If the policy is not applied to the namespace, returns an empty
   *     optional
   * @throws ExecutionException if the operation fails
   */
  default Optional<NamespacePolicy> getNamespacePolicy(String policyName, String namespaceName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves all namespace policies.
   *
   * @return the namespaces policies
   * @throws ExecutionException if the operation fails
   */
  default List<NamespacePolicy> getNamespacePolicies() throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Applies the given policy to the given table of the given namespace.
   *
   * @param policyName the policy name
   * @param namespaceName the namespace name
   * @param tableName the table name
   * @throws ExecutionException if the operation fails
   */
  default void applyPolicyToTable(String policyName, String namespaceName, String tableName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Enables the given policy of the given table of the given namespace.
   *
   * @param policyName the policy name
   * @param namespaceName the namespace name
   * @param tableName the table name
   * @throws ExecutionException if the operation fails
   */
  default void enableTablePolicy(String policyName, String namespaceName, String tableName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Disables the given policy of the given table of the given namespace.
   *
   * @param policyName the policy name
   * @param namespaceName the namespace name
   * @param tableName the table name
   * @throws ExecutionException if the operation fails
   */
  default void disableTablePolicy(String policyName, String namespaceName, String tableName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves the table policy for the given table of the given namespace.
   *
   * @param policyName the policy name
   * @param namespaceName the namespace name
   * @param tableName the table name
   * @return the table policy. If the policy is not applied to the table, returns an empty optional
   * @throws ExecutionException if the operation fails
   */
  default Optional<TablePolicy> getTablePolicy(
      String policyName, String namespaceName, String tableName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves all table policies.
   *
   * @return the table policies
   * @throws ExecutionException if the operation fails
   */
  default List<TablePolicy> getTablePolicies() throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /** The state of a policy. */
  enum PolicyState {
    /** The policy is enabled. */
    ENABLED,

    /** The policy is disabled. */
    DISABLED
  }

  /** The access mode for compartments and groups. */
  enum AccessMode {
    /** The access mode for read only. */
    READ_ONLY,

    /** The access mode for read and write. */
    READ_WRITE
  }

  /** A policy for ABAC. All components of ABAC are associated with a policy. */
  interface Policy {
    /**
     * Returns the policy name.
     *
     * @return the policy name
     */
    String getName();

    /**
     * Returns the data tag column name.
     *
     * @return the data tag column name
     */
    String getDataTagColumnName();

    /**
     * Returns the state of the policy.
     *
     * @return the state
     */
    PolicyState getState();
  }

  /** A level that is one of the components of a tag in ABAC. */
  interface Level {
    /**
     * Returns the policy name.
     *
     * @return the policy name
     */
    String getPolicyName();

    /**
     * Returns the short name of the level.
     *
     * @return the short name of the level
     */
    String getShortName();

    /**
     * Returns the long name of the level.
     *
     * @return the long name of the level
     */
    String getLongName();

    /**
     * Returns the level number.
     *
     * @return the level number
     */
    int getLevelNumber();
  }

  /** A compartment that is one of the components of a tag in ABAC. */
  interface Compartment {
    /**
     * Returns the policy name.
     *
     * @return the policy name
     */
    String getPolicyName();

    /**
     * Returns the short name of the compartment.
     *
     * @return the short name of the compartment
     */
    String getShortName();

    /**
     * Returns the long name of the compartment.
     *
     * @return the long name of the compartment
     */
    String getLongName();
  }

  /** A group that is one of the components of a tag in ABAC. */
  interface Group {
    /**
     * Returns the policy name.
     *
     * @return the policy name
     */
    String getPolicyName();

    /**
     * Returns the short name of the group.
     *
     * @return the short name of the group
     */
    String getShortName();

    /**
     * Returns the long name of the group.
     *
     * @return the long name
     */
    String getLongName();

    /**
     * Returns the parent group short name if the group is not a top-level group.
     *
     * @return the parent group short name. If the group is a top-level group, returns an empty
     *     optional
     */
    Optional<String> getParentGroupShortName();
  }

  /** The user tag information of a user for a policy in ABAC. */
  interface UserTagInfo {

    /** The level information. */
    interface LevelInfo {
      /**
       * Returns the short name of the level.
       *
       * @return the short name of the level
       */
      String getLevelShortName();

      /**
       * Returns the short name of the default level.
       *
       * @return the short name of the default level
       */
      String getDefaultLevelShortName();

      /**
       * Returns the short name of the row level.
       *
       * @return the short name of the row level
       */
      String getRowLevelShortName();
    }

    /** The compartment information. */
    interface CompartmentInfo {
      /**
       * Returns the short names of the compartments that the user has read access to.
       *
       * @return the short names of the compartments that the user has read access to
       */
      List<String> getReadCompartmentShortNames();

      /**
       * Returns the short names of the compartments that the user has write access to.
       *
       * @return the short names of the compartments that the user has write access to
       */
      List<String> getWriteCompartmentShortNames();

      /**
       * Returns the short names of the default compartments that the user has read access to.
       *
       * @return the short names of the default compartments that the user has read access to
       */
      List<String> getDefaultReadCompartmentShortNames();

      /**
       * Returns the short names of the default compartments that the user has write access to.
       *
       * @return the short names of
       */
      List<String> getDefaultWriteCompartmentShortNames();

      /**
       * Returns the short names of the row compartments.
       *
       * @return the short names of the row compartments
       */
      List<String> getRowCompartmentShortNames();
    }

    /** The group information. */
    interface GroupInfo {
      /**
       * Returns the short names of the groups that the user has read access to.
       *
       * @return the short names of the groups that the user has read access to
       */
      List<String> getReadGroupShortNames();

      /**
       * Returns the short names of the groups that the user has write access to.
       *
       * @return the short names of the groups that the user has write access to
       */
      List<String> getWriteGroupShortNames();

      /**
       * Returns the short names of the default groups that the user has read access to.
       *
       * @return the short names of the default groups that the user has read access to
       */
      List<String> getDefaultReadGroupShortNames();

      /**
       * Returns the short names of the default groups that the user has write access to.
       *
       * @return the short names of the default groups that the user has write access to
       */
      List<String> getDefaultWriteGroupShortNames();

      /**
       * Returns the short names of the row groups.
       *
       * @return the short names of the row groups.
       */
      List<String> getRowGroupShortNames();
    }

    /**
     * Returns the policy name.
     *
     * @return the policy name
     */
    String getPolicyName();

    /**
     * Returns the username.
     *
     * @return the username
     */
    String getUsername();

    /**
     * Returns the level information.
     *
     * @return the level information
     */
    LevelInfo getLevelInfo();

    /**
     * Returns the compartment information.
     *
     * @return the compartment information
     */
    CompartmentInfo getCompartmentInfo();

    /**
     * Returns the group information.
     *
     * @return the group information
     */
    GroupInfo getGroupInfo();
  }

  /** The namespace policy. */
  interface NamespacePolicy {
    /**
     * Returns the policy name.
     *
     * @return the policy name
     */
    String getPolicyName();

    /**
     * Returns the namespace name.
     *
     * @return the namespace name
     */
    String getNamespaceName();

    /**
     * Returns the state of the policy.
     *
     * @return the state of the policy
     */
    PolicyState getState();
  }

  /** The table policy. */
  interface TablePolicy {
    /**
     * Returns the policy name.
     *
     * @return the policy name
     */
    String getPolicyName();

    /**
     * Returns the namespace name.
     *
     * @return the namespace name
     */
    String getNamespaceName();

    /**
     * Returns the table name.
     *
     * @return the table name
     */
    String getTableName();

    /**
     * Returns the state of the policy.
     *
     * @return the state of the policy
     */
    PolicyState getState();
  }
}
