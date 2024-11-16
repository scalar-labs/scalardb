package com.scalar.db.api;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/** An interface for administrative operations for Attribute-Based Access Control. */
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
   * Enables a policy with the given name.
   *
   * @param policyName the policy name
   * @throws ExecutionException if the operation fails
   */
  default void enablePolicy(String policyName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Disables a policy with the given name.
   *
   * @param policyName the policy name
   * @throws ExecutionException if the operation fails
   */
  default void disablePolicy(String policyName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves a policy with the given name.
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
   * @param levelShortName the level short name
   * @param levelLongName the level long name
   * @param levelNumber the level number
   * @throws ExecutionException if the operation fails
   */
  default void createLevel(
      String policyName, String levelShortName, String levelLongName, int levelNumber)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Drops a level with the given short name for the given policy.
   *
   * @param policyName the policy name
   * @param levelShortName the level short name
   * @throws ExecutionException if the operation fails
   */
  default void dropLevel(String policyName, String levelShortName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves a level with the given short name for the given policy.
   *
   * @param policyName the policy name
   * @param levelShortName the level short name
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
   * @param compartmentShortName the compartment short name
   * @param compartmentLongName the compartment long name
   * @throws ExecutionException if the operation fails
   */
  default void createCompartment(
      String policyName, String compartmentShortName, String compartmentLongName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Drops a compartment with the given short name for the given policy.
   *
   * @param policyName the policy name
   * @param compartmentShortName the compartment short name
   * @throws ExecutionException if the operation fails
   */
  default void dropCompartment(String policyName, String compartmentShortName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves a compartment with the given short name for the given policy.
   *
   * @param policyName the policy name
   * @param compartmentShortName the compartment short name
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
   * Creates a group with the given short name, long name and parent group short name for the given
   * policy.
   *
   * @param policyName the policy name
   * @param groupShortName the group short name
   * @param groupLongName the group long name
   * @param parentGroupShortName the parent group short name. If null, the group is a top-level
   *     group
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
   * Drops a group with the given short name for the given policy.
   *
   * @param policyName the policy name
   * @param groupShortName the group short name
   * @throws ExecutionException if the operation fails
   */
  default void dropGroup(String policyName, String groupShortName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Retrieves a group with the given short name for the given policy.
   *
   * @param policyName the policy name
   * @param groupShortName the group short name
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
   * Sets levels to a user with the given username for the given policy.
   *
   * @param policyName the policy name
   * @param username the username
   * @param levelShortName the level short name
   * @param defaultLevelShortName the default level short name. If null, the default is the level
   * @param rowLevelShortName the row level short name. If null, the default is the default level
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
   * Adds a compartment to a user with the given username for the given policy. Before adding the
   * compartment, levels must be set to the user.
   *
   * @param policyName the policy name
   * @param username the username
   * @param compartmentShortName the compartment short name
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
   * Removes a compartment from a user with the given username for the given policy.
   *
   * @param policyName the policy name
   * @param username the username
   * @param compartmentShortName the compartment short name
   * @throws ExecutionException if the operation fails
   */
  default void removeCompartmentFromUser(
      String policyName, String username, String compartmentShortName) throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Adds a group to a user with the given username for the given policy. Before adding the group,
   * levels must be set to the user.
   *
   * @param policyName the policy name
   * @param username the username
   * @param groupShortName the group short name
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
   * Removes a group from a user with the given username for the given policy.
   *
   * @param policyName the policy name
   * @param username the username
   * @param groupShortName the group short name
   * @throws ExecutionException if the operation fails
   */
  default void removeGroupFromUser(String policyName, String username, String groupShortName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Drops the user access for the given username for the given policy.
   *
   * @param policyName the policy name
   * @param username the username
   * @throws ExecutionException if the operation fails
   */
  default void dropUserAccess(String policyName, String username) throws ExecutionException {
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
   * Applies a policy to a namespace.
   *
   * @param policyName the policy name
   * @param namespaceName the namespace name
   * @throws ExecutionException if the operation fails
   */
  default void applyNamespacePolicy(String policyName, String namespaceName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Enables a namespace policy.
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
   * Disables a namespace policy.
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
   * Retrieves the namespace policies.
   *
   * @return the namespaces policies
   * @throws ExecutionException if the operation fails
   */
  default List<NamespacePolicy> getNamespacePolicies() throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Applies a policy to a table.
   *
   * @param policyName the policy name
   * @param namespaceName the namespace name
   * @param tableName the table name
   * @throws ExecutionException if the operation fails
   */
  default void applyTablePolicy(String policyName, String namespaceName, String tableName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.ABAC_NOT_ENABLED.buildMessage());
  }

  /**
   * Enables a table policy.
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
   * Disables a table policy.
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
   * Retrieves the table policies.
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
     * @return the short name
     */
    String getShortName();

    /**
     * Returns the long name of the level.
     *
     * @return the long name
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
     * @return the short name
     */
    String getShortName();

    /**
     * Returns the long name of the compartment.
     *
     * @return the long name
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
     * @return the short name
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
       * Returns the level short name.
       *
       * @return the level short name
       */
      String getLevelShortName();

      /**
       * Returns the default level short name.
       *
       * @return the default level short name
       */
      String getDefaultLevelShortName();

      /**
       * Returns the row level short name.
       *
       * @return the row level short name
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
     * @return the state
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
     * @return the state
     */
    PolicyState getState();
  }
}
