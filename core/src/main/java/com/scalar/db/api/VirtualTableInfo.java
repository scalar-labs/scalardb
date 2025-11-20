package com.scalar.db.api;

/**
 * Represents information about a virtual table, which is a view created by joining two source
 * tables.
 */
public interface VirtualTableInfo {
  /**
   * Returns the namespace name of the virtual table.
   *
   * @return the namespace name of the virtual table
   */
  String getNamespaceName();

  /**
   * Returns the table name of the virtual table.
   *
   * @return the table name of the virtual table
   */
  String getTableName();

  /**
   * Returns the namespace name of the left source table.
   *
   * @return the namespace name of the left source table
   */
  String getLeftSourceNamespaceName();

  /**
   * Returns the table name of the left source table.
   *
   * @return the table name of the left source table
   */
  String getLeftSourceTableName();

  /**
   * Returns the namespace name of the right source table.
   *
   * @return the namespace name of the right source table
   */
  String getRightSourceNamespaceName();

  /**
   * Returns the table name of the right source table.
   *
   * @return the table name of the right source table
   */
  String getRightSourceTableName();

  /**
   * Returns the join type used to create this virtual table.
   *
   * @return the join type (INNER or LEFT_OUTER)
   */
  VirtualTableJoinType getJoinType();
}
