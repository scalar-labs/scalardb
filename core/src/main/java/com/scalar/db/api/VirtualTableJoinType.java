package com.scalar.db.api;

/**
 * The type of join to perform between two source tables of a virtual table.
 *
 * <p>This enum defines the types of joins that can be performed when creating a virtual table that
 * combines data from two source tables.
 */
public enum VirtualTableJoinType {
  /**
   * An inner join returns only the rows where there is a match in both source tables based on their
   * primary key.
   */
  INNER,

  /**
   * A left outer join returns all rows from the left source table and the matched rows from the
   * right source table. If there is no match for a left row, the right-side columns appear as
   * {@code NULL}.
   */
  LEFT_OUTER
}
