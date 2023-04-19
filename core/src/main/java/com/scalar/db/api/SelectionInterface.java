package com.scalar.db.api;

import java.util.List;

/** An interface for selection operations. */
public interface SelectionInterface extends BaseOperationInterface {

  /**
   * Returns the table name for this operation
   *
   * @return a {@code List} of column names to be projected
   */
  List<String> getProjections();
}
