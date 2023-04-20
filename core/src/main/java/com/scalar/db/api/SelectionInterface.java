package com.scalar.db.api;

import java.util.List;

/** An interface for selection operations. */
public interface SelectionInterface extends BaseOperationInterface {

  /**
   * Returns the list of column names to be projected for this operation
   *
   * @return the {@code List} of column names to be projected
   */
  List<String> getProjections();
}
