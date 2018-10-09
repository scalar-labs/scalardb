package com.scalar.database.api;

import java.util.List;
import java.util.Optional;

/**
 * A scanner abstraction for iterating results.
 *
 * @author Hiroyuki Yamada
 */
public interface Scanner extends Iterable<Result> {

  /**
   * Returns the first result in the results.
   *
   * @return the first result in the results
   */
  Optional<Result> one();

  /**
   * Returns all the results.
   *
   * @return the list of {@code Result}s
   */
  List<Result> all();
}
