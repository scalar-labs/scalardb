package com.scalar.db.api;

import com.scalar.db.exception.storage.ExecutionException;
import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/**
 * A scanner abstraction for iterating results.
 *
 * @author Hiroyuki Yamada
 */
public interface Scanner extends Closeable, Iterable<Result> {

  /**
   * Returns the first result in the results.
   *
   * @return the first result in the results
   * @throws ExecutionException if the operation fails
   */
  Optional<Result> one() throws ExecutionException;

  /**
   * Returns all the results.
   *
   * @return the list of {@code Result}s
   * @throws ExecutionException if the operation fails
   */
  List<Result> all() throws ExecutionException;
}
