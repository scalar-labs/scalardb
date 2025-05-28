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
   * Returns the next result.
   *
   * @return an {@code Optional} containing the next result if available, or empty if no more
   *     results
   * @throws ExecutionException if the operation fails
   */
  Optional<Result> one() throws ExecutionException;

  /**
   * Returns all remaining results.
   *
   * @return a {@code List} containing all remaining results
   * @throws ExecutionException if the operation fails
   */
  List<Result> all() throws ExecutionException;
}
