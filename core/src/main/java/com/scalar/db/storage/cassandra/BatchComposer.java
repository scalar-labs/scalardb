package com.scalar.db.storage.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.OperationVisitor;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A batch statement composer
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public class BatchComposer implements OperationVisitor {
  private final BatchStatement batch;
  private final StatementHandlerManager handlers;

  /**
   * Constructs a {@code BatchComposer} with the specified {@link BatchStatement} and {@link
   * StatementHandlerManager}
   *
   * @param batch {@code BatchStatement} for multiple statements
   * @param handlers {@code StatementHandlerManager}
   */
  public BatchComposer(BatchStatement batch, StatementHandlerManager handlers) {
    this.batch = checkNotNull(batch);
    this.handlers = checkNotNull(handlers);
  }

  /**
   * Adds a select statement to the batch
   *
   * @param get {@code Get} command to add
   */
  @Override
  public void visit(Get get) {
    composeWith(handlers.select(), get);
  }

  /**
   * Adds a select statement to the batch
   *
   * @param scan {@code Scan} command to add
   */
  @Override
  public void visit(Scan scan) {
    composeWith(handlers.select(), scan);
  }

  /**
   * Adds an insert/update statement to the batch
   *
   * @param put {@code Put} command to add
   */
  @Override
  public void visit(Put put) {
    // detect insert() or update() internally
    composeWith(handlers.get(put), put);
  }

  /**
   * Adds a delete statement to the batch
   *
   * @param delete {@code Delete} command to add
   */
  @Override
  public void visit(Delete delete) {
    composeWith(handlers.delete(), delete);
  }

  @VisibleForTesting
  void composeWith(StatementHandler handler, Operation operation) {
    PreparedStatement prepared = handler.prepare(operation);
    BoundStatement bound = handler.bind(prepared, operation);
    batch.add(bound);
  }
}
