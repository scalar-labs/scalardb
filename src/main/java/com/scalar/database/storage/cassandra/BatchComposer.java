package com.scalar.database.storage.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.database.api.Delete;
import com.scalar.database.api.Get;
import com.scalar.database.api.Operation;
import com.scalar.database.api.OperationVisitor;
import com.scalar.database.api.Put;
import com.scalar.database.api.Scan;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A batch statement composer
 *
 * @author Hiroyuki Yamada, Yuji Ito
 */
@NotThreadSafe
public class BatchComposer implements OperationVisitor {
  private final BatchStatementBuilder builder;
  private final StatementHandlerManager handlers;

  /**
   * Constructs a {@code BatchComposer} with the specified {@link BatchStatementBuilder} and {@link
   * StatementHandlerManager}
   *
   * @param builder {@code BatchStatementBuilder} for multiple statements
   * @param handlers {@code StatementHandlerManager}
   */
  public BatchComposer(BatchStatementBuilder builder, StatementHandlerManager handlers) {
    this.builder = checkNotNull(builder);
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
   * Adds a insert/update statement to the batch
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
    BoundStatementBuilder boundBuilder = handler.bind(prepared, operation);
    builder.addStatement(boundBuilder.build());
  }
}
