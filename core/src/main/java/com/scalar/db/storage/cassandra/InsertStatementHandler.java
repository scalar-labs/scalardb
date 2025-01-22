package com.scalar.db.storage.cassandra;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A handler class for insert statements
 *
 * @author Hiroyuki Yamada
 */
@ThreadSafe
public class InsertStatementHandler extends MutateStatementHandler {

  /**
   * Constructs an {@code InsertStatementHandler} with the specified {@code Session}
   *
   * @param session session to be used with this statement
   */
  public InsertStatementHandler(Session session) {
    super(session);
  }

  @Override
  @Nonnull
  protected PreparedStatement prepare(Operation operation) {
    checkArgument(operation, Put.class);

    Put put = (Put) operation;
    Insert insert = prepare(put);
    String query = insert.getQueryString();

    return prepare(query);
  }

  @Override
  @Nonnull
  protected BoundStatement bind(PreparedStatement prepared, Operation operation) {
    checkArgument(operation, Put.class);

    BoundStatement bound = prepared.bind();
    return bind(bound, (Put) operation);
  }

  @Override
  @Nonnull
  protected ResultSet execute(BoundStatement bound, Operation operation) {
    return session.execute(bound);
  }

  private Insert prepare(Put put) {
    Insert insert =
        insertInto(
            quoteIfNecessary(put.forNamespace().get()), quoteIfNecessary(put.forTable().get()));

    put.getPartitionKey()
        .getColumns()
        .forEach(v -> insert.value(quoteIfNecessary(v.getName()), bindMarker()));
    put.getClusteringKey()
        .ifPresent(
            k ->
                k.getColumns()
                    .forEach(v -> insert.value(quoteIfNecessary(v.getName()), bindMarker())));
    put.getColumns().keySet().forEach(n -> insert.value(quoteIfNecessary(n), bindMarker()));

    setCondition(insert, put);

    return insert;
  }

  private BoundStatement bind(BoundStatement bound, Put put) {
    ValueBinder binder = new ValueBinder(bound);

    // bind in the prepared order
    put.getPartitionKey().getColumns().forEach(c -> c.accept(binder));
    put.getClusteringKey().ifPresent(k -> k.getColumns().forEach(c -> c.accept(binder)));
    put.getColumns().values().forEach(c -> c.accept(binder));

    // it calls for consistency, but actually nothing to bind here
    bindCondition(binder, put);

    return bound;
  }
}
