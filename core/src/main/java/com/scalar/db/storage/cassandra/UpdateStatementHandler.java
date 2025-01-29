package com.scalar.db.storage.cassandra;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A handler class for update statement
 *
 * @author Hiroyuki Yamada
 */
@ThreadSafe
public class UpdateStatementHandler extends MutateStatementHandler {

  /**
   * Constructs an {@code UpdateStatementHandler} with the specified {@code Session}
   *
   * @param session session to be used with this statement
   */
  public UpdateStatementHandler(Session session) {
    super(session);
  }

  @Override
  @Nonnull
  protected PreparedStatement prepare(Operation operation) {
    checkArgument(operation, Put.class);

    Put put = (Put) operation;
    Update update = prepare(put);
    String query = update.getQueryString();

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

  private Update prepare(Put put) {
    Update update =
        QueryBuilder.update(
            quoteIfNecessary(put.forNamespace().get()), quoteIfNecessary(put.forTable().get()));

    Update.Assignments assignments = update.with();
    put.getColumns().keySet().forEach(n -> assignments.and(set(quoteIfNecessary(n), bindMarker())));
    Update.Where where = update.where();
    put.getPartitionKey()
        .getColumns()
        .forEach(v -> where.and(QueryBuilder.eq(quoteIfNecessary(v.getName()), bindMarker())));
    put.getClusteringKey()
        .ifPresent(
            k ->
                k.getColumns()
                    .forEach(
                        v ->
                            where.and(
                                QueryBuilder.eq(quoteIfNecessary(v.getName()), bindMarker()))));

    setCondition(where, put);

    return update;
  }

  private BoundStatement bind(BoundStatement bound, Put put) {
    ValueBinder binder = new ValueBinder(bound);

    // bind from the front in the statement
    put.getColumns().values().forEach(c -> c.accept(binder));
    put.getPartitionKey().getColumns().forEach(c -> c.accept(binder));
    put.getClusteringKey().ifPresent(k -> k.getColumns().forEach(c -> c.accept(binder)));

    bindCondition(binder, put);

    return bound;
  }
}
