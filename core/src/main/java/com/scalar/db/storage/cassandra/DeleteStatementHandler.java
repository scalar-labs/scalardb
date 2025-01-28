package com.scalar.db.storage.cassandra;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Operation;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A handler class for delete statements
 *
 * @author Hiroyuki Yamada
 */
@ThreadSafe
public class DeleteStatementHandler extends MutateStatementHandler {

  /**
   * Constructs a {@code DeleteStatementHandler} with the specified {@code Session}
   *
   * @param session session to be used with this statement
   */
  public DeleteStatementHandler(Session session) {
    super(session);
  }

  @Override
  @Nonnull
  protected PreparedStatement prepare(Operation operation) {
    checkArgument(operation, Delete.class);

    Delete del = (Delete) operation;
    com.datastax.driver.core.querybuilder.Delete delete = prepare(del);

    return prepare(delete.getQueryString());
  }

  @Override
  @Nonnull
  protected BoundStatement bind(PreparedStatement prepared, Operation operation) {
    checkArgument(operation, Delete.class);
    BoundStatement bound = prepared.bind();
    return bind(bound, (Delete) operation);
  }

  @Override
  @Nonnull
  protected ResultSet execute(BoundStatement bound, Operation operation) {
    return session.execute(bound);
  }

  private com.datastax.driver.core.querybuilder.Delete prepare(Delete del) {
    com.datastax.driver.core.querybuilder.Delete delete =
        QueryBuilder.delete()
            .from(
                quoteIfNecessary(del.forNamespace().get()), quoteIfNecessary(del.forTable().get()));
    com.datastax.driver.core.querybuilder.Delete.Where where = delete.where();

    del.getPartitionKey()
        .getColumns()
        .forEach(v -> where.and(QueryBuilder.eq(quoteIfNecessary(v.getName()), bindMarker())));
    del.getClusteringKey()
        .ifPresent(
            k ->
                k.getColumns()
                    .forEach(
                        v ->
                            where.and(
                                QueryBuilder.eq(quoteIfNecessary(v.getName()), bindMarker()))));

    setCondition(where, del);

    return delete;
  }

  private BoundStatement bind(BoundStatement bound, Delete del) {
    ValueBinder binder = new ValueBinder(bound);

    // bind in the prepared order
    del.getPartitionKey().getColumns().forEach(c -> c.accept(binder));
    del.getClusteringKey().ifPresent(k -> k.getColumns().forEach(c -> c.accept(binder)));

    bindCondition(binder, del);

    return bound;
  }
}
