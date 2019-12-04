package com.scalar.database.storage.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.scalar.database.api.Operation;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A handler class for delete statements
 *
 * @author Hiroyuki Yamada, Yuji Ito
 */
@ThreadSafe
public class DeleteStatementHandler extends MutateStatementHandler {

  /**
   * Constructs a {@code DeleteStatementHandler} with the specified {@code CqlSession}
   *
   * @param session session to be used with this statement
   */
  public DeleteStatementHandler(CqlSession session) {
    super(session);
  }

  @Override
  @Nonnull
  protected PreparedStatement prepare(Operation operation) {
    checkArgument(operation, com.scalar.database.api.Delete.class);

    com.scalar.database.api.Delete del = (com.scalar.database.api.Delete) operation;
    Delete delete = prepare(del);

    return prepare(delete.asCql());
  }

  @Override
  @Nonnull
  protected BoundStatementBuilder bind(PreparedStatement prepared, Operation operation) {
    checkArgument(operation, com.scalar.database.api.Delete.class);

    BoundStatementBuilder builder = prepared.boundStatementBuilder();
    bind(builder, (com.scalar.database.api.Delete) operation);

    return builder;
  }

  @Override
  @Nonnull
  protected ResultSet execute(BoundStatement bound, Operation operation) {
    return session.execute(bound);
  }

  private Delete prepare(com.scalar.database.api.Delete del) {
    DeleteSelection delete = deleteFrom(del.forNamespace().get(), del.forTable().get());

    List<Relation> relations = new ArrayList<Relation>();
    del.getPartitionKey()
        .forEach(v -> relations.add(Relation.column(v.getName()).isEqualTo(bindMarker())));
    del.getClusteringKey()
        .ifPresent(
            k -> {
              k.forEach(v -> relations.add(Relation.column(v.getName()).isEqualTo(bindMarker())));
            });

    return (Delete) setCondition(delete.where(relations), del);
  }

  private void bind(BoundStatementBuilder builder, com.scalar.database.api.Delete del) {
    ValueBinder binder = new ValueBinder(builder);

    // bind in the prepared order
    del.getPartitionKey().forEach(v -> v.accept(binder));
    del.getClusteringKey().ifPresent(k -> k.forEach(v -> v.accept(binder)));

    bindCondition(binder, del);
  }
}
