package com.scalar.database.storage.cassandra4driver;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.scalar.database.api.Operation;
import com.scalar.database.api.Put;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A handler class for update statement
 *
 * @author Hiroyuki Yamada, Yuji Ito
 */
@ThreadSafe
public class UpdateStatementHandler extends MutateStatementHandler {

  /**
   * Constructs an {@code UpdateStatementHandler} with the specified {@code CqlSession}
   *
   * @param session session to be used with this statement
   */
  public UpdateStatementHandler(CqlSession session) {
    super(session);
  }

  @Override
  @Nonnull
  protected PreparedStatement prepare(Operation operation) {
    checkArgument(operation, Put.class);

    Put put = (Put) operation;
    Update update = prepare(put);

    return prepare(update.asCql());
  }

  @Override
  @Nonnull
  protected BoundStatementBuilder bind(PreparedStatement prepared, Operation operation) {
    checkArgument(operation, Put.class);

    BoundStatementBuilder builder = prepared.boundStatementBuilder();
    bind(builder, (Put) operation);

    return builder;
  }

  @Override
  @Nonnull
  protected ResultSet execute(BoundStatement bound, Operation operation) {
    return session.execute(bound);
  }

  private Update prepare(Put put) {
    UpdateStart update = update(put.forNamespace().get(), put.forTable().get());

    List<Assignment> assignments = new ArrayList<>();
    put.getValues().forEach((k, v) -> assignments.add(Assignment.setColumn(k, bindMarker())));

    List<Relation> relations = new ArrayList<>();
    put.getPartitionKey()
        .forEach(v -> relations.add(Relation.column(v.getName()).isEqualTo(bindMarker())));

    put.getClusteringKey()
        .ifPresent(
            k -> {
              k.forEach(v -> relations.add(Relation.column(v.getName()).isEqualTo(bindMarker())));
            });

    return (Update) setCondition(update.set(assignments).where(relations), put);
  }

  private void bind(BoundStatementBuilder builder, Put put) {
    ValueBinder binder = new ValueBinder(builder);

    // bind from the front in the statement
    put.getValues().forEach((k, v) -> v.accept(binder));
    put.getPartitionKey().forEach(v -> v.accept(binder));
    put.getClusteringKey().ifPresent(k -> k.forEach(v -> v.accept(binder)));

    bindCondition(binder, put);
  }
}
