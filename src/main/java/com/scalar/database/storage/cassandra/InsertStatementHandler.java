package com.scalar.database.storage.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.OngoingValues;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.scalar.database.api.Operation;
import com.scalar.database.api.Put;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A handler class for insert statements
 *
 * @author Hiroyuki Yamada, Yuji Ito
 */
@ThreadSafe
public class InsertStatementHandler extends MutateStatementHandler {

  /**
   * Constructs an {@code InsertStatementHandler} with the specified {@code CqlSession}
   *
   * @param session session to be used with this statement
   */
  public InsertStatementHandler(CqlSession session) {
    super(session);
  }

  @Override
  @Nonnull
  protected PreparedStatement prepare(Operation operation) {
    checkArgument(operation, Put.class);

    Put put = (Put) operation;
    Insert insert = prepare(put);

    return prepare(insert.asCql());
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

  private Insert prepare(Put put) {
    OngoingValues insert = insertInto(put.forNamespace().get(), put.forTable().get());
    Map<String, Term> values = new LinkedHashMap<>();

    put.getPartitionKey().forEach(v -> values.put(v.getName(), bindMarker()));
    put.getClusteringKey()
        .ifPresent(
            k -> {
              k.forEach(v -> values.put(v.getName(), bindMarker()));
            });
    put.getValues().forEach((k, v) -> values.put(v.getName(), bindMarker()));

    return (Insert) setCondition(insert.values(values), put);
  }

  private void bind(BoundStatementBuilder builder, Put put) {
    ValueBinder binder = new ValueBinder(builder);

    // bind in the prepared order
    put.getPartitionKey().forEach(v -> v.accept(binder));
    put.getClusteringKey().ifPresent(k -> k.forEach(v -> v.accept(binder)));
    put.getValues().forEach((k, v) -> v.accept(binder));

    // it calls for consistency, but actually nothing to bind here
    bindCondition(binder, put);
  }
}
