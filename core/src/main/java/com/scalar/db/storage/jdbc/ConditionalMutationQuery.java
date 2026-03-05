package com.scalar.db.storage.jdbc;

import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationConditionVisitor;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.UpdateIf;
import com.scalar.db.api.UpdateIfExists;
import com.scalar.db.storage.jdbc.query.Query;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A query for a conditional mutation.
 *
 * @author Toshihiro Suzuki
 */
@SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
@NotThreadSafe
public class ConditionalMutationQuery implements MutationConditionVisitor, Query {

  private final Mutation mutation;
  private final TableMetadata tableMetadata;
  private final QueryBuilder queryBuilder;
  private final RdbEngineStrategy rdbEngine;

  @LazyInit private Query query;
  private boolean putIfNotExists;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ConditionalMutationQuery(
      Mutation mutation,
      TableMetadata tableMetadata,
      RdbEngineStrategy rdbEngine,
      QueryBuilder queryBuilder) {
    assert mutation instanceof Put || mutation instanceof Delete;
    assert mutation.getCondition().isPresent();
    this.mutation = mutation;
    this.tableMetadata = tableMetadata;
    this.rdbEngine = rdbEngine;
    this.queryBuilder = queryBuilder;

    mutation.getCondition().get().accept(this);
  }

  @Override
  public void visit(PutIf condition) {
    Put put = (Put) mutation;
    query =
        queryBuilder
            .update(put.forNamespace().get(), put.forTable().get(), tableMetadata)
            .set(put.getColumns())
            .where(put.getPartitionKey(), put.getClusteringKey(), condition.getExpressions())
            .build();
  }

  @Override
  public void visit(PutIfExists condition) {
    Put put = (Put) mutation;
    query =
        queryBuilder
            .update(put.forNamespace().get(), put.forTable().get(), tableMetadata)
            .set(put.getColumns())
            .where(put.getPartitionKey(), put.getClusteringKey())
            .build();
  }

  @Override
  public void visit(PutIfNotExists condition) {
    Put put = (Put) mutation;
    query =
        queryBuilder
            .insertInto(put.forNamespace().get(), put.forTable().get(), tableMetadata)
            .values(put.getPartitionKey(), put.getClusteringKey(), put.getColumns())
            .build();
    putIfNotExists = true;
  }

  @Override
  public void visit(DeleteIf condition) {
    Delete delete = (Delete) mutation;
    query =
        queryBuilder
            .deleteFrom(delete.forNamespace().get(), delete.forTable().get(), tableMetadata)
            .where(delete.getPartitionKey(), delete.getClusteringKey(), condition.getExpressions())
            .build();
  }

  @Override
  public void visit(DeleteIfExists condition) {
    Delete delete = (Delete) mutation;
    query =
        queryBuilder
            .deleteFrom(delete.forNamespace().get(), delete.forTable().get(), tableMetadata)
            .where(delete.getPartitionKey(), delete.getClusteringKey())
            .build();
  }

  @Override
  public void visit(UpdateIf condition) {
    throw new AssertionError("UpdateIf is not supported");
  }

  @Override
  public void visit(UpdateIfExists condition) {
    throw new AssertionError("UpdateIfExists is not supported");
  }

  @Override
  public String sql() {
    return query.sql();
  }

  @Override
  public void bind(PreparedStatement preparedStatement) throws SQLException {
    query.bind(preparedStatement);
  }

  public boolean isBatchable() {
    // PutIfNotExists is not batchable because its duplicate key error should be handled
    return !putIfNotExists;
  }

  public boolean handleSQLException(SQLException e) throws SQLException {
    // Ignore the duplicate key error for PutIfNotExists
    if (putIfNotExists && rdbEngine.isDuplicateKeyError(e)) {
      // Indicate that the mutation was not applied
      return false;
    }

    throw e;
  }
}
