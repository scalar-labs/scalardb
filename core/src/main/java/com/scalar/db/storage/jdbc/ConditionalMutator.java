package com.scalar.db.storage.jdbc;

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
import com.scalar.db.storage.jdbc.query.DeleteQuery;
import com.scalar.db.storage.jdbc.query.InsertQuery;
import com.scalar.db.storage.jdbc.query.Query;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.UpdateQuery;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A visitor class to execute a conditional mutation.
 *
 * @author Toshihiro Suzuki
 */
@SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
@NotThreadSafe
public class ConditionalMutator implements MutationConditionVisitor {

  private final Mutation mutation;
  private final TableMetadata tableMetadata;
  private final Connection connection;
  private final QueryBuilder queryBuilder;
  private final RdbEngineStrategy rdbEngine;

  private boolean isMutated;
  private SQLException sqlException;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ConditionalMutator(
      Mutation mutation,
      TableMetadata tableMetadata,
      Connection connection,
      RdbEngineStrategy rdbEngine,
      QueryBuilder queryBuilder) {
    assert mutation.getCondition().isPresent();
    this.mutation = mutation;
    this.tableMetadata = tableMetadata;
    this.connection = connection;
    this.rdbEngine = rdbEngine;
    this.queryBuilder = queryBuilder;
  }

  public boolean mutate() throws SQLException {
    mutation.getCondition().ifPresent(condition -> condition.accept(this));
    throwSQLExceptionIfOccurred();
    return isMutated;
  }

  private void throwSQLExceptionIfOccurred() throws SQLException {
    if (sqlException != null) {
      throw sqlException;
    }
  }

  @Override
  public void visit(PutIf condition) {
    Put put = (Put) mutation;
    UpdateQuery updateQuery =
        queryBuilder
            .update(put.forNamespace().get(), put.forTable().get(), tableMetadata)
            .set(put.getColumns())
            .where(put.getPartitionKey(), put.getClusteringKey(), condition.getExpressions())
            .build();
    executeMutate(updateQuery);
  }

  @Override
  public void visit(PutIfExists condition) {
    Put put = (Put) mutation;
    UpdateQuery updateQuery =
        queryBuilder
            .update(put.forNamespace().get(), put.forTable().get(), tableMetadata)
            .set(put.getColumns())
            .where(put.getPartitionKey(), put.getClusteringKey())
            .build();
    executeMutate(updateQuery);
  }

  @Override
  public void visit(PutIfNotExists condition) {
    Put put = (Put) mutation;
    InsertQuery insertQuery =
        queryBuilder
            .insertInto(put.forNamespace().get(), put.forTable().get(), tableMetadata)
            .values(put.getPartitionKey(), put.getClusteringKey(), put.getColumns())
            .build();
    try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery.sql())) {
      insertQuery.bind(preparedStatement);
      preparedStatement.executeUpdate();
      isMutated = true;
    } catch (SQLException e) {
      // ignore the duplicate key error
      if (!rdbEngine.isDuplicateKeyError(e)) {
        sqlException = e;
      }
    }
  }

  @Override
  public void visit(DeleteIf condition) {
    Delete delete = (Delete) mutation;
    DeleteQuery deleteQuery =
        queryBuilder
            .deleteFrom(delete.forNamespace().get(), delete.forTable().get(), tableMetadata)
            .where(delete.getPartitionKey(), delete.getClusteringKey(), condition.getExpressions())
            .build();
    executeMutate(deleteQuery);
  }

  @Override
  public void visit(DeleteIfExists condition) {
    Delete delete = (Delete) mutation;
    DeleteQuery deleteQuery =
        queryBuilder
            .deleteFrom(delete.forNamespace().get(), delete.forTable().get(), tableMetadata)
            .where(delete.getPartitionKey(), delete.getClusteringKey())
            .build();
    executeMutate(deleteQuery);
  }

  private void executeMutate(Query query) {
    try (PreparedStatement preparedStatement = connection.prepareStatement(query.sql())) {
      query.bind(preparedStatement);
      int res = preparedStatement.executeUpdate();
      if (res > 0) {
        isMutated = true;
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(UpdateIf condition) {
    throw new AssertionError("UpdateIf is not supported");
  }

  @Override
  public void visit(UpdateIfExists condition) {
    throw new AssertionError("UpdateIfExists is not supported");
  }
}
