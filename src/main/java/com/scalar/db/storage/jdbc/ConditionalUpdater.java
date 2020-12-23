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
import com.scalar.db.storage.jdbc.query.DeleteQuery;
import com.scalar.db.storage.jdbc.query.InsertQuery;
import com.scalar.db.storage.jdbc.query.Query;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.UpdateQuery;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A visitor class to execute conditional put
 *
 * @author Toshihiro Suzuki
 */
@NotThreadSafe
public class ConditionalUpdater implements MutationConditionVisitor {

  private final Mutation mutation;
  private final Connection connection;
  private final QueryBuilder queryBuilder;

  private boolean updated;
  private SQLException sqlException;

  public ConditionalUpdater(Mutation mutation, Connection connection, QueryBuilder queryBuilder) {
    assert mutation.getCondition().isPresent();
    this.mutation = mutation;
    this.connection = connection;
    this.queryBuilder = queryBuilder;
  }

  public boolean update() throws SQLException {
    mutation.getCondition().ifPresent(condition -> condition.accept(this));
    throwSQLExceptionIfOccurred();
    return updated;
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
            .update(put.forFullTableName().get())
            .set(put.getValues())
            .where(put.getPartitionKey(), put.getClusteringKey(), condition.getExpressions())
            .build();
    executeUpdate(updateQuery);
  }

  @Override
  public void visit(PutIfExists condition) {
    Put put = (Put) mutation;
    UpdateQuery updateQuery =
        queryBuilder
            .update(put.forFullTableName().get())
            .set(put.getValues())
            .where(put.getPartitionKey(), put.getClusteringKey())
            .build();
    executeUpdate(updateQuery);
  }

  @Override
  public void visit(PutIfNotExists condition) {
    Put put = (Put) mutation;
    InsertQuery insertQuery =
        queryBuilder
            .insertInto(put.forFullTableName().get())
            .values(put.getPartitionKey(), put.getClusteringKey(), put.getValues())
            .build();
    try (PreparedStatement preparedStatement = insertQuery.prepareAndBind(connection)) {
      preparedStatement.executeUpdate();
      updated = true;
    } catch (SQLException e) {
      if (e.getSQLState().equals("23000") || e.getSQLState().equals("23505")) {
        // The duplicate key error
        // "23000" is for MySQL/Oracle/SQL Server and "23505" is for PostgreSQL
      } else {
        sqlException = e;
      }
    }
  }

  @Override
  public void visit(DeleteIf condition) {
    Delete delete = (Delete) mutation;
    DeleteQuery deleteQuery =
        queryBuilder
            .deleteFrom(delete.forFullTableName().get())
            .where(delete.getPartitionKey(), delete.getClusteringKey(), condition.getExpressions())
            .build();
    executeUpdate(deleteQuery);
  }

  @Override
  public void visit(DeleteIfExists condition) {
    Delete delete = (Delete) mutation;
    DeleteQuery deleteQuery =
        queryBuilder
            .deleteFrom(delete.forFullTableName().get())
            .where(delete.getPartitionKey(), delete.getClusteringKey())
            .build();
    executeUpdate(deleteQuery);
  }

  private void executeUpdate(Query query) {
    try (PreparedStatement preparedStatement = query.prepareAndBind(connection)) {
      int res = preparedStatement.executeUpdate();
      if (res > 0) {
        updated = true;
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }
}
