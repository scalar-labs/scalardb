package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.MutationConditionVisitor;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.storage.jdbc.query.InsertQuery;
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
public class ConditionalPutExecutor implements MutationConditionVisitor {

  private final Put put;
  private final Connection connection;
  private final QueryBuilder queryBuilder;

  private boolean result;
  private SQLException sqlException;

  public ConditionalPutExecutor(Put put, Connection connection, QueryBuilder queryBuilder) {
    assert put.getCondition().isPresent();
    this.put = put;
    this.connection = connection;
    this.queryBuilder = queryBuilder;
  }

  public boolean update() throws SQLException {
    put.getCondition().ifPresent(condition -> condition.accept(this));
    throwSQLExceptionIfOccurred();
    return result;
  }

  private void throwSQLExceptionIfOccurred() throws SQLException {
    if (sqlException != null) {
      throw sqlException;
    }
  }

  @Override
  public void visit(PutIf condition) {
    UpdateQuery updateQuery =
        queryBuilder
            .update(put.forFullTableName().get())
            .set(put.getValues())
            .where(put.getPartitionKey(), put.getClusteringKey(), condition.getExpressions())
            .build();
    try (PreparedStatement preparedStatement = updateQuery.prepareAndBind(connection)) {
      int res = preparedStatement.executeUpdate();
      if (res > 0) {
        result = true;
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(PutIfExists condition) {
    UpdateQuery updateQuery =
        queryBuilder
            .update(put.forFullTableName().get())
            .set(put.getValues())
            .where(put.getPartitionKey(), put.getClusteringKey())
            .build();
    try (PreparedStatement preparedStatement = updateQuery.prepareAndBind(connection)) {
      int res = preparedStatement.executeUpdate();
      if (res > 0) {
        result = true;
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(PutIfNotExists condition) {
    InsertQuery insertQuery =
        queryBuilder
            .insertInto(put.forFullTableName().get())
            .values(put.getPartitionKey(), put.getClusteringKey(), put.getValues())
            .build();
    try (PreparedStatement preparedStatement = insertQuery.prepareAndBind(connection)) {
      preparedStatement.executeUpdate();
      result = true;
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
    assert false;
  }

  @Override
  public void visit(DeleteIfExists condition) {
    assert false;
  }
}
