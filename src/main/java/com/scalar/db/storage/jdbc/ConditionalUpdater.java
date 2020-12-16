package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.MutationConditionVisitor;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.jdbc.query.DeleteQuery;
import com.scalar.db.storage.jdbc.query.InsertQuery;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.UpdateQuery;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * A visitor class to perform conditional updates
 *
 * @author Toshihiro Suzuki
 */
@NotThreadSafe
public class ConditionalUpdater implements MutationConditionVisitor {

  private final Connection connection;
  private final QueryBuilder queryBuilder;
  private final Table table;
  private final Key partitionKey;
  @Nullable private final Key clusteringKey;
  private final MutationCondition condition;
  @Nullable private final Map<String, Value> values;

  private boolean result;
  private SQLException sqlException;

  public ConditionalUpdater(
      Connection connection,
      QueryBuilder queryBuilder,
      Table table,
      Key partitionKey,
      @Nullable Key clusteringKey,
      MutationCondition condition,
      @Nullable Map<String, Value> values) {
    this.connection = connection;
    this.queryBuilder = queryBuilder;
    this.table = table;
    this.partitionKey = partitionKey;
    this.clusteringKey = clusteringKey;
    this.condition = condition;
    this.values = values;
  }

  public boolean update() throws SQLException {
    condition.accept(this);
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
            .update(table)
            .set(values)
            .where(partitionKey, clusteringKey, condition.getExpressions())
            .build();
    try {
      PreparedStatement preparedStatement = updateQuery.prepareAndBind(connection);
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
        queryBuilder.update(table).set(values).where(partitionKey, clusteringKey).build();
    try {
      PreparedStatement preparedStatement = updateQuery.prepareAndBind(connection);
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
        queryBuilder.insertInto(table).values(partitionKey, clusteringKey, values).build();
    try {
      PreparedStatement preparedStatement = insertQuery.prepareAndBind(connection);
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
    DeleteQuery deleteQuery =
        queryBuilder
            .deleteFrom(table)
            .where(partitionKey, clusteringKey, condition.getExpressions())
            .build();
    try {
      PreparedStatement preparedStatement = deleteQuery.prepareAndBind(connection);
      int res = preparedStatement.executeUpdate();
      if (res > 0) {
        result = true;
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }

  @Override
  public void visit(DeleteIfExists condition) {
    DeleteQuery deleteQuery =
        queryBuilder.deleteFrom(table).where(partitionKey, clusteringKey).build();
    try {
      PreparedStatement preparedStatement = deleteQuery.prepareAndBind(connection);
      int res = preparedStatement.executeUpdate();
      if (res > 0) {
        result = true;
      }
    } catch (SQLException e) {
      sqlException = e;
    }
  }
}
