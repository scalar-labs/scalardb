package com.scalar.db.storage.jdbc;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.MutationConditionVisitor;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.storage.jdbc.query.DeleteQuery;
import com.scalar.db.storage.jdbc.query.QueryBuilder;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A visitor class to execute conditional delete
 *
 * @author Toshihiro Suzuki
 */
@NotThreadSafe
public class ConditionalDeleteExecutor implements MutationConditionVisitor {

  private final Delete delete;
  private final Connection connection;
  private final QueryBuilder queryBuilder;

  private DeleteQuery deleteQuery;

  public ConditionalDeleteExecutor(
      Delete delete, Connection connection, QueryBuilder queryBuilder) {
    assert delete.getCondition().isPresent();
    this.delete = delete;
    this.connection = connection;
    this.queryBuilder = queryBuilder;
  }

  public boolean update() throws SQLException {
    delete.getCondition().ifPresent(condition -> condition.accept(this));
    try (PreparedStatement preparedStatement = deleteQuery.prepareAndBind(connection)) {
      int res = preparedStatement.executeUpdate();
      return res > 0;
    }
  }

  @Override
  public void visit(PutIf condition) {
    assert false;
  }

  @Override
  public void visit(PutIfExists condition) {
    assert false;
  }

  @Override
  public void visit(PutIfNotExists condition) {
    assert false;
  }

  @Override
  public void visit(DeleteIf condition) {
    deleteQuery =
        queryBuilder
            .deleteFrom(delete.forFullTableName().get())
            .where(delete.getPartitionKey(), delete.getClusteringKey(), condition.getExpressions())
            .build();
  }

  @Override
  public void visit(DeleteIfExists condition) {
    deleteQuery =
        queryBuilder
            .deleteFrom(delete.forFullTableName().get())
            .where(delete.getPartitionKey(), delete.getClusteringKey())
            .build();
  }
}
