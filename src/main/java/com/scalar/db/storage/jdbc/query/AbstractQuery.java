package com.scalar.db.storage.jdbc.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class AbstractQuery implements Query {

  @Override
  public PreparedStatement prepareAndBind(Connection connection) throws SQLException {
    PreparedStatement preparedStatement = prepare(connection);
    PreparedStatementBinder preparedStatementBinder =
        new PreparedStatementBinder(preparedStatement);
    bind(preparedStatementBinder);
    return preparedStatement;
  }

  protected abstract String sql();

  protected PreparedStatement prepare(Connection connection) throws SQLException {
    return connection.prepareStatement(sql());
  }

  protected abstract void bind(PreparedStatementBinder preparedStatementBinder) throws SQLException;

  @Override
  public String toString() {
    return sql();
  }
}
