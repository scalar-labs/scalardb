package com.scalar.db.storage.hbase.query;

import com.scalar.db.io.Key;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class AbstractQuery implements Query {

  protected static final String HASH_COLUMN_NAME = "hash";

  @Override
  public PreparedStatement prepareAndBind(Connection connection) throws SQLException {
    PreparedStatement preparedStatement = prepare(connection);
    bind(preparedStatement);
    return preparedStatement;
  }

  protected abstract String sql();

  protected PreparedStatement prepare(Connection connection) throws SQLException {
    return connection.prepareStatement(sql());
  }

  protected abstract void bind(PreparedStatement preparedStatement) throws SQLException;

  @Override
  public String toString() {
    return sql();
  }

  protected static int hash(Key partitionKey) {
    return new HashCalculator(partitionKey).calculate();
  }
}
