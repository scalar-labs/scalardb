package com.scalar.db.storage.jdbc.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface Query {
  String sql();

  void bind(PreparedStatement preparedStatement) throws SQLException;
}
