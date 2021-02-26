package com.scalar.db.storage.jdbc.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface Query {
  PreparedStatement prepareAndBind(Connection connection) throws SQLException;
}
