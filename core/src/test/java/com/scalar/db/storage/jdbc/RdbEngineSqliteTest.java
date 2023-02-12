package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RdbEngineSqliteTest {

  private RdbEngineSqlite rdbEngine;

  private Connection connection;
  private Statement statement;
  private File dbFile;

  @BeforeEach
  void setUp() throws SQLException, IOException {
    dbFile = File.createTempFile(getClass().getName(), ".db");
    connection = DriverManager.getConnection("jdbc:sqlite:" + dbFile.getAbsolutePath());
    statement = connection.createStatement();

    rdbEngine = new RdbEngineSqlite();
  }

  @AfterEach
  void tearDown() throws SQLException {
    connection.close();

    boolean r = dbFile.delete();
    assertTrue(r);
  }

  @Test
  void isDuplicateTableError() throws SQLException {
    statement.executeUpdate("create table t (c integer)");

    // true case
    SQLException e =
        (SQLException) catchThrowable(() -> statement.executeUpdate("create table t (c integer)"));
    assertTrue(rdbEngine.isDuplicateTableError(e));

    // false case
    SQLException e2 =
        (SQLException) catchThrowable(() -> statement.executeUpdate("drop table t404"));
    assertFalse(rdbEngine.isDuplicateTableError(e2));
  }

  @Test
  void isDuplicateKeyError() {}

  @Test
  void isUndefinedTableError() throws SQLException {
    // true case
    SQLException e =
        (SQLException) catchThrowable(() -> statement.executeUpdate("select 1 from t404"));
    assertTrue(rdbEngine.isUndefinedTableError(e));

    // false case
    statement.executeUpdate("create table t (c integer)");
    SQLException e2 =
        (SQLException) catchThrowable(() -> statement.executeUpdate("select c404 from t"));
    assertFalse(rdbEngine.isUndefinedTableError(e2));
  }

  @Test
  void isConflictError() {}

  @Test
  void isCreateMetadataSchemaDuplicateSchemaError() {}
}
