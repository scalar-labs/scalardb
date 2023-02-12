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
    statement.close();
    connection.close();

    boolean r = dbFile.delete();
    assertTrue(r);
  }

  private SQLException causeSyntaxError() {
    return (SQLException) catchThrowable(() -> statement.executeUpdate("invalid sql"));
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
  void isDuplicateKeyError() throws SQLException {
    statement.executeUpdate("create table t (c1 integer, c2 integer)");
    statement.executeUpdate("create index i on t (c1)");

    // true case
    SQLException e =
        (SQLException) catchThrowable(() -> statement.executeUpdate("create index i on t (c2)"));
    assertTrue(rdbEngine.isDuplicateKeyError(e));

    // false case
    SQLException e3 =
        (SQLException) catchThrowable(() -> statement.executeUpdate("drop index i404"));
    assertFalse(rdbEngine.isDuplicateTableError(e3));
  }

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
  void isConflictError_True_SQLITE_BUSY_WhenUpdatingBeingDeletedTableInSeparateConnection()
      throws SQLException {
    statement.executeUpdate("create table t (c integer)");
    statement.executeUpdate("insert into t values (1)");

    Connection connection2 = DriverManager.getConnection("jdbc:sqlite:" + dbFile.getAbsolutePath());
    Statement statement2 = connection2.createStatement();

    statement.executeUpdate("begin");
    statement2.executeUpdate("begin");

    statement.executeUpdate("delete from t where c = 1");

    SQLException e =
        (SQLException)
            catchThrowable(() -> statement2.executeUpdate("update t set c = c + 1 where c = 1"));
    assertTrue(rdbEngine.isConflictError(e));
  }

  @Test
  void isConflictError_False() {
    assertFalse(rdbEngine.isConflictError(causeSyntaxError()));
  }

  @Test
  void isCreateMetadataSchemaDuplicateSchemaError() {}
}
