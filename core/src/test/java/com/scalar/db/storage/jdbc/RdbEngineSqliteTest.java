package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  void isDuplicateTableError_True() throws SQLException {
    statement.executeUpdate("create table t (c integer)");

    SQLException e =
        (SQLException) catchThrowable(() -> statement.executeUpdate("create table t (c integer)"));
    assertTrue(rdbEngine.isDuplicateTableError(e));
  }

  @Test
  void isDuplicateTableError_False() {
    assertFalse(rdbEngine.isDuplicateTableError(causeSyntaxError()));
  }

  @Test
  void isDuplicateKeyError_True_OnPrimaryKeyConstraintViolation() throws SQLException {
    statement.executeUpdate("create table t (c1 integer, c2 integer, primary key (c1))");
    statement.executeUpdate("insert into t values (1, 2)");

    SQLException e =
        (SQLException) catchThrowable(() -> statement.executeUpdate("insert into t values (1, 3)"));
    assertTrue(rdbEngine.isDuplicateKeyError(e));
  }

  @Test
  void isDuplicateKeyError_True_OnUniqueConstraintViolation() throws SQLException {
    statement.executeUpdate("create table t (c1 integer, c2 integer, unique (c1))");
    statement.executeUpdate("insert into t values (1, 2)");

    SQLException e =
        (SQLException) catchThrowable(() -> statement.executeUpdate("insert into t values (1, 3)"));
    assertTrue(rdbEngine.isDuplicateKeyError(e));
  }

  @Test
  void isDuplicateKeyError_False() {
    assertFalse(rdbEngine.isDuplicateTableError(causeSyntaxError()));
  }

  @Test
  void isUndefinedTableError_True() {
    SQLException e =
        (SQLException) catchThrowable(() -> statement.executeUpdate("select 1 from t404"));
    assertTrue(rdbEngine.isUndefinedTableError(e));
  }

  @Test
  void isUndefinedTableError_False() {
    assertFalse(rdbEngine.isUndefinedTableError(causeSyntaxError()));
  }

  @Test
  void isConflict_True_SQLITE_BUSY_WhenUpdatingBeingDeletedTableInSeparateConnection()
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
    assertTrue(rdbEngine.isConflict(e));
  }

  @Test
  void isConflict_False() {
    assertFalse(rdbEngine.isConflict(causeSyntaxError()));
  }

  @Test
  void isValidTableName_True() {
    assertTrue(rdbEngine.isValidTableName("a_b"));
  }

  @Test
  void isValidTableName_False_WhenContainsNamespaceSeparator() {
    assertFalse(rdbEngine.isValidTableName("a$b"));
  }
}
