package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
  void throwIfInvalidNamespaceName_ShouldNotThrowAnyException() {
    assertThatCode(() -> rdbEngine.throwIfInvalidNamespaceName("a-b")).doesNotThrowAnyException();
  }

  @Test
  void
      throwIfInvalidNamespaceName_WhenContainsNamespaceSeparator_ShouldThrowIllegalArgumentException() {
    assertThatThrownBy(() -> rdbEngine.throwIfInvalidNamespaceName("a$b"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void throwIfInvalidTableName_ShouldNotThrowAnyException() {
    assertThatCode(() -> rdbEngine.throwIfInvalidTableName("a-b")).doesNotThrowAnyException();
  }

  @Test
  void
      throwIfInvalidTableName_WhenContainsNamespaceSeparator_ShouldThrowIllegalArgumentException() {
    assertThatThrownBy(() -> rdbEngine.throwIfInvalidTableName("a$b"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void getBinaryTextCollationClause_ShouldReturnCollateBinary() {
    // Arrange
    RdbEngineStrategy strategy = rdbEngine;

    // Act
    String clause = strategy.getBinaryTextCollationClause();

    // Assert
    assertThat(clause).isEqualTo("COLLATE BINARY");
  }

  @Test
  void getDatabaseEncodingIssue_DefaultDatabase_ShouldReturnNull() throws SQLException {
    // Arrange Act
    DatabaseEncodingIssue encoding = rdbEngine.getDatabaseEncodingIssue(connection);

    // Assert
    assertThat(encoding).isNull();
  }

  @Test
  void getDatabaseEncodingIssue_Utf16Database_ShouldReturnUtf16CodeUnitOrder()
      throws SQLException, IOException {
    // Arrange
    File utf16File = File.createTempFile(getClass().getName(), ".db");
    try (Connection utf16Connection =
        DriverManager.getConnection("jdbc:sqlite:" + utf16File.getAbsolutePath())) {
      try (Statement stmt = utf16Connection.createStatement()) {
        stmt.execute("PRAGMA encoding = 'UTF-16le'");
        stmt.execute("CREATE TABLE t (c TEXT)");
      }

      // Act
      DatabaseEncodingIssue encoding = rdbEngine.getDatabaseEncodingIssue(utf16Connection);

      // Assert
      assertThat(encoding)
          .isEqualTo(
              new DatabaseEncodingIssue(
                  "UTF-16le", DatabaseEncodingIssue.Kind.UTF16_CODE_UNIT_ORDER));
    } finally {
      assertTrue(utf16File.delete());
    }
  }
}
