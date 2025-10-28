package com.scalar.db.storage.jdbc;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IsolationLevelCheck {
  private RdbEngineStrategy rdbEngine;
  private BasicDataSource dataSource;

  @BeforeAll
  public void beforeAll() {
    JdbcConfig config =
        new JdbcConfig(new DatabaseConfig(JdbcEnv.getProperties("isolation_level_check")));
    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
  }

  @AfterAll
  public void afterAll() throws SQLException {
    if (dataSource != null) {
      dataSource.close();
    }
  }

  @SuppressFBWarnings("REC_CATCH_EXCEPTION")
  @Test
  void test() throws Exception {
    try {
      try (Connection connection = dataSource.getConnection();
          Statement stmt = connection.createStatement()) {
        // Create a test schema
        if (rdbEngine instanceof RdbEngineOracle) {
          stmt.execute("CREATE USER ns IDENTIFIED BY \"Oracle1234!@#$\"");
          stmt.execute("ALTER USER ns quota unlimited on USERS");
        } else {
          stmt.execute("CREATE SCHEMA ns");
        }

        // Create test tables
        stmt.execute(
            "CREATE TABLE ns.tbl1 (id "
                + rdbEngine.getDataTypeForEngine(DataType.INT)
                + " NOT NULL PRIMARY KEY, value "
                + rdbEngine.getDataTypeForEngine(DataType.INT)
                + ")");
        stmt.execute(
            "CREATE TABLE ns.tbl2 (id "
                + rdbEngine.getDataTypeForEngine(DataType.INT)
                + " NOT NULL PRIMARY KEY, value "
                + rdbEngine.getDataTypeForEngine(DataType.INT)
                + ")");

        // Insert initial data
        stmt.executeUpdate("INSERT INTO ns.tbl1 (id, value) VALUES (0, 0)");
        stmt.executeUpdate("INSERT INTO ns.tbl2 (id, value) VALUES (0, 0)");
      }

      Integer[] isolationLevels;
      if (rdbEngine instanceof RdbEngineSqlServer) {
        isolationLevels =
            new Integer[] {
              Connection.TRANSACTION_READ_COMMITTED,
              Connection.TRANSACTION_REPEATABLE_READ,
              SQLServerConnection.TRANSACTION_SNAPSHOT,
              Connection.TRANSACTION_SERIALIZABLE
            };
      } else {
        isolationLevels =
            new Integer[] {
              Connection.TRANSACTION_READ_COMMITTED,
              Connection.TRANSACTION_REPEATABLE_READ,
              Connection.TRANSACTION_SERIALIZABLE
            };
      }

      for (Integer isolationLevel : isolationLevels) {
        try {
          try (Connection connection1 = dataSource.getConnection();
              Connection connection2 = dataSource.getConnection()) {
            connection1.setAutoCommit(false);
            connection2.setAutoCommit(false);
            connection1.setTransactionIsolation(isolationLevel);
            connection2.setTransactionIsolation(isolationLevel);

            System.out.println("Testing isolation level: " + getIsolationLevelName(isolationLevel));

            try (Statement stmt1 = connection1.createStatement();
                Statement stmt2 = connection2.createStatement()) {
              if (rdbEngine instanceof RdbEngineSqlServer) {
                stmt1.execute("SET LOCK_TIMEOUT 5000");
                stmt2.execute("SET LOCK_TIMEOUT 5000");
              } else if (rdbEngine instanceof RdbEngineDb2) {
                stmt1.execute("SET CURRENT LOCK TIMEOUT WAIT 5");
                stmt2.execute("SET CURRENT LOCK TIMEOUT WAIT 5");
              }

              int value1;
              try (ResultSet rs1 = stmt1.executeQuery("SELECT value FROM ns.tbl1 WHERE id = 0")) {
                rs1.next();
                value1 = rs1.getInt("value");
              }

              stmt2.executeUpdate("UPDATE ns.tbl1 SET value = 100 WHERE id = 0");
              stmt2.executeUpdate("UPDATE ns.tbl2 SET value = 200 WHERE id = 0");
              connection2.commit();

              int value2;
              try (ResultSet rs1 = stmt1.executeQuery("SELECT value FROM ns.tbl2 WHERE id = 0")) {
                rs1.next();
                value2 = rs1.getInt("value");
              }
              connection1.commit();

              System.out.printf("Read values: ns.tbl1 -> %d, ns.tbl2 -> %d%n", value1, value2);
            }
          }

          // Reset data
          try (Connection connection = dataSource.getConnection();
              Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("UPDATE ns.tbl1 SET value = 0 WHERE id = 0");
            stmt.executeUpdate("UPDATE ns.tbl2 SET value = 0 WHERE id = 0");
          }
        } catch (Exception e) {
          System.out.println(
              "An error occurred while testing isolation level: "
                  + getIsolationLevelName(isolationLevel));
          e.printStackTrace();
        }
      }
    } finally {
      // Drop the test table
      //      try (Connection connection = dataSource.getConnection();
      //          Statement stmt = connection.createStatement()) {
      //        stmt.execute("DROP TABLE ns.tbl1");
      //        stmt.execute("DROP TABLE ns.tbl2");
      //        stmt.execute("DROP SCHEMA ns");
      //      }
    }
  }

  String getIsolationLevelName(int isolationLevel) {
    switch (isolationLevel) {
      case Connection.TRANSACTION_READ_COMMITTED:
        return "READ_COMMITTED";
      case Connection.TRANSACTION_REPEATABLE_READ:
        return "REPEATABLE_READ";
      case SQLServerConnection.TRANSACTION_SNAPSHOT:
        return "SNAPSHOT";
      case Connection.TRANSACTION_SERIALIZABLE:
        return "SERIALIZABLE";
      default:
        return "UNKNOWN";
    }
  }
}
