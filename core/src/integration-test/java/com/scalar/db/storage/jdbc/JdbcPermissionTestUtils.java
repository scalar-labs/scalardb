package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.PermissionTestUtils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

public class JdbcPermissionTestUtils implements PermissionTestUtils {
  private final RdbEngineStrategy rdbEngine;
  private final BasicDataSource dataSource;

  public JdbcPermissionTestUtils(Properties properties) {
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
  }

  @Override
  public void createNormalUser(String userName, String password) {
    try (Connection connection = dataSource.getConnection()) {
      String createUserSql = getCreateUserSql(userName, password);
      try (Statement statement = connection.createStatement()) {
        statement.execute(createUserSql);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to create user: " + userName, e);
    }
  }

  @Override
  public void dropNormalUser(String userName) {
    try (Connection connection = dataSource.getConnection()) {
      String dropUserSql = getDropUserSql(userName);
      try (Statement statement = connection.createStatement()) {
        statement.execute(dropUserSql);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to drop user: " + userName, e);
    }
  }

  @Override
  public void grantRequiredPermission(String userName) {
    try (Connection connection = dataSource.getConnection()) {
      String[] grantStatements = getGrantPermissionStatements(userName);
      try (Statement statement = connection.createStatement()) {
        for (String sql : grantStatements) {
          statement.execute(sql);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to grant permissions to user: " + userName, e);
    }
  }

  private String getCreateUserSql(String userName, String password) {
    if (!(rdbEngine instanceof RdbEngineMysql)) {
      throw new UnsupportedOperationException("Creating users is only supported for MySQL");
    }
    return String.format(
        "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s'", userName, password);
  }

  private String getDropUserSql(String userName) {
    if (!(rdbEngine instanceof RdbEngineMysql)) {
      throw new UnsupportedOperationException("Dropping users is only supported for MySQL");
    }
    return String.format("DROP USER IF EXISTS '%s'@'%%'", userName);
  }

  private String[] getGrantPermissionStatements(String userName) {
    if (!(rdbEngine instanceof RdbEngineMysql)) {
      throw new UnsupportedOperationException("Granting permissions is only supported for MySQL");
    }
    return new String[] {
      String.format("GRANT CREATE ON *.* TO '%s'@'%%'", userName),
      String.format("GRANT DROP ON *.* TO '%s'@'%%'", userName),
      String.format("GRANT INDEX ON *.* TO '%s'@'%%'", userName),
      String.format("GRANT ALTER ON *.* TO '%s'@'%%'", userName),
      String.format("GRANT SELECT ON *.* TO '%s'@'%%'", userName),
      String.format("GRANT INSERT ON *.* TO '%s'@'%%'", userName),
      String.format("GRANT UPDATE ON *.* TO '%s'@'%%'", userName),
      String.format("GRANT DELETE ON *.* TO '%s'@'%%'", userName),
      "FLUSH PRIVILEGES"
    };
  }

  @Override
  public void close() {
    try {
      dataSource.close();
    } catch (Exception e) {
      throw new RuntimeException("Failed to close the data source", e);
    }
  }
}
