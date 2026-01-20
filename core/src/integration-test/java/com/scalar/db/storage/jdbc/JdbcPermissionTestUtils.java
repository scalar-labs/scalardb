package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.PermissionTestUtils;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JdbcPermissionTestUtils implements PermissionTestUtils {
  public static final int DDL_WAIT_SECONDS = 1;
  private final RdbEngineStrategy rdbEngine;
  private final HikariDataSource dataSource;

  public JdbcPermissionTestUtils(Properties properties) {
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
  }

  @Override
  public void createNormalUser(String userName, String password) {
    if (!JdbcTestUtils.isDb2(rdbEngine)) {
      try (Connection connection = dataSource.getConnection()) {
        String createUserSql = getCreateUserSql(userName, password);
        try (Statement statement = connection.createStatement()) {
          statement.execute(createUserSql);
        }
      } catch (SQLException e) {
        throw new RuntimeException("Failed to create user: " + userName, e);
      }
    }
  }

  @Override
  public void dropNormalUser(String userName) {
    if (!JdbcTestUtils.isDb2(rdbEngine)) {
      try (Connection connection = dataSource.getConnection()) {
        String dropUserSql = getDropUserSql(userName);
        try (Statement statement = connection.createStatement()) {
          statement.execute(dropUserSql);
        }
      } catch (SQLException e) {
        throw new RuntimeException("Failed to drop user: " + userName, e);
      }
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
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      return String.format("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", userName, password);
    } else if (JdbcTestUtils.isOracle(rdbEngine)) {
      return String.format("CREATE USER %s IDENTIFIED BY %s", userName, password);
    } else if (JdbcTestUtils.isPostgresql(rdbEngine)) {
      return String.format("CREATE USER %s WITH PASSWORD '%s'", userName, password);
    } else if (JdbcTestUtils.isSqlite(rdbEngine)) {
      throw new UnsupportedOperationException("SQLite does not support user management");
    } else if (JdbcTestUtils.isSqlServer(rdbEngine)) {
      return String.format(
          "CREATE LOGIN %s WITH PASSWORD = '%s', DEFAULT_DATABASE = master , CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF; CREATE USER %s FOR LOGIN %s",
          userName, password, userName, userName);
    } else {
      throw new UnsupportedOperationException("Creating users is not supported for " + rdbEngine);
    }
  }

  private String getDropUserSql(String userName) {
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      return String.format("DROP USER '%s'@'%%'", userName);
    } else if (JdbcTestUtils.isOracle(rdbEngine)) {
      return String.format("DROP USER %s", userName);
    } else if (JdbcTestUtils.isPostgresql(rdbEngine)) {
      return String.format("DROP USER %s", userName);
    } else if (JdbcTestUtils.isSqlite(rdbEngine)) {
      throw new UnsupportedOperationException("SQLite does not support user management");
    } else if (JdbcTestUtils.isSqlServer(rdbEngine)) {
      return String.format("DROP USER %s; DROP LOGIN %s", userName, userName);
    } else {
      throw new UnsupportedOperationException("Dropping users is not supported for " + rdbEngine);
    }
  }

  private String[] getGrantPermissionStatements(String userName) {
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      return new String[] {
        String.format("GRANT CREATE ON *.* TO '%s'@'%%'", userName),
        String.format("GRANT DROP ON *.* TO '%s'@'%%'", userName),
        String.format("GRANT INDEX ON *.* TO '%s'@'%%'", userName),
        String.format("GRANT ALTER ON *.* TO '%s'@'%%'", userName),
        String.format("GRANT SELECT ON *.* TO '%s'@'%%'", userName),
        String.format("GRANT INSERT ON *.* TO '%s'@'%%'", userName),
        String.format("GRANT UPDATE ON *.* TO '%s'@'%%'", userName),
        String.format("GRANT DELETE ON *.* TO '%s'@'%%'", userName)
      };
    } else if (JdbcTestUtils.isOracle(rdbEngine)) {
      return new String[] {
        String.format("GRANT CREATE SESSION TO %s", userName),
        String.format("GRANT CREATE USER TO %s", userName),
        String.format("GRANT DROP USER TO %s", userName),
        String.format("GRANT ALTER USER TO %s", userName),
        String.format("GRANT CREATE ANY TABLE TO %s", userName),
        String.format("GRANT DROP ANY TABLE TO %s", userName),
        String.format("GRANT CREATE ANY INDEX TO %s", userName),
        String.format("GRANT DROP ANY INDEX TO %s", userName),
        String.format("GRANT ALTER ANY TABLE TO %s", userName),
        String.format("GRANT SELECT ANY TABLE TO %s", userName),
        String.format("GRANT INSERT ANY TABLE TO %s", userName),
        String.format("GRANT UPDATE ANY TABLE TO %s", userName),
        String.format("GRANT DELETE ANY TABLE TO %s", userName)
      };
    } else if (JdbcTestUtils.isPostgresql(rdbEngine)) {
      return new String[] {String.format("ALTER ROLE %s SUPERUSER;", userName)};
    } else if (JdbcTestUtils.isSqlite(rdbEngine)) {
      throw new UnsupportedOperationException("SQLite does not support authorization mechanism");
    } else if (JdbcTestUtils.isSqlServer(rdbEngine)) {
      return new String[] {
        String.format("ALTER ROLE [db_ddladmin] ADD MEMBER %s", userName),
        String.format("ALTER ROLE [db_datareader] ADD MEMBER %s", userName),
        String.format("ALTER ROLE [db_datawriter] ADD MEMBER %s", userName)
      };
    } else if (JdbcTestUtils.isDb2(rdbEngine)) {
      return new String[] {
        String.format("GRANT DBADM ON DATABASE TO USER %s", userName),
        String.format("GRANT DATAACCESS ON DATABASE TO USER %s", userName)
      };
    } else {
      throw new UnsupportedOperationException(
          "Granting permissions is not supported for " + rdbEngine);
    }
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
