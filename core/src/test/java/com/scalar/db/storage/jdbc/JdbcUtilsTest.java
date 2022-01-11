package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.config.DatabaseConfig;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;

public class JdbcUtilsTest {

  @Test
  public void initDataSource_NonTransactional_ShouldReturnProperDataSource() throws SQLException {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, "jdbc:mysql://localhost:3306/");
    properties.setProperty(DatabaseConfig.USERNAME, "root");
    properties.setProperty(DatabaseConfig.PASSWORD, "mysql");
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(JdbcConfig.ISOLATION_LEVEL, "SERIALIZABLE");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "10");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MAX_IDLE, "20");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MAX_TOTAL, "30");
    properties.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_ENABLED, "true");
    properties.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_MAX_OPEN, "100");

    JdbcConfig config = new JdbcConfig(properties);

    // Act
    BasicDataSource dataSource = JdbcUtils.initDataSource(config);

    // Assert
    assertThat(dataSource.getUrl()).isEqualTo("jdbc:mysql://localhost:3306/");
    assertThat(dataSource.getDriver().getClass().getName()).isEqualTo("com.mysql.cj.jdbc.Driver");
    assertThat(dataSource.getUsername()).isEqualTo("root");
    assertThat(dataSource.getPassword()).isEqualTo("mysql");

    assertThat(dataSource.getDefaultAutoCommit()).isEqualTo(null);
    assertThat(dataSource.getAutoCommitOnReturn()).isEqualTo(true);
    assertThat(dataSource.getDefaultTransactionIsolation())
        .isEqualTo(Connection.TRANSACTION_SERIALIZABLE);

    assertThat(dataSource.getMinIdle()).isEqualTo(10);
    assertThat(dataSource.getMaxIdle()).isEqualTo(20);
    assertThat(dataSource.getMaxTotal()).isEqualTo(30);
    assertThat(dataSource.isPoolPreparedStatements()).isEqualTo(true);
    assertThat(dataSource.getMaxOpenPreparedStatements()).isEqualTo(100);

    dataSource.close();
  }

  @Test
  public void initDataSource_Transactional_ShouldReturnProperDataSource() throws SQLException {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, "jdbc:postgresql://localhost:5432/");
    properties.setProperty(DatabaseConfig.USERNAME, "user");
    properties.setProperty(DatabaseConfig.PASSWORD, "postgres");
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(JdbcConfig.ISOLATION_LEVEL, "READ_COMMITTED");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "30");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MAX_IDLE, "40");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MAX_TOTAL, "50");
    properties.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_ENABLED, "true");
    properties.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_MAX_OPEN, "200");

    JdbcConfig config = new JdbcConfig(properties);

    // Act
    BasicDataSource dataSource = JdbcUtils.initDataSource(config, true);

    // Assert
    assertThat(dataSource.getUrl()).isEqualTo("jdbc:postgresql://localhost:5432/");
    assertThat(dataSource.getDriver().getClass().getName()).isEqualTo("org.postgresql.Driver");
    assertThat(dataSource.getUsername()).isEqualTo("user");
    assertThat(dataSource.getPassword()).isEqualTo("postgres");

    assertThat(dataSource.getDefaultAutoCommit()).isEqualTo(false);
    assertThat(dataSource.getAutoCommitOnReturn()).isEqualTo(false);
    assertThat(dataSource.getDefaultTransactionIsolation())
        .isEqualTo(Connection.TRANSACTION_READ_COMMITTED);

    assertThat(dataSource.getMinIdle()).isEqualTo(30);
    assertThat(dataSource.getMaxIdle()).isEqualTo(40);
    assertThat(dataSource.getMaxTotal()).isEqualTo(50);
    assertThat(dataSource.isPoolPreparedStatements()).isEqualTo(true);
    assertThat(dataSource.getMaxOpenPreparedStatements()).isEqualTo(200);

    dataSource.close();
  }

  @Test
  public void initDataSourceForTableMetadata_ShouldReturnProperDataSource() throws SQLException {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(
        DatabaseConfig.CONTACT_POINTS, "jdbc:oracle:thin:@localhost:1521/XEPDB1");
    properties.setProperty(DatabaseConfig.USERNAME, "user");
    properties.setProperty(DatabaseConfig.PASSWORD, "oracle");
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MIN_IDLE, "100");
    properties.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_IDLE, "200");
    properties.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL, "300");

    JdbcConfig config = new JdbcConfig(properties);

    // Act
    BasicDataSource tableMetadataDataSource = JdbcUtils.initDataSourceForTableMetadata(config);

    // Assert
    assertThat(tableMetadataDataSource.getUrl())
        .isEqualTo("jdbc:oracle:thin:@localhost:1521/XEPDB1");
    assertThat(tableMetadataDataSource.getDriver().getClass().getName())
        .isEqualTo("oracle.jdbc.OracleDriver");
    assertThat(tableMetadataDataSource.getUsername()).isEqualTo("user");
    assertThat(tableMetadataDataSource.getPassword()).isEqualTo("oracle");

    assertThat(tableMetadataDataSource.getMinIdle()).isEqualTo(100);
    assertThat(tableMetadataDataSource.getMaxIdle()).isEqualTo(200);
    assertThat(tableMetadataDataSource.getMaxTotal()).isEqualTo(300);

    tableMetadataDataSource.close();
  }

  @Test
  public void initDataSourceForAdmin_ShouldReturnProperDataSource() throws SQLException {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, "jdbc:sqlserver://localhost:1433");
    properties.setProperty(DatabaseConfig.USERNAME, "user");
    properties.setProperty(DatabaseConfig.PASSWORD, "sqlserver");
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MIN_IDLE, "100");
    properties.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_IDLE, "200");
    properties.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_TOTAL, "300");

    JdbcConfig config = new JdbcConfig(properties);

    // Act
    BasicDataSource adminDataSource = JdbcUtils.initDataSourceForAdmin(config);

    // Assert
    assertThat(adminDataSource.getUrl()).isEqualTo("jdbc:sqlserver://localhost:1433");
    assertThat(adminDataSource.getDriver().getClass().getName())
        .isEqualTo("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    assertThat(adminDataSource.getUsername()).isEqualTo("user");
    assertThat(adminDataSource.getPassword()).isEqualTo("sqlserver");

    assertThat(adminDataSource.getMinIdle()).isEqualTo(100);
    assertThat(adminDataSource.getMaxIdle()).isEqualTo(200);
    assertThat(adminDataSource.getMaxTotal()).isEqualTo(300);

    adminDataSource.close();
  }
}
