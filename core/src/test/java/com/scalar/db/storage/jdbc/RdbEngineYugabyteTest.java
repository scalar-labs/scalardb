package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.scalar.db.config.DatabaseConfig;
import com.zaxxer.hikari.HikariConfig;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class RdbEngineYugabyteTest {

  @Test
  void buildHikariConfig() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, "jdbc:yugabytedb://localhost:5433/mydb");
    props.setProperty(DatabaseConfig.USERNAME, "my-user");
    props.setProperty(DatabaseConfig.PASSWORD, "my-password");
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");
    props.setProperty(DatabaseConfig.SYSTEM_NAMESPACE_NAME, "my-namespace");
    props.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "1");
    props.setProperty(JdbcConfig.CONNECTION_POOL_MAX_IDLE, "100");
    props.setProperty(JdbcConfig.CONNECTION_POOL_MAX_TOTAL, "500");
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_ENABLED, "true");
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_MAX_OPEN, "300");
    props.setProperty(JdbcConfig.ISOLATION_LEVEL, Isolation.SERIALIZABLE.name());
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MIN_IDLE, "100");
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_IDLE, "200");
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL, "300");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MIN_IDLE, "50");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_IDLE, "150");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_TOTAL, "200");
    JdbcConfig jdbcConfig = new JdbcConfig(new DatabaseConfig(props));

    RdbEngineYugabyte engine = new RdbEngineYugabyte();

    // Act
    HikariConfig hikariConfig = engine.buildHikariConfig(jdbcConfig);

    // Assert
    assertThat(hikariConfig.getDataSourceClassName())
        .isEqualTo("com.yugabyte.ysql.YBClusterAwareDataSource");
    assertThat(hikariConfig.getDataSourceProperties().getProperty("serverName"))
        .isEqualTo("localhost");
    assertThat(hikariConfig.getDataSourceProperties().getProperty("portNumber")).isEqualTo("5433");
    assertThat(hikariConfig.getDataSourceProperties().getProperty("databaseName"))
        .isEqualTo("mydb");
    assertThat(hikariConfig.getDataSourceProperties().getProperty("user")).isEqualTo("my-user");
    assertThat(hikariConfig.getDataSourceProperties().getProperty("password"))
        .isEqualTo("my-password");
  }
}
