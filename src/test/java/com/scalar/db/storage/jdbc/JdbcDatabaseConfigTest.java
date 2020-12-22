package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class JdbcDatabaseConfigTest {

  private static final String ANY_JDBC_URL = "jdbc:mysql://localhost:3306/";
  private static final String ANY_USERNAME = "root";
  private static final String ANY_PASSWORD = "mysql";
  private static final String ANY_NAMESPACE_PREFIX = "prefix";

  @Test
  public void constructor_AllPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, ANY_NAMESPACE_PREFIX);
    props.setProperty(JdbcDatabaseConfig.CONNECTION_POOL_MIN_IDLE, "1");
    props.setProperty(JdbcDatabaseConfig.CONNECTION_POOL_MAX_IDLE, "100");
    props.setProperty(JdbcDatabaseConfig.CONNECTION_POOL_MAX_TOTAL, "200");
    props.setProperty(JdbcDatabaseConfig.PREPARED_STATEMENTS_POOL_ENABLED, "true");
    props.setProperty(JdbcDatabaseConfig.PREPARED_STATEMENTS_POOL_MAX_OPEN, "300");

    // Act
    JdbcDatabaseConfig config = new JdbcDatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_JDBC_URL));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getNamespacePrefix().isPresent()).isTrue();
    assertThat(config.getNamespacePrefix().get()).isEqualTo(ANY_NAMESPACE_PREFIX + "_");
    assertThat(config.getStorageClass()).isEqualTo(JdbcDatabase.class);
    assertThat(config.getConnectionPoolMinIdle()).isEqualTo(1);
    assertThat(config.getConnectionPoolMaxIdle()).isEqualTo(100);
    assertThat(config.getConnectionPoolMaxTotal()).isEqualTo(200);
    assertThat(config.isPreparedStatementsPoolEnabled()).isEqualTo(true);
    assertThat(config.getPreparedStatementsPoolMaxOpen()).isEqualTo(300);
  }

  @Test
  public void
      constructor_PropertiesWithoutConnectionPoolPropertiesGiven_ShouldLoadProperlyAndUseDefaultValues() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, ANY_NAMESPACE_PREFIX);

    // Act
    JdbcDatabaseConfig config = new JdbcDatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_JDBC_URL));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getNamespacePrefix().isPresent()).isTrue();
    assertThat(config.getNamespacePrefix().get()).isEqualTo(ANY_NAMESPACE_PREFIX + "_");
    assertThat(config.getStorageClass()).isEqualTo(JdbcDatabase.class);
    assertThat(config.getConnectionPoolMinIdle())
        .isEqualTo(JdbcDatabaseConfig.DEFAULT_CONNECTION_POOL_MIN_IDLE);
    assertThat(config.getConnectionPoolMaxIdle())
        .isEqualTo(JdbcDatabaseConfig.DEFAULT_CONNECTION_POOL_MAX_IDLE);
    assertThat(config.getConnectionPoolMaxTotal())
        .isEqualTo(JdbcDatabaseConfig.DEFAULT_CONNECTION_POOL_MAX_TOTAL);
    assertThat(config.isPreparedStatementsPoolEnabled())
        .isEqualTo(JdbcDatabaseConfig.DEFAULT_PREPARED_STATEMENTS_POOL_ENABLED);
    assertThat(config.getPreparedStatementsPoolMaxOpen())
        .isEqualTo(JdbcDatabaseConfig.DEFAULT_PREPARED_STATEMENTS_POOL_MAX_OPEN);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidConnectionPoolPropertiesGiven_ShouldLoadWithoutErrorsAndUseDefaultValues() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, ANY_NAMESPACE_PREFIX);
    props.setProperty(JdbcDatabaseConfig.CONNECTION_POOL_MIN_IDLE, "aaa");
    props.setProperty(JdbcDatabaseConfig.CONNECTION_POOL_MAX_IDLE, "bbb");
    props.setProperty(JdbcDatabaseConfig.CONNECTION_POOL_MAX_TOTAL, "ccc");
    props.setProperty(JdbcDatabaseConfig.PREPARED_STATEMENTS_POOL_ENABLED, "ddd");
    props.setProperty(JdbcDatabaseConfig.PREPARED_STATEMENTS_POOL_MAX_OPEN, "eee");

    // Act
    JdbcDatabaseConfig config = new JdbcDatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_JDBC_URL));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getNamespacePrefix().isPresent()).isTrue();
    assertThat(config.getNamespacePrefix().get()).isEqualTo(ANY_NAMESPACE_PREFIX + "_");
    assertThat(config.getStorageClass()).isEqualTo(JdbcDatabase.class);
    assertThat(config.getConnectionPoolMinIdle())
        .isEqualTo(JdbcDatabaseConfig.DEFAULT_CONNECTION_POOL_MIN_IDLE);
    assertThat(config.getConnectionPoolMaxIdle())
        .isEqualTo(JdbcDatabaseConfig.DEFAULT_CONNECTION_POOL_MAX_IDLE);
    assertThat(config.getConnectionPoolMaxTotal())
        .isEqualTo(JdbcDatabaseConfig.DEFAULT_CONNECTION_POOL_MAX_TOTAL);
    assertThat(config.isPreparedStatementsPoolEnabled())
        .isEqualTo(JdbcDatabaseConfig.DEFAULT_PREPARED_STATEMENTS_POOL_ENABLED);
    assertThat(config.getPreparedStatementsPoolMaxOpen())
        .isEqualTo(JdbcDatabaseConfig.DEFAULT_PREPARED_STATEMENTS_POOL_MAX_OPEN);
  }
}
