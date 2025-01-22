package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class JdbcConfigTest {

  private static final String ANY_JDBC_URL = "jdbc:mysql://localhost:3306/";
  private static final String ANY_USERNAME = "root";
  private static final String ANY_PASSWORD = "mysql";
  private static final String JDBC_STORAGE = "jdbc";
  private static final String ANY_TABLE_METADATA_SCHEMA = "any_schema";

  @Test
  public void constructor_AllPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);
    props.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "1");
    props.setProperty(JdbcConfig.CONNECTION_POOL_MAX_IDLE, "100");
    props.setProperty(JdbcConfig.CONNECTION_POOL_MAX_TOTAL, "500");
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_ENABLED, "true");
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_MAX_OPEN, "300");
    props.setProperty(JdbcConfig.ISOLATION_LEVEL, Isolation.SERIALIZABLE.name());
    props.setProperty(JdbcConfig.TABLE_METADATA_SCHEMA, ANY_TABLE_METADATA_SCHEMA);
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MIN_IDLE, "100");
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_IDLE, "200");
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL, "300");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MIN_IDLE, "50");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_IDLE, "150");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_TOTAL, "200");
    props.setProperty(JdbcConfig.MYSQL_VARIABLE_KEY_COLUMN_SIZE, "64");
    props.setProperty(JdbcConfig.ORACLE_VARIABLE_KEY_COLUMN_SIZE, "64");
    props.setProperty(JdbcConfig.ORACLE_TIME_COLUMN_DEFAULT_DATE_COMPONENT, "2020-01-01");

    // Act
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getJdbcUrl()).isEqualTo(ANY_JDBC_URL);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getConnectionPoolMinIdle()).isEqualTo(1);
    assertThat(config.getConnectionPoolMaxIdle()).isEqualTo(100);
    assertThat(config.getConnectionPoolMaxTotal()).isEqualTo(500);
    assertThat(config.isPreparedStatementsPoolEnabled()).isEqualTo(true);
    assertThat(config.getPreparedStatementsPoolMaxOpen()).isEqualTo(300);
    assertThat(config.getIsolation()).isPresent();
    assertThat(config.getIsolation().get()).isEqualTo(Isolation.SERIALIZABLE);
    assertThat(config.getTableMetadataSchema()).isPresent();
    assertThat(config.getTableMetadataSchema().get()).isEqualTo(ANY_TABLE_METADATA_SCHEMA);
    assertThat(config.getTableMetadataConnectionPoolMinIdle()).isEqualTo(100);
    assertThat(config.getTableMetadataConnectionPoolMaxIdle()).isEqualTo(200);
    assertThat(config.getTableMetadataConnectionPoolMaxTotal()).isEqualTo(300);
    assertThat(config.getAdminConnectionPoolMinIdle()).isEqualTo(50);
    assertThat(config.getAdminConnectionPoolMaxIdle()).isEqualTo(150);
    assertThat(config.getAdminConnectionPoolMaxTotal()).isEqualTo(200);
    assertThat(config.getMysqlVariableKeyColumnSize()).isEqualTo(64);
    assertThat(config.getOracleVariableKeyColumnSize()).isEqualTo(64);
    assertThat(config.getOracleTimeColumnDefaultDateComponent())
        .isEqualTo(LocalDate.parse("2020-01-01", DateTimeFormatter.ISO_LOCAL_DATE));
  }

  @Test
  public void
      constructor_PropertiesWithoutConnectionPoolPropertiesGiven_ShouldLoadProperlyAndUseDefaultValues() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);

    // Act
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getJdbcUrl()).isEqualTo(ANY_JDBC_URL);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getConnectionPoolMinIdle())
        .isEqualTo(JdbcConfig.DEFAULT_CONNECTION_POOL_MIN_IDLE);
    assertThat(config.getConnectionPoolMaxIdle())
        .isEqualTo(JdbcConfig.DEFAULT_CONNECTION_POOL_MAX_IDLE);
    assertThat(config.getConnectionPoolMaxTotal())
        .isEqualTo(JdbcConfig.DEFAULT_CONNECTION_POOL_MAX_TOTAL);
    assertThat(config.isPreparedStatementsPoolEnabled())
        .isEqualTo(JdbcConfig.DEFAULT_PREPARED_STATEMENTS_POOL_ENABLED);
    assertThat(config.getPreparedStatementsPoolMaxOpen())
        .isEqualTo(JdbcConfig.DEFAULT_PREPARED_STATEMENTS_POOL_MAX_OPEN);
    assertThat(config.getIsolation()).isNotPresent();
    assertThat(config.getTableMetadataSchema()).isNotPresent();
    assertThat(config.getTableMetadataConnectionPoolMinIdle())
        .isEqualTo(JdbcConfig.DEFAULT_TABLE_METADATA_CONNECTION_POOL_MIN_IDLE);
    assertThat(config.getTableMetadataConnectionPoolMaxIdle())
        .isEqualTo(JdbcConfig.DEFAULT_TABLE_METADATA_CONNECTION_POOL_MAX_IDLE);
    assertThat(config.getTableMetadataConnectionPoolMaxTotal())
        .isEqualTo(JdbcConfig.DEFAULT_TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL);
    assertThat(config.getMysqlVariableKeyColumnSize())
        .isEqualTo(JdbcConfig.DEFAULT_VARIABLE_KEY_COLUMN_SIZE);
    assertThat(config.getOracleVariableKeyColumnSize())
        .isEqualTo(JdbcConfig.DEFAULT_VARIABLE_KEY_COLUMN_SIZE);
    assertThat(config.getOracleTimeColumnDefaultDateComponent())
        .isEqualTo(JdbcConfig.DEFAULT_ORACLE_TIME_COLUMN_DEFAULT_DATE_COMPONENT);
  }

  @Test
  public void constructor_PropertiesWithWrongStorage_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "aaa");

    // Act Assert
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidConnectionPoolPropertiesGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);
    props.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "aaa");
    props.setProperty(JdbcConfig.CONNECTION_POOL_MAX_IDLE, "bbb");
    props.setProperty(JdbcConfig.CONNECTION_POOL_MAX_TOTAL, "ccc");
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_ENABLED, "ddd");
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_MAX_OPEN, "eee");

    // Act Assert
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidPreparedStatementsPoolPropertiesGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_ENABLED, "ddd");
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_MAX_OPEN, "eee");

    // Act Assert
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidIsolationLevelGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);
    props.setProperty(JdbcConfig.ISOLATION_LEVEL, "aaa");

    // Act Assert
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidTableMetadataConnectionPoolPropertiesGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MIN_IDLE, "aaa");
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_IDLE, "bbb");
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL, "ccc");

    // Act Assert
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidAdminConnectionPoolPropertiesGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MIN_IDLE, "aaa");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_IDLE, "bbb");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_TOTAL, "ccc");

    // Act Assert
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithEmptyContactPointsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);

    // Act Assert
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithSmallKeyColumnSizeGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props1 = new Properties();
    props1.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props1.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props1.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props1.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);
    props1.setProperty(JdbcConfig.MYSQL_VARIABLE_KEY_COLUMN_SIZE, "32");
    Properties props2 = new Properties();
    props2.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props2.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props2.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props2.setProperty(DatabaseConfig.STORAGE, JDBC_STORAGE);
    props2.setProperty(JdbcConfig.ORACLE_VARIABLE_KEY_COLUMN_SIZE, "32");

    // Act Assert
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props1)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new JdbcConfig(new DatabaseConfig(props2)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
