package com.scalar.db.storage.multi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.cassandra.Cassandra;
import com.scalar.db.storage.jdbc.JdbcDatabase;
import java.util.Collections;
import java.util.Properties;
import org.junit.Test;

public class MultiDatabasesConfigTest {

  @Test
  public void constructor_AllPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi");

    props.setProperty(MultiDatabasesConfig.DATABASES, "cassandra,mysql");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.storage", "cassandra");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.contact_points", "localhost");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.contact_port", "7000");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.username", "cassandra");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.password", "cassandra");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.namespace_prefix", "prefix");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiDatabasesConfig.DATABASES + ".mysql.contact_points", "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.username", "root");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.password", "mysql");

    props.setProperty(
        MultiDatabasesConfig.TABLE_MAPPING, "user.order,user.customer,coordinator.state");
    props.setProperty(MultiDatabasesConfig.TABLE_MAPPING + ".user.order", "cassandra");
    props.setProperty(MultiDatabasesConfig.TABLE_MAPPING + ".user.customer", "mysql");
    props.setProperty(MultiDatabasesConfig.TABLE_MAPPING + ".coordinator.state", "cassandra");

    props.setProperty(MultiDatabasesConfig.DEFAULT_DATABASE, "cassandra");

    // Act
    MultiDatabasesConfig config = new MultiDatabasesConfig(props);

    // Assert
    assertThat(config.getDatabaseConfigMap().size()).isEqualTo(2);
    assertThat(config.getDatabaseConfigMap().containsKey("cassandra")).isTrue();
    DatabaseConfig c = config.getDatabaseConfigMap().get("cassandra");
    assertThat(c.getStorageClass()).isEqualTo(Cassandra.class);
    assertThat(c.getContactPoints()).isEqualTo(Collections.singletonList("localhost"));
    assertThat(c.getContactPort()).isEqualTo(7000);
    assertThat(c.getUsername()).isEqualTo("cassandra");
    assertThat(c.getPassword()).isEqualTo("cassandra");
    assertThat(c.getNamespacePrefix().isPresent()).isTrue();
    assertThat(c.getNamespacePrefix().get()).isEqualTo("prefix_");
    assertThat(config.getDatabaseConfigMap().containsKey("mysql")).isTrue();
    c = config.getDatabaseConfigMap().get("mysql");
    assertThat(c.getStorageClass()).isEqualTo(JdbcDatabase.class);
    assertThat(c.getContactPoints())
        .isEqualTo(Collections.singletonList("jdbc:mysql://localhost:3306/"));
    assertThat(c.getUsername()).isEqualTo("root");
    assertThat(c.getPassword()).isEqualTo("mysql");
    assertThat(c.getNamespacePrefix().isPresent()).isFalse();

    assertThat(config.getTableDatabaseMap().size()).isEqualTo(3);
    assertThat(config.getTableDatabaseMap().get("user.order")).isEqualTo("cassandra");
    assertThat(config.getTableDatabaseMap().get("user.customer")).isEqualTo("mysql");
    assertThat(config.getTableDatabaseMap().get("coordinator.state")).isEqualTo("cassandra");

    assertThat(config.getDefaultDatabase()).isEqualTo("cassandra");
  }

  @Test
  public void constructor_WrongStorageNameGiven_ShouldThrowIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "aaa"); // wrong

    props.setProperty(MultiDatabasesConfig.DATABASES, "cassandra,mysql");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.storage", "cassandra");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.contact_points", "localhost");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.contact_port", "7000");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.username", "cassandra");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.password", "cassandra");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.namespace_prefix", "prefix");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiDatabasesConfig.DATABASES + ".mysql.contact_points", "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.username", "root");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.password", "mysql");

    props.setProperty(
        MultiDatabasesConfig.TABLE_MAPPING, "user.order,user.customer,coordinator.state");
    props.setProperty(MultiDatabasesConfig.TABLE_MAPPING + ".user.order", "cassandra");
    props.setProperty(MultiDatabasesConfig.TABLE_MAPPING + ".user.customer", "mysql");
    props.setProperty(MultiDatabasesConfig.TABLE_MAPPING + ".coordinator.state", "cassandra");

    props.setProperty(MultiDatabasesConfig.DEFAULT_DATABASE, "cassandra");

    // Act Assert
    assertThatThrownBy(() -> new MultiDatabasesConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_NonExistentDatabaseGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi");

    props.setProperty(MultiDatabasesConfig.DATABASES, "cassandra,mysql");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.storage", "cassandra");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.contact_points", "localhost");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.contact_port", "7000");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.username", "cassandra");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.password", "cassandra");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.namespace_prefix", "prefix");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiDatabasesConfig.DATABASES + ".mysql.contact_points", "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.username", "root");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.password", "mysql");

    props.setProperty(
        MultiDatabasesConfig.TABLE_MAPPING, "user.order,user.customer,coordinator.state");
    props.setProperty(MultiDatabasesConfig.TABLE_MAPPING + ".user.order", "cassandra");
    props.setProperty(MultiDatabasesConfig.TABLE_MAPPING + ".user.customer", "mysql");
    props.setProperty(
        MultiDatabasesConfig.TABLE_MAPPING + ".coordinator.state",
        "dynamo"); // non-existent database

    props.setProperty(MultiDatabasesConfig.DEFAULT_DATABASE, "cassandra");

    // Act Assert
    assertThatThrownBy(() -> new MultiDatabasesConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_NonExistentDatabaseForDefaultGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi");

    props.setProperty(MultiDatabasesConfig.DATABASES, "cassandra,mysql");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.storage", "cassandra");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.contact_points", "localhost");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.contact_port", "7000");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.username", "cassandra");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.password", "cassandra");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.namespace_prefix", "prefix");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiDatabasesConfig.DATABASES + ".mysql.contact_points", "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.username", "root");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.password", "mysql");

    props.setProperty(
        MultiDatabasesConfig.TABLE_MAPPING, "user.order,user.customer,coordinator.state");
    props.setProperty(MultiDatabasesConfig.TABLE_MAPPING + ".user.order", "cassandra");
    props.setProperty(MultiDatabasesConfig.TABLE_MAPPING + ".user.customer", "mysql");
    props.setProperty(MultiDatabasesConfig.TABLE_MAPPING + ".coordinator.state", "cassandra");

    props.setProperty(MultiDatabasesConfig.DEFAULT_DATABASE, "dynamo"); // non-existent database

    // Act Assert
    assertThatThrownBy(() -> new MultiDatabasesConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_NestedMultiDatabasesGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi");

    props.setProperty(MultiDatabasesConfig.DATABASES, "db,mysql");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".db.storage", "multi"); // nested
    props.setProperty(MultiDatabasesConfig.DATABASES + ".db.contact_points", "localhost");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".db.username", "user");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".db.password", "pass");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiDatabasesConfig.DATABASES + ".mysql.contact_points", "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.username", "root");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.password", "mysql");

    props.setProperty(
        MultiDatabasesConfig.TABLE_MAPPING, "user.order,user.customer,coordinator.state");
    props.setProperty(MultiDatabasesConfig.TABLE_MAPPING + ".user.order", "cassandra");
    props.setProperty(MultiDatabasesConfig.TABLE_MAPPING + ".user.customer", "mysql");
    props.setProperty(MultiDatabasesConfig.TABLE_MAPPING + ".coordinator.state", "cassandra");

    props.setProperty(MultiDatabasesConfig.DEFAULT_DATABASE, "cassandra");

    // Act Assert
    assertThatThrownBy(() -> new MultiDatabasesConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
