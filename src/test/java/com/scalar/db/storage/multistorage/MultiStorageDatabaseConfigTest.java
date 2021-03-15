package com.scalar.db.storage.multistorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.cassandra.Cassandra;
import com.scalar.db.storage.jdbc.JdbcDatabase;
import java.util.Collections;
import java.util.Properties;
import org.junit.Test;

public class MultiStorageDatabaseConfigTest {

  @Test
  public void constructor_AllPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multistorage");

    props.setProperty(MultiStorageDatabaseConfig.STORAGES, "cassandra,mysql");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.storage", "cassandra");
    props.setProperty(
        MultiStorageDatabaseConfig.STORAGES + ".cassandra.contact_points", "localhost");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.contact_port", "7000");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.username", "cassandra");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.password", "cassandra");
    props.setProperty(
        MultiStorageDatabaseConfig.STORAGES + ".cassandra.namespace_prefix", "prefix");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiStorageDatabaseConfig.STORAGES + ".mysql.contact_points",
        "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".mysql.username", "root");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".mysql.password", "mysql");

    props.setProperty(
        MultiStorageDatabaseConfig.TABLE_MAPPING,
        "user.order:cassandra,user.customer:mysql,coordinator.state:cassandra");

    props.setProperty(MultiStorageDatabaseConfig.DEFAULT_STORAGE, "cassandra");

    // Act
    MultiStorageDatabaseConfig config = new MultiStorageDatabaseConfig(props);

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

    assertThat(config.getTableStorageMap().size()).isEqualTo(3);
    assertThat(config.getTableStorageMap().get("user.order")).isEqualTo("cassandra");
    assertThat(config.getTableStorageMap().get("user.customer")).isEqualTo("mysql");
    assertThat(config.getTableStorageMap().get("coordinator.state")).isEqualTo("cassandra");

    assertThat(config.getDefaultStorage()).isEqualTo("cassandra");
  }

  @Test
  public void constructor_WrongStorageNameGiven_ShouldThrowIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "aaa"); // wrong

    props.setProperty(MultiStorageDatabaseConfig.STORAGES, "cassandra,mysql");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.storage", "cassandra");
    props.setProperty(
        MultiStorageDatabaseConfig.STORAGES + ".cassandra.contact_points", "localhost");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.contact_port", "7000");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.username", "cassandra");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.password", "cassandra");
    props.setProperty(
        MultiStorageDatabaseConfig.STORAGES + ".cassandra.namespace_prefix", "prefix");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiStorageDatabaseConfig.STORAGES + ".mysql.contact_points",
        "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".mysql.username", "root");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".mysql.password", "mysql");

    props.setProperty(
        MultiStorageDatabaseConfig.TABLE_MAPPING,
        "user.order:cassandra,user.customer:mysql,coordinator.state:cassandra");

    props.setProperty(MultiStorageDatabaseConfig.DEFAULT_STORAGE, "cassandra");

    // Act Assert
    assertThatThrownBy(() -> new MultiStorageDatabaseConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_NonExistentStorageGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multistorage");

    props.setProperty(MultiStorageDatabaseConfig.STORAGES, "cassandra,mysql");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.storage", "cassandra");
    props.setProperty(
        MultiStorageDatabaseConfig.STORAGES + ".cassandra.contact_points", "localhost");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.contact_port", "7000");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.username", "cassandra");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.password", "cassandra");
    props.setProperty(
        MultiStorageDatabaseConfig.STORAGES + ".cassandra.namespace_prefix", "prefix");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiStorageDatabaseConfig.STORAGES + ".mysql.contact_points",
        "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".mysql.username", "root");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".mysql.password", "mysql");

    props.setProperty(
        MultiStorageDatabaseConfig.TABLE_MAPPING,
        "user.order:cassandra,user.customer:mysql,"
            + "coordinator.state:dynamo"); // non-existent storage

    props.setProperty(MultiStorageDatabaseConfig.DEFAULT_STORAGE, "cassandra");

    // Act Assert
    assertThatThrownBy(() -> new MultiStorageDatabaseConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_NonExistentStorageForDefaultGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multistorage");

    props.setProperty(MultiStorageDatabaseConfig.STORAGES, "cassandra,mysql");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.storage", "cassandra");
    props.setProperty(
        MultiStorageDatabaseConfig.STORAGES + ".cassandra.contact_points", "localhost");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.contact_port", "7000");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.username", "cassandra");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".cassandra.password", "cassandra");
    props.setProperty(
        MultiStorageDatabaseConfig.STORAGES + ".cassandra.namespace_prefix", "prefix");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiStorageDatabaseConfig.STORAGES + ".mysql.contact_points",
        "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".mysql.username", "root");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".mysql.password", "mysql");

    props.setProperty(
        MultiStorageDatabaseConfig.TABLE_MAPPING,
        "user.order:cassandra,user.customer:mysql,coordinator.state:cassandra");

    props.setProperty(MultiStorageDatabaseConfig.DEFAULT_STORAGE, "dynamo"); // non-existent storage

    // Act Assert
    assertThatThrownBy(() -> new MultiStorageDatabaseConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_NestedMultiStorageGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multistorage");

    props.setProperty(MultiStorageDatabaseConfig.STORAGES, "db,mysql");
    props.setProperty(
        MultiStorageDatabaseConfig.STORAGES + ".db.storage", "multistorage"); // nested
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".db.contact_points", "localhost");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".db.username", "user");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".db.password", "pass");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiStorageDatabaseConfig.STORAGES + ".mysql.contact_points",
        "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".mysql.username", "root");
    props.setProperty(MultiStorageDatabaseConfig.STORAGES + ".mysql.password", "mysql");

    props.setProperty(
        MultiStorageDatabaseConfig.TABLE_MAPPING,
        "user.order:cassandra,user.customer:mysql,coordinator.state:cassandra");

    props.setProperty(MultiStorageDatabaseConfig.DEFAULT_STORAGE, "cassandra");

    // Act Assert
    assertThatThrownBy(() -> new MultiStorageDatabaseConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
