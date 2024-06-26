package com.scalar.db.storage.multistorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.Collections;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class MultiStorageConfigTest {

  @Test
  public void constructor_AllPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    props.setProperty(MultiStorageConfig.STORAGES, "cassandra,mysql");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.storage", "cassandra");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.contact_points", "localhost");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.contact_port", "7000");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.username", "cassandra");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.password", "cassandra");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiStorageConfig.STORAGES + ".mysql.contact_points", "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.username", "root");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.password", "mysql");

    props.setProperty(
        MultiStorageConfig.TABLE_MAPPING,
        "user.order:cassandra,user.customer:mysql,coordinator.state:cassandra");

    props.setProperty(
        MultiStorageConfig.NAMESPACE_MAPPING, "namespace1:cassandra,namespace2:mysql");

    props.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "cassandra");

    // Global properties for all storages
    props.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN, "true");

    // Act
    MultiStorageConfig config = new MultiStorageConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getDatabasePropertiesMap().size()).isEqualTo(2);

    assertThat(config.getDatabasePropertiesMap().containsKey("cassandra")).isTrue();
    DatabaseConfig c = new DatabaseConfig(config.getDatabasePropertiesMap().get("cassandra"));
    assertThat(c.getStorage()).isEqualTo("cassandra");
    assertThat(c.getContactPoints()).isEqualTo(Collections.singletonList("localhost"));
    assertThat(c.getContactPort()).isEqualTo(7000);
    assertThat(c.getUsername().isPresent()).isTrue();
    assertThat(c.getUsername().get()).isEqualTo("cassandra");
    assertThat(c.getPassword().isPresent()).isTrue();
    assertThat(c.getPassword().get()).isEqualTo("cassandra");
    assertThat(c.isCrossPartitionScanEnabled()).isTrue();

    assertThat(config.getDatabasePropertiesMap().containsKey("mysql")).isTrue();
    c = new DatabaseConfig(config.getDatabasePropertiesMap().get("mysql"));
    assertThat(c.getStorage()).isEqualTo("jdbc");
    assertThat(c.getContactPoints())
        .isEqualTo(Collections.singletonList("jdbc:mysql://localhost:3306/"));
    assertThat(c.getUsername().isPresent()).isTrue();
    assertThat(c.getUsername().get()).isEqualTo("root");
    assertThat(c.getPassword().isPresent()).isTrue();
    assertThat(c.getPassword().get()).isEqualTo("mysql");
    assertThat(c.isCrossPartitionScanEnabled()).isTrue();

    assertThat(config.getTableStorageMap().size()).isEqualTo(3);
    assertThat(config.getTableStorageMap().get("user.order")).isEqualTo("cassandra");
    assertThat(config.getTableStorageMap().get("user.customer")).isEqualTo("mysql");
    assertThat(config.getTableStorageMap().get("coordinator.state")).isEqualTo("cassandra");

    assertThat(config.getNamespaceStorageMap().size()).isEqualTo(2);
    assertThat(config.getNamespaceStorageMap().get("namespace1")).isEqualTo("cassandra");
    assertThat(config.getNamespaceStorageMap().get("namespace2")).isEqualTo("mysql");

    assertThat(config.getDefaultStorage()).isEqualTo("cassandra");
  }

  @Test
  public void constructor_WrongStorageNameGiven_ShouldThrowIllegalArgumentException() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "aaa"); // wrong

    props.setProperty(MultiStorageConfig.STORAGES, "cassandra,mysql");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.storage", "cassandra");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.contact_points", "localhost");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.contact_port", "7000");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.username", "cassandra");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.password", "cassandra");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiStorageConfig.STORAGES + ".mysql.contact_points", "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.username", "root");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.password", "mysql");

    props.setProperty(
        MultiStorageConfig.TABLE_MAPPING,
        "user.order:cassandra,user.customer:mysql,coordinator.state:cassandra");

    props.setProperty(
        MultiStorageConfig.NAMESPACE_MAPPING, "namespace1:cassandra,namespace2:mysql");

    props.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "cassandra");

    // Act Assert
    assertThatThrownBy(() -> new MultiStorageConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_NonExistentStorageGivenInTableMapping_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    props.setProperty(MultiStorageConfig.STORAGES, "cassandra,mysql");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.storage", "cassandra");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.contact_points", "localhost");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.contact_port", "7000");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.username", "cassandra");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.password", "cassandra");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiStorageConfig.STORAGES + ".mysql.contact_points", "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.username", "root");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.password", "mysql");

    props.setProperty(
        MultiStorageConfig.TABLE_MAPPING,
        "user.order:cassandra,user.customer:mysql,"
            + "coordinator.state:dynamo"); // non-existent storage

    props.setProperty(
        MultiStorageConfig.NAMESPACE_MAPPING, "namespace1:cassandra,namespace2:mysql");

    props.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "cassandra");

    // Act Assert
    assertThatThrownBy(() -> new MultiStorageConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_NonExistentStorageGivenInNamespaceMapping_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    props.setProperty(MultiStorageConfig.STORAGES, "cassandra,mysql");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.storage", "cassandra");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.contact_points", "localhost");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.contact_port", "7000");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.username", "cassandra");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.password", "cassandra");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiStorageConfig.STORAGES + ".mysql.contact_points", "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.username", "root");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.password", "mysql");

    props.setProperty(
        MultiStorageConfig.TABLE_MAPPING,
        "user.order:cassandra,user.customer:mysql,coordinator.state:cassandra");

    props.setProperty(
        MultiStorageConfig.NAMESPACE_MAPPING,
        "namespace1:cassandra,namespace2:dynamo"); // non-existent storage

    props.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "cassandra");

    // Act Assert
    assertThatThrownBy(() -> new MultiStorageConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_NonExistentStorageForDefaultGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    props.setProperty(MultiStorageConfig.STORAGES, "cassandra,mysql");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.storage", "cassandra");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.contact_points", "localhost");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.contact_port", "7000");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.username", "cassandra");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.password", "cassandra");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiStorageConfig.STORAGES + ".mysql.contact_points", "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.username", "root");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.password", "mysql");

    props.setProperty(
        MultiStorageConfig.TABLE_MAPPING,
        "user.order:cassandra,user.customer:mysql,coordinator.state:cassandra");

    props.setProperty(
        MultiStorageConfig.NAMESPACE_MAPPING, "namespace1:cassandra,namespace2:mysql");

    props.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "dynamo"); // non-existent storage

    // Act Assert
    assertThatThrownBy(() -> new MultiStorageConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_NestedMultiStorageGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    props.setProperty(MultiStorageConfig.STORAGES, "db,mysql");
    props.setProperty(MultiStorageConfig.STORAGES + ".db.storage", "multis-torage"); // nested
    props.setProperty(MultiStorageConfig.STORAGES + ".db.contact_points", "localhost");
    props.setProperty(MultiStorageConfig.STORAGES + ".db.username", "user");
    props.setProperty(MultiStorageConfig.STORAGES + ".db.password", "pass");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiStorageConfig.STORAGES + ".mysql.contact_points", "jdbc:mysql://localhost:3306/");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.username", "root");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.password", "mysql");

    props.setProperty(
        MultiStorageConfig.TABLE_MAPPING,
        "user.order:cassandra,user.customer:mysql,coordinator.state:cassandra");

    props.setProperty(
        MultiStorageConfig.NAMESPACE_MAPPING, "namespace1:cassandra,namespace2:mysql");

    props.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "cassandra");

    // Act Assert
    assertThatThrownBy(() -> new MultiStorageConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
