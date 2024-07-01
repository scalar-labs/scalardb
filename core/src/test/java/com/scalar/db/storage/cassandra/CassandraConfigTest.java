package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.config.DatabaseConfig;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class CassandraConfigTest {

  private static final String ANY_SYSTEM_NAMESPACE = "any_namespace";

  @Test
  public void constructor_SystemNamespaceGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(CassandraConfig.SYSTEM_NAMESPACE_NAME, ANY_SYSTEM_NAMESPACE);

    // Act
    CassandraConfig config = new CassandraConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getSystemNamespaceName()).isEqualTo(Optional.of(ANY_SYSTEM_NAMESPACE));
  }

  @Test
  public void constructor_NoPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();

    // Act
    CassandraConfig config = new CassandraConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getSystemNamespaceName()).isEmpty();
  }
}
