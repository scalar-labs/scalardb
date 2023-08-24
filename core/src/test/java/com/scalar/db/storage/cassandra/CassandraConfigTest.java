package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

public class CassandraConfigTest {
  private static final String ANY_METADATA_NAMESPACE = "any_namespace";

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void constructor_MetadataNamespaceGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(CassandraConfig.METADATA_KEYSPACE, ANY_METADATA_NAMESPACE);

    // Act
    CassandraConfig config = new CassandraConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getMetadataKeyspace()).isPresent();
    assertThat(config.getMetadataKeyspace().get()).isEqualTo(ANY_METADATA_NAMESPACE);
  }

  @Test
  public void constructor_WithNoPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();

    // Act
    CassandraConfig config = new CassandraConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getMetadataKeyspace()).isEmpty();
  }
}
