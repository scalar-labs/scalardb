package com.scalar.db.storage.objectstorage.cloudstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class CloudStorageConfigTest {
  private static final String ANY_PROJECT_ID = "any_project_id";
  private static final String ANY_PASSWORD = "any_password";
  private static final String ANY_BUCKET = "bucket";
  private static final String ANY_CONTACT_POINT = ANY_BUCKET;
  private static final String CloudStorage_STORAGE = "cloud-storage";
  private static final String ANY_TABLE_METADATA_NAMESPACE = "any_namespace";
  private static final String ANY_PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES = "5242880"; // 5MB

  @Test
  public void constructor_AllPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, ANY_PROJECT_ID);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, CloudStorage_STORAGE);
    props.setProperty(DatabaseConfig.SYSTEM_NAMESPACE_NAME, ANY_TABLE_METADATA_NAMESPACE);
    props.setProperty(
        CloudStorageConfig.PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES,
        ANY_PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES);

    // Act
    CloudStorageConfig config = new CloudStorageConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getProjectId()).isEqualTo(ANY_PROJECT_ID);
    assertThat(config.getBucket()).isEqualTo(ANY_BUCKET);
    assertThat(config.getMetadataNamespace()).isEqualTo(ANY_TABLE_METADATA_NAMESPACE);
    assertThat(config.getParallelUploadBlockSizeInBytes()).isNotEmpty();
    assertThat(config.getParallelUploadBlockSizeInBytes().get()).isEqualTo(5242880);
  }

  @Test
  public void constructor_PropertiesWithoutNonMandatoryOptionsGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, ANY_PROJECT_ID);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, CloudStorage_STORAGE);

    // Act
    CloudStorageConfig config = new CloudStorageConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getProjectId()).isEqualTo(ANY_PROJECT_ID);
    assertThat(config.getBucket()).isEqualTo(ANY_BUCKET);
    assertThat(config.getMetadataNamespace())
        .isEqualTo(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    assertThat(config.getParallelUploadBlockSizeInBytes()).isEmpty();
  }

  @Test
  public void constructor_WithoutStorage_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);

    // Act Assert
    assertThatThrownBy(() -> new CloudStorageConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithEmptyContactPointsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, "");
    props.setProperty(DatabaseConfig.STORAGE, CloudStorage_STORAGE);

    // Act Assert
    assertThatThrownBy(() -> new CloudStorageConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
