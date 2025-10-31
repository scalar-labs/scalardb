package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.objectstorage.blob.BlobConfig;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class BlobConfigTest {
  private static final String ANY_USERNAME = "any_user";
  private static final String ANY_PASSWORD = "any_password";
  private static final String ANY_BUCKET = "bucket";
  private static final String ANY_ENDPOINT = "http://localhost:10000/" + ANY_USERNAME;
  private static final String ANY_CONTACT_POINT = ANY_ENDPOINT + "/" + ANY_BUCKET;
  private static final String BLOB_STORAGE = "blob";
  private static final String ANY_TABLE_METADATA_NAMESPACE = "any_namespace";
  private static final String ANY_PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES = "5242880"; // 5MB
  private static final String ANY_PARALLEL_UPLOAD_MAX_PARALLELISM = "4";
  private static final String ANY_PARALLEL_UPLOAD_THRESHOLD_IN_BYTES = "10485760"; // 10MB
  private static final String ANY_REQUEST_TIMEOUT_IN_SECONDS = "30";

  @Test
  public void constructor_AllPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, BLOB_STORAGE);
    props.setProperty(DatabaseConfig.SYSTEM_NAMESPACE_NAME, ANY_TABLE_METADATA_NAMESPACE);
    props.setProperty(
        BlobConfig.PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES, ANY_PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES);
    props.setProperty(
        BlobConfig.PARALLEL_UPLOAD_MAX_PARALLELISM, ANY_PARALLEL_UPLOAD_MAX_PARALLELISM);
    props.setProperty(
        BlobConfig.PARALLEL_UPLOAD_THRESHOLD_IN_BYTES, ANY_PARALLEL_UPLOAD_THRESHOLD_IN_BYTES);
    props.setProperty(BlobConfig.REQUEST_TIMEOUT_IN_SECONDS, ANY_REQUEST_TIMEOUT_IN_SECONDS);

    // Act
    BlobConfig config = new BlobConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getEndpoint()).isEqualTo(ANY_ENDPOINT);
    assertThat(config.getUsername()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getBucket()).isEqualTo(ANY_BUCKET);
    assertThat(config.getMetadataNamespace()).isEqualTo(ANY_TABLE_METADATA_NAMESPACE);
    assertThat(config.getParallelUploadBlockSizeInBytes())
        .isEqualTo(Long.parseLong(ANY_PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES));
    assertThat(config.getParallelUploadMaxParallelism())
        .isEqualTo(Integer.parseInt(ANY_PARALLEL_UPLOAD_MAX_PARALLELISM));
    assertThat(config.getParallelUploadThresholdInBytes())
        .isEqualTo(Long.parseLong(ANY_PARALLEL_UPLOAD_THRESHOLD_IN_BYTES));
    assertThat(config.getRequestTimeoutInSeconds())
        .isEqualTo(Integer.parseInt(ANY_REQUEST_TIMEOUT_IN_SECONDS));
  }

  @Test
  public void constructor_PropertiesWithoutOptimizationOptionsGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, BLOB_STORAGE);
    props.setProperty(DatabaseConfig.SYSTEM_NAMESPACE_NAME, ANY_TABLE_METADATA_NAMESPACE);

    // Act
    BlobConfig config = new BlobConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getEndpoint()).isEqualTo(ANY_ENDPOINT);
    assertThat(config.getUsername()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getBucket()).isEqualTo(ANY_BUCKET);
    assertThat(config.getMetadataNamespace()).isEqualTo(ANY_TABLE_METADATA_NAMESPACE);
    assertThat(config.getParallelUploadBlockSizeInBytes())
        .isEqualTo(BlobConfig.DEFAULT_PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES);
    assertThat(config.getParallelUploadMaxParallelism())
        .isEqualTo(BlobConfig.DEFAULT_PARALLEL_UPLOAD_MAX_PARALLELISM);
    assertThat(config.getParallelUploadThresholdInBytes())
        .isEqualTo(BlobConfig.DEFAULT_PARALLEL_UPLOAD_THRESHOLD_IN_BYTES);
    assertThat(config.getRequestTimeoutInSeconds())
        .isEqualTo(BlobConfig.DEFAULT_REQUEST_TIMEOUT_IN_SECONDS);
  }

  @Test
  public void constructor_WithoutStorage_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);

    // Act Assert
    assertThatThrownBy(() -> new BlobConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_WithoutSystemNamespaceName_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, BLOB_STORAGE);

    // Act
    BlobConfig config = new BlobConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getEndpoint()).isEqualTo(ANY_ENDPOINT);
    assertThat(config.getUsername()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getBucket()).isEqualTo(ANY_BUCKET);
    assertThat(config.getMetadataNamespace())
        .isEqualTo(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    assertThat(config.getParallelUploadBlockSizeInBytes())
        .isEqualTo(BlobConfig.DEFAULT_PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES);
    assertThat(config.getParallelUploadMaxParallelism())
        .isEqualTo(BlobConfig.DEFAULT_PARALLEL_UPLOAD_MAX_PARALLELISM);
    assertThat(config.getParallelUploadThresholdInBytes())
        .isEqualTo(BlobConfig.DEFAULT_PARALLEL_UPLOAD_THRESHOLD_IN_BYTES);
    assertThat(config.getRequestTimeoutInSeconds())
        .isEqualTo(BlobConfig.DEFAULT_REQUEST_TIMEOUT_IN_SECONDS);
  }

  @Test
  public void
      constructor_PropertiesWithEmptyContactPointsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, BLOB_STORAGE);

    // Act
    assertThatThrownBy(() -> new BlobConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
