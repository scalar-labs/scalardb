package com.scalar.db.storage.objectstorage.blobstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class BlobStorageConfigTest {
  private static final String ANY_USERNAME = "any_user";
  private static final String ANY_PASSWORD = "any_password";
  private static final String ANY_BUCKET = "bucket";
  private static final String ANY_ENDPOINT = "http://localhost:10000/" + ANY_USERNAME;
  private static final String ANY_CONTACT_POINT = ANY_ENDPOINT + "/" + ANY_BUCKET;
  private static final String BLOB_STORAGE = "blob-storage";
  private static final String ANY_TABLE_METADATA_NAMESPACE = "any_namespace";
  private static final String ANY_PARALLEL_UPLOAD_BLOCK_SIZE_BYTES = "5242880"; // 5MB
  private static final String ANY_PARALLEL_UPLOAD_MAX_CONCURRENCY = "4";
  private static final String ANY_PARALLEL_UPLOAD_THRESHOLD_SIZE_BYTES = "10485760"; // 10MB
  private static final String ANY_REQUEST_TIMEOUT_SECS = "30";

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
        BlobStorageConfig.PARALLEL_UPLOAD_BLOCK_SIZE_BYTES, ANY_PARALLEL_UPLOAD_BLOCK_SIZE_BYTES);
    props.setProperty(
        BlobStorageConfig.PARALLEL_UPLOAD_MAX_CONCURRENCY, ANY_PARALLEL_UPLOAD_MAX_CONCURRENCY);
    props.setProperty(
        BlobStorageConfig.PARALLEL_UPLOAD_THRESHOLD_SIZE_BYTES,
        ANY_PARALLEL_UPLOAD_THRESHOLD_SIZE_BYTES);
    props.setProperty(BlobStorageConfig.REQUEST_TIMEOUT_SECS, ANY_REQUEST_TIMEOUT_SECS);

    // Act
    BlobStorageConfig config = new BlobStorageConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getEndpoint()).isEqualTo(ANY_ENDPOINT);
    assertThat(config.getUsername()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getBucket()).isEqualTo(ANY_BUCKET);
    assertThat(config.getMetadataNamespace()).isEqualTo(ANY_TABLE_METADATA_NAMESPACE);
    assertThat(config.getParallelUploadBlockSizeBytes()).isNotEmpty();
    assertThat(config.getParallelUploadBlockSizeBytes().get()).isEqualTo(5242880);
    assertThat(config.getParallelUploadMaxConcurrency()).isNotEmpty();
    assertThat(config.getParallelUploadMaxConcurrency().get()).isEqualTo(4);
    assertThat(config.getParallelUploadThresholdSizeBytes()).isNotEmpty();
    assertThat(config.getParallelUploadThresholdSizeBytes().get()).isEqualTo(10485760);
    assertThat(config.getRequestTimeoutSecs()).isNotEmpty();
    assertThat(config.getRequestTimeoutSecs().get()).isEqualTo(30);
  }

  @Test
  public void constructor_PropertiesWithoutNonMandatoryOptionsGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, BLOB_STORAGE);

    // Act
    BlobStorageConfig config = new BlobStorageConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getEndpoint()).isEqualTo(ANY_ENDPOINT);
    assertThat(config.getUsername()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getBucket()).isEqualTo(ANY_BUCKET);
    assertThat(config.getMetadataNamespace())
        .isEqualTo(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    assertThat(config.getParallelUploadBlockSizeBytes()).isEmpty();
    assertThat(config.getParallelUploadMaxConcurrency()).isEmpty();
    assertThat(config.getParallelUploadThresholdSizeBytes()).isEmpty();
    assertThat(config.getRequestTimeoutSecs()).isEmpty();
  }

  @Test
  public void constructor_WithoutStorage_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);

    // Act Assert
    assertThatThrownBy(() -> new BlobStorageConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithEmptyContactPointsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, BLOB_STORAGE);

    // Act
    assertThatThrownBy(() -> new BlobStorageConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
