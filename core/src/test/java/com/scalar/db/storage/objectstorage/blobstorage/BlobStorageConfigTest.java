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
    props.setProperty(BlobStorageConfig.TABLE_METADATA_NAMESPACE, ANY_TABLE_METADATA_NAMESPACE);
    props.setProperty(
        BlobStorageConfig.PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES,
        ANY_PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES);
    props.setProperty(
        BlobStorageConfig.PARALLEL_UPLOAD_MAX_PARALLELISM, ANY_PARALLEL_UPLOAD_MAX_PARALLELISM);
    props.setProperty(
        BlobStorageConfig.PARALLEL_UPLOAD_THRESHOLD_IN_BYTES,
        ANY_PARALLEL_UPLOAD_THRESHOLD_IN_BYTES);
    props.setProperty(BlobStorageConfig.REQUEST_TIMEOUT_IN_SECONDS, ANY_REQUEST_TIMEOUT_IN_SECONDS);

    // Act
    BlobStorageConfig config = new BlobStorageConfig(new DatabaseConfig(props));

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
    assertThat(config.getParallelUploadBlockSizeInBytes())
        .isEqualTo(BlobStorageConfig.DEFAULT_PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES);
    assertThat(config.getParallelUploadMaxParallelism())
        .isEqualTo(BlobStorageConfig.DEFAULT_PARALLEL_UPLOAD_MAX_PARALLELISM);
    assertThat(config.getParallelUploadThresholdInBytes())
        .isEqualTo(BlobStorageConfig.DEFAULT_PARALLEL_UPLOAD_THRESHOLD_IN_BYTES);
    assertThat(config.getRequestTimeoutInSeconds())
        .isEqualTo(BlobStorageConfig.DEFAULT_REQUEST_TIMEOUT_IN_SECONDS);
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
