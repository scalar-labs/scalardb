package com.scalar.db.storage.objectstorage.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class S3ConfigTest {
  private static final String ANY_USERNAME = "any_user";
  private static final String ANY_PASSWORD = "any_password";
  private static final String ANY_REGION = "us-west-2";
  private static final String ANY_BUCKET = "bucket";
  private static final String ANY_CONTACT_POINT = ANY_REGION + "/" + ANY_BUCKET;
  private static final String S3_STORAGE = "s3";
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
    props.setProperty(DatabaseConfig.STORAGE, S3_STORAGE);
    props.setProperty(S3Config.TABLE_METADATA_NAMESPACE, ANY_TABLE_METADATA_NAMESPACE);
    props.setProperty(
        S3Config.PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES, ANY_PARALLEL_UPLOAD_BLOCK_SIZE_IN_BYTES);
    props.setProperty(
        S3Config.PARALLEL_UPLOAD_MAX_PARALLELISM, ANY_PARALLEL_UPLOAD_MAX_PARALLELISM);
    props.setProperty(
        S3Config.PARALLEL_UPLOAD_THRESHOLD_IN_BYTES, ANY_PARALLEL_UPLOAD_THRESHOLD_IN_BYTES);
    props.setProperty(S3Config.REQUEST_TIMEOUT_IN_SECONDS, ANY_REQUEST_TIMEOUT_IN_SECONDS);

    // Act
    S3Config config = new S3Config(new DatabaseConfig(props));

    // Assert
    assertThat(config.getRegion()).isEqualTo(ANY_REGION);
    assertThat(config.getBucket()).isEqualTo(ANY_BUCKET);
    assertThat(config.getUsername()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getMetadataNamespace()).isEqualTo(ANY_TABLE_METADATA_NAMESPACE);
    assertThat(config.getParallelUploadBlockSizeInBytes()).isNotEmpty();
    assertThat(config.getParallelUploadBlockSizeInBytes().get()).isEqualTo(5242880);
    assertThat(config.getParallelUploadMaxParallelism()).isNotEmpty();
    assertThat(config.getParallelUploadMaxParallelism().get()).isEqualTo(4);
    assertThat(config.getParallelUploadThresholdInBytes()).isNotEmpty();
    assertThat(config.getParallelUploadThresholdInBytes().get()).isEqualTo(10485760);
    assertThat(config.getRequestTimeoutInSeconds()).isNotEmpty();
    assertThat(config.getRequestTimeoutInSeconds().get()).isEqualTo(30);
  }

  @Test
  public void constructor_PropertiesWithoutNonMandatoryOptionsGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);
    props.setProperty(DatabaseConfig.STORAGE, S3_STORAGE);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);

    // Act
    S3Config config = new S3Config(new DatabaseConfig(props));

    // Assert
    assertThat(config.getRegion()).isEqualTo(ANY_REGION);
    assertThat(config.getBucket()).isEqualTo(ANY_BUCKET);
    assertThat(config.getUsername()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getMetadataNamespace())
        .isEqualTo(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    assertThat(config.getParallelUploadBlockSizeInBytes()).isEmpty();
    assertThat(config.getParallelUploadMaxParallelism()).isEmpty();
    assertThat(config.getParallelUploadThresholdInBytes()).isEmpty();
    assertThat(config.getRequestTimeoutInSeconds()).isEmpty();
  }

  @Test
  public void constructor_WithoutStorage_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);

    // Act Assert
    assertThatThrownBy(() -> new S3Config(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithEmptyContactPointsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, "");
    props.setProperty(DatabaseConfig.STORAGE, S3_STORAGE);

    // Act Assert
    assertThatThrownBy(() -> new S3Config(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
