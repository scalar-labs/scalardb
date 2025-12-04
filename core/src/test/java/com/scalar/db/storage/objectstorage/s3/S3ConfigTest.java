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
  private static final String ANY_MULTIPART_UPLOAD_PART_SIZE_BYTES = "5242880"; // 5MB
  private static final String ANY_MULTIPART_UPLOAD_MAX_CONCURRENCY = "4";
  private static final String ANY_MULTIPART_UPLOAD_THRESHOLD_SIZE_BYTES = "10485760"; // 10MB
  private static final String ANY_REQUEST_TIMEOUT_SECS = "30";

  @Test
  public void constructor_AllPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, S3_STORAGE);
    props.setProperty(DatabaseConfig.SYSTEM_NAMESPACE_NAME, ANY_TABLE_METADATA_NAMESPACE);
    props.setProperty(
        S3Config.MULTIPART_UPLOAD_PART_SIZE_BYTES, ANY_MULTIPART_UPLOAD_PART_SIZE_BYTES);
    props.setProperty(
        S3Config.MULTIPART_UPLOAD_MAX_CONCURRENCY, ANY_MULTIPART_UPLOAD_MAX_CONCURRENCY);
    props.setProperty(
        S3Config.MULTIPART_UPLOAD_THRESHOLD_SIZE_BYTES, ANY_MULTIPART_UPLOAD_THRESHOLD_SIZE_BYTES);
    props.setProperty(S3Config.REQUEST_TIMEOUT_SECS, ANY_REQUEST_TIMEOUT_SECS);

    // Act
    S3Config config = new S3Config(new DatabaseConfig(props));

    // Assert
    assertThat(config.getRegion()).isEqualTo(ANY_REGION);
    assertThat(config.getBucket()).isEqualTo(ANY_BUCKET);
    assertThat(config.getUsername()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getMetadataNamespace()).isEqualTo(ANY_TABLE_METADATA_NAMESPACE);
    assertThat(config.getMultipartUploadPartSizeBytes()).isNotEmpty();
    assertThat(config.getMultipartUploadPartSizeBytes().get())
        .isEqualTo(Long.parseLong(ANY_MULTIPART_UPLOAD_PART_SIZE_BYTES));
    assertThat(config.getMultipartUploadMaxConcurrency()).isNotEmpty();
    assertThat(config.getMultipartUploadMaxConcurrency().get())
        .isEqualTo(Integer.parseInt(ANY_MULTIPART_UPLOAD_MAX_CONCURRENCY));
    assertThat(config.getMultipartUploadThresholdSizeBytes()).isNotEmpty();
    assertThat(config.getMultipartUploadThresholdSizeBytes().get())
        .isEqualTo(Long.parseLong(ANY_MULTIPART_UPLOAD_THRESHOLD_SIZE_BYTES));
    assertThat(config.getRequestTimeoutSecs()).isNotEmpty();
    assertThat(config.getRequestTimeoutSecs().get())
        .isEqualTo(Integer.parseInt(ANY_REQUEST_TIMEOUT_SECS));
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
    assertThat(config.getMultipartUploadPartSizeBytes()).isEmpty();
    assertThat(config.getMultipartUploadMaxConcurrency()).isEmpty();
    assertThat(config.getMultipartUploadThresholdSizeBytes()).isEmpty();
    assertThat(config.getRequestTimeoutSecs()).isEmpty();
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
