package com.scalar.db.storage.objectstorage.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class S3ConfigTest {
  private static final String ANY_REGION = "us-west-2";
  private static final String ANY_BUCKET = "bucket";
  private static final String ANY_CONTACT_POINT = ANY_REGION + "/" + ANY_BUCKET;
  private static final String S3_STORAGE = "s3";

  @Test
  public void constructor_AllPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);
    props.setProperty(DatabaseConfig.STORAGE, S3_STORAGE);

    // Act
    S3Config config = new S3Config(new DatabaseConfig(props));

    // Assert
    assertThat(config.getRegion()).isEqualTo(ANY_REGION);
    assertThat(config.getBucket()).isEqualTo(ANY_BUCKET);
  }

  @Test
  public void constructor_PropertiesWithoutNonMandatoryOptionsGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);
    props.setProperty(DatabaseConfig.STORAGE, S3_STORAGE);

    // Act
    S3Config config = new S3Config(new DatabaseConfig(props));

    // Assert
    assertThat(config.getRegion()).isEqualTo(ANY_REGION);
    assertThat(config.getBucket()).isEqualTo(ANY_BUCKET);
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
