package com.scalar.db.storage.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class GrpcConfigTest {

  private static final String ANY_HOST = "localhost";
  private static final int ANY_PORT = 60000;

  @Test
  public void constructor_PropertiesWithPortGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.CONTACT_PORT, Integer.toString(ANY_PORT));
    props.setProperty(DatabaseConfig.STORAGE, "grpc");

    // Act
    GrpcConfig config = new GrpcConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getHost()).isEqualTo(ANY_HOST);
    assertThat(config.getPort()).isEqualTo(ANY_PORT);
    assertThat(config.getDeadlineDurationMillis())
        .isEqualTo(GrpcConfig.DEFAULT_DEADLINE_DURATION_MILLIS);
    assertThat(config.getMaxInboundMessageSize()).isNotPresent();
    assertThat(config.getMaxInboundMetadataSize()).isNotPresent();
  }

  @Test
  public void constructor_PropertiesWithoutPortGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.STORAGE, "grpc");

    // Act
    GrpcConfig config = new GrpcConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getHost()).isEqualTo(ANY_HOST);
    assertThat(config.getPort()).isEqualTo(GrpcConfig.DEFAULT_SERVER_PORT);
    assertThat(config.getDeadlineDurationMillis())
        .isEqualTo(GrpcConfig.DEFAULT_DEADLINE_DURATION_MILLIS);
  }

  @Test
  public void constructor_PropertiesWithValidDeadlineDurationMillisGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.STORAGE, "grpc");
    props.setProperty(GrpcConfig.DEADLINE_DURATION_MILLIS, "5000");

    // Act
    GrpcConfig config = new GrpcConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getHost()).isEqualTo(ANY_HOST);
    assertThat(config.getDeadlineDurationMillis()).isEqualTo(5000);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidDeadlineDurationMillisGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.STORAGE, "grpc");
    props.setProperty(GrpcConfig.DEADLINE_DURATION_MILLIS, "aaa");

    // Act
    assertThatThrownBy(() -> new GrpcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithEmptyContactPointsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "grpc");

    // Act
    assertThatThrownBy(() -> new GrpcConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithMaxInboundMessageSizeAndMaxInboundMetadataSizeGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.STORAGE, "grpc");
    props.setProperty(GrpcConfig.MAX_INBOUND_MESSAGE_SIZE, "1000");
    props.setProperty(GrpcConfig.MAX_INBOUND_METADATA_SIZE, "2000");

    // Act
    GrpcConfig config = new GrpcConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getMaxInboundMessageSize()).isPresent();
    assertThat(config.getMaxInboundMessageSize().get()).isEqualTo(1000);
    assertThat(config.getMaxInboundMetadataSize()).isPresent();
    assertThat(config.getMaxInboundMetadataSize().get()).isEqualTo(2000);
  }
}
