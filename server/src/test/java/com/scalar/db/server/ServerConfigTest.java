package com.scalar.db.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;
import org.junit.jupiter.api.Test;

public class ServerConfigTest {

  private static final int ANY_PORT = 9999;

  @Test
  public void constructor_NothingGiven_ShouldUseDefault() {
    // Arrange
    Properties props = new Properties();

    // Act
    ServerConfig config = new ServerConfig(props);

    // Assert
    assertThat(config.getPort()).isEqualTo(ServerConfig.DEFAULT_PORT);
    assertThat(config.getPrometheusExporterPort())
        .isEqualTo(ServerConfig.DEFAULT_PROMETHEUS_EXPORTER_PORT);
    assertThat(config.getGrpcMaxInboundMessageSize()).isNotPresent();
    assertThat(config.getGrpcMaxInboundMetadataSize()).isNotPresent();
    assertThat(config.getDecommissioningDurationSecs()).isEqualTo(30);
  }

  @Test
  public void constructor_ValidPortGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ServerConfig.PORT, Integer.toString(ANY_PORT));

    // Act
    ServerConfig config = new ServerConfig(props);

    // Assert
    assertThat(config.getPort()).isEqualTo(ANY_PORT);
  }

  @Test
  public void constructor_InvalidPortGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ServerConfig.PORT, "abc");

    // Act Assert
    assertThatThrownBy(() -> new ServerConfig(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_ValidPrometheusExporterPortGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ServerConfig.PROMETHEUS_EXPORTER_PORT, Integer.toString(ANY_PORT));

    // Act
    ServerConfig config = new ServerConfig(props);

    // Assert
    assertThat(config.getPrometheusExporterPort()).isEqualTo(ANY_PORT);
  }

  @Test
  public void constructor_InvalidPrometheusExporterPortGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ServerConfig.PROMETHEUS_EXPORTER_PORT, "abc");

    // Act Assert
    assertThatThrownBy(() -> new ServerConfig(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithMaxInboundMessageSizeAndMaxInboundMetadataSizeGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ServerConfig.GRPC_MAX_INBOUND_MESSAGE_SIZE, "1000");
    props.setProperty(ServerConfig.GRPC_MAX_INBOUND_METADATA_SIZE, "2000");

    // Act
    ServerConfig config = new ServerConfig(props);

    // Assert
    assertThat(config.getGrpcMaxInboundMessageSize()).isPresent();
    assertThat(config.getGrpcMaxInboundMessageSize().get()).isEqualTo(1000);
    assertThat(config.getGrpcMaxInboundMetadataSize()).isPresent();
    assertThat(config.getGrpcMaxInboundMetadataSize().get()).isEqualTo(2000);
  }

  @Test
  public void constructor_DecommissioningDurationSecsGiven_ShouldLoadProperly() {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(ServerConfig.DECOMMISSIONING_DURATION_SECS, "60");

    // Act
    ServerConfig config = new ServerConfig(properties);

    // Assert
    assertThat(config.getDecommissioningDurationSecs()).isEqualTo(60);
  }
}
