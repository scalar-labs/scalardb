package com.scalar.db.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;
import org.junit.Test;

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
    assertThat(config.getGateKeeperClass()).isEqualTo(LockFreeGateKeeper.class);
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
  public void constructor_ValidGateKeeperTypeGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ServerConfig.GATE_KEEPER_TYPE, "synchronized");

    // Act
    ServerConfig config = new ServerConfig(props);

    // Assert
    assertThat(config.getGateKeeperClass()).isEqualTo(SynchronizedGateKeeper.class);
  }

  @Test
  public void constructor_InvalidGateKeeperTypeGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ServerConfig.GATE_KEEPER_TYPE, "aaa");

    // Act Assert
    assertThatThrownBy(() -> new ServerConfig(props)).isInstanceOf(IllegalArgumentException.class);
  }
}
