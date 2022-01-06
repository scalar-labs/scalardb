package com.scalar.db.graphql.server.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Properties;
import org.junit.Test;

public class ServerConfigTest {
  private static final int ANY_PORT = 9999;
  private static final String ANY_PATH = "/zzz";
  private static final String ANY_NAMESPACES = "n1, n2, n3";

  @Test
  public void constructor_NothingGiven_ShouldUseDefault() {
    // Arrange
    Properties props = new Properties();

    // Act
    ServerConfig config = new ServerConfig(props);

    // Assert
    assertThat(config.getPort()).isEqualTo(ServerConfig.DEFAULT_PORT);
    assertThat(config.getPath()).isEqualTo(ServerConfig.DEFAULT_PATH);
    assertThat(config.getNamespaces()).isEmpty();
    assertThat(config.getGraphiql()).isEqualTo(ServerConfig.DEFAULT_GRAPHIQL);
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
  public void constructor_ValidPathGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ServerConfig.PATH, ANY_PATH);

    // Act
    ServerConfig config = new ServerConfig(props);

    // Assert
    assertThat(config.getPath()).isEqualTo(ANY_PATH);
  }

  @Test
  public void constructor_ValidNamespacesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ServerConfig.NAMESPACES, ANY_NAMESPACES);

    // Act
    ServerConfig config = new ServerConfig(props);

    // Assert
    assertThat(config.getNamespaces())
        .isEqualTo(new LinkedHashSet<>(Arrays.asList(ANY_NAMESPACES.split("\\s*,\\s*"))));
  }

  @Test
  public void constructor_ValidGraphiqlGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ServerConfig.GRAPHIQL, Boolean.toString(false));

    // Act
    ServerConfig config = new ServerConfig(props);

    // Assert
    assertThat(config.getGraphiql()).isFalse();
  }

  @Test
  public void constructor_InvalidGraphiqlGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ServerConfig.GRAPHIQL, "abc");

    // Act Assert
    assertThatThrownBy(() -> new ServerConfig(props)).isInstanceOf(IllegalArgumentException.class);
  }
}
