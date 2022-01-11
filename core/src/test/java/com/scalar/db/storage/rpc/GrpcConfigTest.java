package com.scalar.db.storage.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.Collections;
import java.util.Properties;
import org.junit.Test;

public class GrpcConfigTest {

  private static final String ANY_HOST = "localhost";

  @Test
  public void constructor_PropertiesWithoutPortGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.STORAGE, "grpc");

    // Act
    GrpcConfig config = new GrpcConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getDeadlineDurationMillis())
        .isEqualTo(GrpcConfig.DEFAULT_DEADLINE_DURATION_MILLIS);
    assertThat(config.isActiveTransactionsManagementEnabled()).isEqualTo(true);
  }

  @Test
  public void constructor_PropertiesWithValidDeadlineDurationMillisGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.STORAGE, "grpc");
    props.setProperty(GrpcConfig.DEADLINE_DURATION_MILLIS, "5000");

    // Act
    GrpcConfig config = new GrpcConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getDeadlineDurationMillis()).isEqualTo(5000);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidDeadlineDurationMillisGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.STORAGE, "grpc");
    props.setProperty(GrpcConfig.ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED, "aaa");

    // Act
    assertThatThrownBy(() -> new GrpcConfig(props)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithValidActiveTransactionsManagementEnabledGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.STORAGE, "grpc");
    props.setProperty(GrpcConfig.ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED, "false");

    // Act
    GrpcConfig config = new GrpcConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.isActiveTransactionsManagementEnabled()).isEqualTo(false);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidActiveTransactionsManagementEnabledGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.STORAGE, "grpc");
    props.setProperty(GrpcConfig.ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED, "aaa");

    // Act Assert
    assertThatThrownBy(() -> new GrpcConfig(props)).isInstanceOf(IllegalArgumentException.class);
  }
}
