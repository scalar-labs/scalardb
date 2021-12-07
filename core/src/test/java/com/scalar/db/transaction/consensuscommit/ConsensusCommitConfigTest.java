package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import org.junit.Test;

public class ConsensusCommitConfigTest {

  private static final String ANY_HOST = "localhost";

  @Test
  public void constructor_PropertiesWithOnlyContactPointsGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.getIsolation()).isEqualTo(Isolation.SNAPSHOT);
    assertThat(config.getSerializableStrategy()).isEqualTo(SerializableStrategy.EXTRA_READ);
    assertThat(config.isActiveTransactionsManagementEnabled()).isEqualTo(true);
    assertThat(config.getCoordinatorNamespace()).isNotPresent();
  }

  @Test
  public void constructor_PropertiesWithIsolationLevelGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.ISOLATION_LEVEL, Isolation.SERIALIZABLE.toString());

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.getIsolation()).isEqualTo(Isolation.SERIALIZABLE);
  }

  @Test
  public void constructor_PropertiesWithDeprecatedIsolationLevelGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty("scalar.db.isolation_level", Isolation.SERIALIZABLE.toString());

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.getIsolation()).isEqualTo(Isolation.SERIALIZABLE);
  }

  @Test
  public void constructor_UnsupportedIsolationGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.ISOLATION_LEVEL, "READ_COMMITTED");

    // Act Assert
    assertThatThrownBy(() -> new ConsensusCommitConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_PropertiesWithSerializableStrategyGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(
        ConsensusCommitConfig.SERIALIZABLE_STRATEGY, SerializableStrategy.EXTRA_WRITE.toString());

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.getSerializableStrategy()).isEqualTo(SerializableStrategy.EXTRA_WRITE);
  }

  @Test
  public void
      constructor_UnsupportedSerializableStrategyGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.SERIALIZABLE_STRATEGY, "NO_STRATEGY");

    // Act Assert
    assertThatThrownBy(() -> new ConsensusCommitConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithValidActiveTransactionsManagementEnabledGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED, "false");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.isActiveTransactionsManagementEnabled()).isEqualTo(false);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidActiveTransactionsManagementEnabledGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED, "aaa");

    // Act Assert
    assertThatThrownBy(() -> new ConsensusCommitConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_PropertiesWithCoordinatorNamespaceGiven_ShouldLoadAsDefaultValue() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE, "changed_coordinator");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.getCoordinatorNamespace()).isPresent();
    assertThat(config.getCoordinatorNamespace().get()).isEqualTo("changed_coordinator");
  }
}
