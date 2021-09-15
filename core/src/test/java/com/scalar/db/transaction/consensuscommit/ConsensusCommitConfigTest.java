package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.Collections;
import java.util.Properties;
import org.junit.Test;

public class ConsensusCommitConfigTest {

  private static final String ANY_HOST = "localhost";

  @Test
  public void constructor_PropertiesWithoutPortGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getSerializableStrategy()).isEqualTo(SerializableStrategy.EXTRA_READ);
    assertThat(config.isActiveTransactionsManagementEnabled()).isEqualTo(true);
  }

  @Test
  public void constructor_PropertiesWithSerializableStrategyGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(
        ConsensusCommitConfig.SERIALIZABLE_STRATEGY, SerializableStrategy.EXTRA_WRITE.toString());

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getSerializableStrategy()).isEqualTo(SerializableStrategy.EXTRA_WRITE);
  }

  @Test
  public void
      constructor_UnsupportedSerializableStrategyGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
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
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(ConsensusCommitConfig.ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED, "true");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.isActiveTransactionsManagementEnabled()).isEqualTo(true);
  }

  @Test
  public void
      constructor_PropertiesWithInvalidActiveTransactionsManagementEnabledGiven_ShouldLoadAsFalse() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(ConsensusCommitConfig.ACTIVE_TRANSACTIONS_MANAGEMENT_ENABLED, "aaa");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.isActiveTransactionsManagementEnabled()).isEqualTo(false);
  }
}
