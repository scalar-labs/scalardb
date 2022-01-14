package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import org.junit.Test;

public class ConsensusCommitConfigTest {

  @Test
  public void constructor_NoPropertiesGiven_ShouldLoadAsDefaultValues() {
    // Arrange
    Properties props = new Properties();

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.getIsolation()).isEqualTo(Isolation.SNAPSHOT);
    assertThat(config.getSerializableStrategy()).isEqualTo(SerializableStrategy.EXTRA_READ);
    assertThat(config.isActiveTransactionsManagementEnabled()).isEqualTo(true);
    assertThat(config.getCoordinatorNamespace()).isNotPresent();
    assertThat(config.getParallelExecutorCount())
        .isEqualTo(ConsensusCommitConfig.DEFAULT_PARALLEL_EXECUTOR_COUNT);
    assertThat(config.isParallelPreparationEnabled()).isEqualTo(false);
    assertThat(config.isParallelValidationEnabled()).isEqualTo(false);
    assertThat(config.isParallelCommitEnabled()).isEqualTo(false);
    assertThat(config.isParallelRollbackEnabled()).isEqualTo(false);
    assertThat(config.isAsyncCommitEnabled()).isEqualTo(false);
    assertThat(config.isAsyncCommitEnabled()).isEqualTo(false);
    assertThat(config.getTableMetadataCacheExpirationTimeSecs()).isEqualTo(-1);
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
  public void constructor_PropertiesWithCoordinatorNamespaceGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE, "changed_coordinator");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.getCoordinatorNamespace()).isPresent();
    assertThat(config.getCoordinatorNamespace().get()).isEqualTo("changed_coordinator");
  }

  @Test
  public void constructor_ParallelExecutionRelatedPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.PARALLEL_EXECUTOR_COUNT, "100");
    props.setProperty(ConsensusCommitConfig.PARALLEL_PREPARATION_ENABLED, "true");
    props.setProperty(ConsensusCommitConfig.PARALLEL_VALIDATION_ENABLED, "true");
    props.setProperty(ConsensusCommitConfig.PARALLEL_COMMIT_ENABLED, "true");
    props.setProperty(ConsensusCommitConfig.PARALLEL_ROLLBACK_ENABLED, "true");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.getParallelExecutorCount()).isEqualTo(100);
    assertThat(config.isParallelPreparationEnabled()).isEqualTo(true);
    assertThat(config.isParallelValidationEnabled()).isEqualTo(true);
    assertThat(config.isParallelCommitEnabled()).isEqualTo(true);
    assertThat(config.isParallelRollbackEnabled()).isEqualTo(true);
  }

  @Test
  public void
      constructor_ParallelExecutionRelatedPropertiesWithoutParallelValidationAndParallelRollbackPropertyGiven_ShouldUseParallelCommitValueForParallelValidationAndParallelRollback() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.PARALLEL_EXECUTOR_COUNT, "100");
    props.setProperty(ConsensusCommitConfig.PARALLEL_PREPARATION_ENABLED, "false");
    props.setProperty(ConsensusCommitConfig.PARALLEL_COMMIT_ENABLED, "true");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.getParallelExecutorCount()).isEqualTo(100);
    assertThat(config.isParallelPreparationEnabled()).isEqualTo(false);
    assertThat(config.isParallelValidationEnabled())
        .isEqualTo(true); // use the parallel commit value
    assertThat(config.isParallelCommitEnabled()).isEqualTo(true);
    assertThat(config.isParallelRollbackEnabled()).isEqualTo(true); // use the parallel commit value
  }

  @Test
  public void constructor_AsyncExecutionRelatedPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.ASYNC_COMMIT_ENABLED, "true");
    props.setProperty(ConsensusCommitConfig.ASYNC_ROLLBACK_ENABLED, "true");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.isAsyncCommitEnabled()).isEqualTo(true);
    assertThat(config.isAsyncRollbackEnabled()).isEqualTo(true);
  }

  @Test
  public void
      constructor_AsyncExecutionRelatedPropertiesWithoutAsyncRollbackPropertyGiven_ShouldUseAsyncCommitValueForAsyncRollback() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.ASYNC_COMMIT_ENABLED, "true");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.isAsyncCommitEnabled()).isEqualTo(true);
    assertThat(config.isAsyncRollbackEnabled()).isEqualTo(true); // use the async commit value
  }

  @Test
  public void constructor_TableMetadataCacheExpirationTimeGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.TABLE_METADATA_CACHE_EXPIRATION_TIME_SECS, "3600");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(props);

    // Assert
    assertThat(config.getTableMetadataCacheExpirationTimeSecs()).isEqualTo(3600);
  }

  @Test
  public void
      constructor_InvalidTableMetadataCacheExpirationTimeGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.TABLE_METADATA_CACHE_EXPIRATION_TIME_SECS, "aaa");

    // Act Assert
    assertThatThrownBy(() -> new ConsensusCommitConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
