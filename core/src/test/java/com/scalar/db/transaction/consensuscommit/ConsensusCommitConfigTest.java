package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class ConsensusCommitConfigTest {

  @Test
  public void constructor_NoPropertiesGiven_ShouldLoadAsDefaultValues() {
    // Arrange
    Properties props = new Properties();

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getIsolation()).isEqualTo(Isolation.SNAPSHOT);
    assertThat(config.getCoordinatorNamespace()).isNotPresent();
    assertThat(config.getParallelExecutorCount()).isEqualTo(128);
    assertThat(config.isParallelPreparationEnabled()).isTrue();
    assertThat(config.isParallelValidationEnabled()).isTrue();
    assertThat(config.isParallelCommitEnabled()).isTrue();
    assertThat(config.isParallelRollbackEnabled()).isTrue();
    assertThat(config.isAsyncCommitEnabled()).isFalse();
    assertThat(config.isAsyncRollbackEnabled()).isFalse();
    assertThat(config.isCoordinatorWriteOmissionOnReadOnlyEnabled()).isTrue();
    assertThat(config.isOnePhaseCommitEnabled()).isFalse();
    assertThat(config.isParallelImplicitPreReadEnabled()).isTrue();
    assertThat(config.isIncludeMetadataEnabled()).isFalse();
  }

  @Test
  public void constructor_PropertiesWithIsolationLevelGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.ISOLATION_LEVEL, Isolation.SERIALIZABLE.toString());

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getIsolation()).isEqualTo(Isolation.SERIALIZABLE);
  }

  @Test
  public void constructor_PropertiesWithDeprecatedIsolationLevelGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty("scalar.db.isolation_level", Isolation.SERIALIZABLE.toString());

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getIsolation()).isEqualTo(Isolation.SERIALIZABLE);
  }

  @Test
  public void constructor_UnsupportedIsolationGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.ISOLATION_LEVEL, "READ_UNCOMMITTED");

    // Act Assert
    assertThatThrownBy(() -> new ConsensusCommitConfig(new DatabaseConfig(props)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_PropertiesWithCoordinatorNamespaceGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE, "changed_coordinator");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getCoordinatorNamespace()).isPresent();
    assertThat(config.getCoordinatorNamespace().get()).isEqualTo("changed_coordinator");
  }

  @Test
  public void constructor_ParallelExecutionRelatedPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.PARALLEL_EXECUTOR_COUNT, "100");
    props.setProperty(ConsensusCommitConfig.PARALLEL_PREPARATION_ENABLED, "false");
    props.setProperty(ConsensusCommitConfig.PARALLEL_VALIDATION_ENABLED, "false");
    props.setProperty(ConsensusCommitConfig.PARALLEL_COMMIT_ENABLED, "false");
    props.setProperty(ConsensusCommitConfig.PARALLEL_ROLLBACK_ENABLED, "false");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getParallelExecutorCount()).isEqualTo(100);
    assertThat(config.isParallelPreparationEnabled()).isFalse();
    assertThat(config.isParallelValidationEnabled()).isFalse();
    assertThat(config.isParallelCommitEnabled()).isFalse();
    assertThat(config.isParallelRollbackEnabled()).isFalse();
  }

  @Test
  public void
      constructor_ParallelExecutionRelatedPropertiesWithoutParallelValidationAndParallelRollbackPropertyGiven_ShouldUseParallelCommitValueForParallelValidationAndParallelRollback() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.PARALLEL_EXECUTOR_COUNT, "100");
    props.setProperty(ConsensusCommitConfig.PARALLEL_PREPARATION_ENABLED, "true");
    props.setProperty(ConsensusCommitConfig.PARALLEL_COMMIT_ENABLED, "false");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.getParallelExecutorCount()).isEqualTo(100);
    assertThat(config.isParallelPreparationEnabled()).isTrue();
    assertThat(config.isParallelValidationEnabled()).isFalse(); // use the parallel commit value
    assertThat(config.isParallelCommitEnabled()).isFalse();
    assertThat(config.isParallelRollbackEnabled()).isFalse(); // use the parallel commit value
  }

  @Test
  public void constructor_AsyncExecutionRelatedPropertiesGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.ASYNC_COMMIT_ENABLED, "true");
    props.setProperty(ConsensusCommitConfig.ASYNC_ROLLBACK_ENABLED, "true");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.isAsyncCommitEnabled()).isTrue();
    assertThat(config.isAsyncRollbackEnabled()).isTrue();
  }

  @Test
  public void
      constructor_AsyncExecutionRelatedPropertiesWithoutAsyncRollbackPropertyGiven_ShouldUseAsyncCommitValueForAsyncRollback() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.ASYNC_COMMIT_ENABLED, "false");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.isAsyncCommitEnabled()).isFalse();
    assertThat(config.isAsyncRollbackEnabled()).isFalse(); // use the async commit value
  }

  @Test
  public void
      constructor_PropertiesWithCoordinatorWriteOmissionOnReadOnlyEnabledGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(
        ConsensusCommitConfig.COORDINATOR_WRITE_OMISSION_ON_READ_ONLY_ENABLED, "false");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.isCoordinatorWriteOmissionOnReadOnlyEnabled()).isFalse();
  }

  @Test
  public void constructor_PropertiesWithOnePhaseCommitEnabledGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.ONE_PHASE_COMMIT_ENABLED, "true");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.isOnePhaseCommitEnabled()).isTrue();
  }

  @Test
  public void constructor_PropertiesWithParallelImplicitPreReadEnabledGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.PARALLEL_IMPLICIT_PRE_READ, "false");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.isParallelImplicitPreReadEnabled()).isFalse();
  }

  @Test
  public void constructor_PropertiesWithIncludeMetadataEnabledGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(ConsensusCommitConfig.INCLUDE_METADATA_ENABLED, "true");

    // Act
    ConsensusCommitConfig config = new ConsensusCommitConfig(new DatabaseConfig(props));

    // Assert
    assertThat(config.isIncludeMetadataEnabled()).isTrue();
  }
}
