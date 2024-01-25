package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.azure.cosmos.ConsistencyLevel;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CosmosUtilsTest {

  @Mock private CosmosConfig cosmosConfig;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void getConsistencyLevel_ShouldReturnStrongConsistency() {
    // Arrange

    // Act
    ConsistencyLevel actual = CosmosUtils.getConsistencyLevel(cosmosConfig);

    // Assert
    assertThat(actual).isEqualTo(ConsistencyLevel.STRONG);
  }

  @Test
  public void getConsistencyLevel_StrongGiven_ShouldReturnStrongConsistency() {
    // Arrange
    when(cosmosConfig.getConsistencyLevel()).thenReturn(Optional.of("STRONG"));

    // Act
    ConsistencyLevel actual = CosmosUtils.getConsistencyLevel(cosmosConfig);

    // Assert
    assertThat(actual).isEqualTo(ConsistencyLevel.STRONG);
  }

  @Test
  public void getConsistencyLevel_BoundedStalenessGiven_ShouldReturnBoundedStalenessConsistency() {
    // Arrange
    when(cosmosConfig.getConsistencyLevel()).thenReturn(Optional.of("bounded_staleness"));

    // Act
    ConsistencyLevel actual = CosmosUtils.getConsistencyLevel(cosmosConfig);

    // Assert
    assertThat(actual).isEqualTo(ConsistencyLevel.BOUNDED_STALENESS);
  }

  @Test
  public void getConsistencyLevel_InvalidConsistencyGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    when(cosmosConfig.getConsistencyLevel()).thenReturn(Optional.of("any"));

    // Act Assert
    Assertions.assertThatThrownBy(() -> CosmosUtils.getConsistencyLevel(cosmosConfig))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
