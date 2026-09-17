package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosException;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CosmosUtilsTest {

  private static final String SHORT_MESSAGE =
      "[\"Request rate is large. More Request Units may be needed, so no changes were made. "
          + "Please retry this request later. Learn more: http://aka.ms/cosmosdb-error-429\"]";

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

  @Test
  public void buildErrorDetails_CosmosExceptionGiven_ShouldReturnStatusCodesAndShortMessage() {
    // Arrange
    CosmosException exception = mock(CosmosException.class);
    when(exception.getStatusCode()).thenReturn(429);
    when(exception.getSubStatusCode()).thenReturn(3200);
    when(exception.getShortMessage()).thenReturn(SHORT_MESSAGE);

    // Act
    String actual = CosmosUtils.buildErrorDetails(exception);

    // Assert
    assertThat(actual).isEqualTo("statusCode=429, subStatusCode=3200, message=" + SHORT_MESSAGE);
  }

  @Test
  public void buildErrorDetails_CosmosExceptionWithDiagnosticsGiven_ShouldNotContainDiagnostics() {
    // Arrange
    // getMessage() of CosmosException embeds the whole CosmosDiagnostics, which can grow to tens of
    // kilobytes. Callers that carry error messages across a boundary generally impose size limits
    // far below that, so buildErrorDetails() must not use it.
    String diagnostics = createLargeDiagnostics();
    CosmosException exception = mock(CosmosException.class);
    when(exception.getStatusCode()).thenReturn(429);
    when(exception.getSubStatusCode()).thenReturn(3200);
    when(exception.getShortMessage()).thenReturn(SHORT_MESSAGE);
    when(exception.getMessage())
        .thenReturn(
            "{\"innerErrorMessage\":"
                + SHORT_MESSAGE
                + ",\"cosmosDiagnostics\":{"
                + diagnostics
                + "}}");

    // Act
    String actual = CosmosUtils.buildErrorDetails(exception);

    // Assert
    assertThat(actual).doesNotContain(diagnostics);
    assertThat(actual).contains(SHORT_MESSAGE);
    assertThat(actual.length()).isLessThan(1024);
  }

  @Test
  public void buildErrorDetails_CosmosExceptionWithoutShortMessageGiven_ShouldNotThrow() {
    // Arrange
    CosmosException exception = mock(CosmosException.class);
    when(exception.getStatusCode()).thenReturn(500);
    when(exception.getSubStatusCode()).thenReturn(0);
    when(exception.getShortMessage()).thenReturn(null);

    // Act
    String actual = CosmosUtils.buildErrorDetails(exception);

    // Assert
    assertThat(actual).isEqualTo("statusCode=500, subStatusCode=0, message=null");
  }

  private static String createLargeDiagnostics() {
    StringBuilder builder = new StringBuilder();
    while (builder.length() < 36000) {
      builder.append("\"retryContext\":{\"statusAndSubStatusCodes\":[[429,3200]]},");
    }
    return builder.toString();
  }
}
