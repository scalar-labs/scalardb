package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import com.azure.cosmos.CosmosClient;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CosmosTest {

  private Cosmos cosmos;
  @Mock private DatabaseConfig config;
  @Mock private CosmosClient client;
  @Mock private SelectStatementHandler select;
  @Mock private PutStatementHandler put;
  @Mock private DeleteStatementHandler delete;
  @Mock private BatchHandler batch;
  @Mock private OperationChecker operationChecker;
  @Mock private Key partitionKey;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    cosmos = new Cosmos(config, client, select, put, delete, batch, operationChecker);
  }

  @Test
  public void scan_ScanAllWithoutOrderingAndConditionsGiven_ShouldScanHandled()
      throws ExecutionException {
    // Arrange
    Scan scan = Scan.newBuilder().namespace("namespace").table("table").all().build();

    // Act
    cosmos.scan(scan);

    // Assert
    verify(select).handle(scan);
  }

  @Test
  public void scan_ScanAllWithConditionsGiven_ShouldThrowUnsupportedOperationException() {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace("namespace")
            .table("table")
            .all()
            .where(ConditionBuilder.column("column").isEqualToInt(1))
            .build();

    // Act Assert
    assertThatThrownBy(() -> cosmos.scan(scan)).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void
      get_IllegalArgumentExceptionThrownByOperationChecker_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    Get get = Get.newBuilder().namespace("ns").table("tbl").partitionKey(partitionKey).build();
    doThrow(IllegalArgumentException.class).when(operationChecker).check(get);

    // Act Assert
    assertThatThrownBy(() -> cosmos.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      scan_IllegalArgumentExceptionThrownByOperationChecker_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    Scan scan = Scan.newBuilder().namespace("ns").table("tbl").partitionKey(partitionKey).build();
    doThrow(IllegalArgumentException.class).when(operationChecker).check(scan);

    // Act Assert
    assertThatThrownBy(() -> cosmos.scan(scan)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      put_IllegalArgumentExceptionThrownByOperationChecker_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    Put put = Put.newBuilder().namespace("ns").table("tbl").partitionKey(partitionKey).build();
    doThrow(IllegalArgumentException.class).when(operationChecker).check(put);

    // Act Assert
    assertThatThrownBy(() -> cosmos.put(put)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      put_MultiplePutsGiven_IllegalArgumentExceptionThrownByOperationChecker_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    Put put1 = Put.newBuilder().namespace("ns").table("tbl").partitionKey(partitionKey).build();
    Put put2 = Put.newBuilder().namespace("ns").table("tbl").partitionKey(partitionKey).build();

    doThrow(IllegalArgumentException.class).when(operationChecker).check(Arrays.asList(put1, put2));

    // Act Assert
    assertThatThrownBy(() -> cosmos.put(Arrays.asList(put1, put2)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      delete_IllegalArgumentExceptionThrownByOperationChecker_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    Delete delete =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(partitionKey).build();
    doThrow(IllegalArgumentException.class).when(operationChecker).check(delete);

    // Act Assert
    assertThatThrownBy(() -> cosmos.delete(delete)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      delete_MultipleDeletesGiven_IllegalArgumentExceptionThrownByOperationChecker_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    Delete delete1 =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(partitionKey).build();
    Delete delete2 =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(partitionKey).build();

    doThrow(IllegalArgumentException.class)
        .when(operationChecker)
        .check(Arrays.asList(delete1, delete2));

    // Act Assert
    assertThatThrownBy(() -> cosmos.delete(Arrays.asList(delete1, delete2)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      mutate_IllegalArgumentExceptionThrownByOperationChecker_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    Put put = Put.newBuilder().namespace("ns").table("tbl").partitionKey(partitionKey).build();
    Delete delete =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(partitionKey).build();

    doThrow(IllegalArgumentException.class)
        .when(operationChecker)
        .check(Arrays.asList(put, delete));

    // Act Assert
    assertThatThrownBy(() -> cosmos.mutate(Arrays.asList(put, delete)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
