package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

import com.azure.cosmos.CosmosClient;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Scan;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
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
}
