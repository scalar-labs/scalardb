package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.ResultSet;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CassandraTest {
  private Cassandra cassandra;
  private StatementHandlerManager handlers;
  @Mock private DatabaseConfig config;
  @Mock private ClusterManager clusterManager;
  @Mock private SelectStatementHandler select;
  @Mock private InsertStatementHandler insert;
  @Mock private UpdateStatementHandler update;
  @Mock private DeleteStatementHandler delete;
  @Mock private BatchHandler batchHandler;
  @Mock private TableMetadataManager metadataManager;
  @Mock private OperationChecker operationChecker;
  @Mock private ResultSet resultSet;
  @Mock private TableMetadata tableMetadata;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    handlers =
        StatementHandlerManager.builder()
            .select(select)
            .insert(insert)
            .update(update)
            .delete(delete)
            .build();
    cassandra =
        new Cassandra(
            config, clusterManager, handlers, batchHandler, metadataManager, operationChecker);
  }

  @Test
  public void scan_ScanAllWithoutOrderingAndConditionsGiven_ShouldScanHandled()
      throws ExecutionException {
    // Arrange
    Scan scan = Scan.newBuilder().namespace("namespace").table("table").all().build();
    when(handlers.select().handle(any())).thenReturn(resultSet);
    when(metadataManager.getTableMetadata(any())).thenReturn(tableMetadata);

    // Act
    cassandra.scan(scan);

    // Assert
    verify(handlers.select()).handle(scan);
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
    assertThatThrownBy(() -> cassandra.scan(scan))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
