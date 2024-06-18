package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.cosmos.CosmosClient;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.common.FilterableScanner;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CosmosTest {

  private static final int ANY_LIMIT = 100;

  private Cosmos cosmos;
  @Mock private CosmosClient cosmosClient;
  @Mock private SelectStatementHandler selectStatementHandler;
  @Mock private PutStatementHandler putStatementHandler;
  @Mock private DeleteStatementHandler deleteStatementHandler;
  @Mock private BatchHandler batchHandler;
  @Mock private OperationChecker operationChecker;
  @Mock private ScannerImpl scanner;
  @Mock private Key partitionKey;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    Properties cosmosConfigProperties = new Properties();
    cosmos =
        new Cosmos(
            new DatabaseConfig(cosmosConfigProperties),
            cosmosClient,
            selectStatementHandler,
            putStatementHandler,
            deleteStatementHandler,
            batchHandler,
            operationChecker);
  }

  @Test
  public void get_WithoutConjunction_ShouldHandledWithOriginalGet() throws ExecutionException {
    // Arrange
    Get get =
        Get.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(partitionKey)
            .projection("col1")
            .build();
    when(selectStatementHandler.handle(any(Get.class))).thenReturn(scanner);

    // Act
    Optional<Result> actual = cosmos.get(get);

    // Assert
    assertThat(actual.isPresent()).isFalse();
    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(selectStatementHandler).handle(captor.capture());
    Get actualGet = captor.getValue();
    assertThat(actualGet).isEqualTo(get);
  }

  @Test
  public void get_WithConjunctionWithoutProjections_ShouldHandledWithoutProjections()
      throws ExecutionException {
    // Arrange
    Get get =
        Get.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(partitionKey)
            .where(ConditionBuilder.column("col2").isLessThanInt(0))
            .build();
    when(selectStatementHandler.handle(any(Get.class))).thenReturn(scanner);

    // Act
    Optional<Result> actual = cosmos.get(get);

    // Assert
    assertThat(actual.isPresent()).isFalse();
    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(selectStatementHandler).handle(captor.capture());
    Get actualGet = captor.getValue();
    assertThat(actualGet.getProjections()).isEmpty();
  }

  @Test
  public void get_WithConjunctionAndProjections_ShouldHandledWithExtendedProjections()
      throws ExecutionException {
    // Arrange
    Get get =
        Get.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(partitionKey)
            .projections("col1")
            .where(ConditionBuilder.column("col2").isLessThanInt(0))
            .build();
    when(selectStatementHandler.handle(any(Get.class))).thenReturn(scanner);

    // Act
    Optional<Result> actual = cosmos.get(get);

    // Assert
    assertThat(actual.isPresent()).isFalse();
    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(selectStatementHandler).handle(captor.capture());
    Get actualGet = captor.getValue();
    assertThat(actualGet.getProjections()).containsExactlyInAnyOrder("col1", "col2");
  }

  @Test
  public void scan_WithLimitWithoutConjunction_ShouldHandledWithLimit() throws ExecutionException {
    // Arrange
    Scan scan = Scan.newBuilder().namespace("ns").table("tbl").all().limit(ANY_LIMIT).build();
    when(selectStatementHandler.handle(scan)).thenReturn(scanner);

    // Act
    Scanner actual = cosmos.scan(scan);

    // Assert
    assertThat(actual).isInstanceOf(ScannerImpl.class);
    ArgumentCaptor<Scan> captor = ArgumentCaptor.forClass(Scan.class);
    verify(selectStatementHandler).handle(captor.capture());
    Scan actualScan = captor.getValue();
    assertThat(actualScan.getLimit()).isEqualTo(ANY_LIMIT);
  }

  @Test
  public void scan_WithLimitAndConjunction_ShouldHandledWithoutLimit() throws ExecutionException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .all()
            .where(mock(ConditionalExpression.class))
            .limit(ANY_LIMIT)
            .build();
    when(selectStatementHandler.handle(scan)).thenReturn(scanner);

    // Act
    Scanner actual = cosmos.scan(scan);

    // Assert
    assertThat(actual).isInstanceOf(FilterableScanner.class);
    ArgumentCaptor<Scan> captor = ArgumentCaptor.forClass(Scan.class);
    verify(selectStatementHandler).handle(captor.capture());
    Scan actualScan = captor.getValue();
    assertThat(actualScan.getLimit()).isEqualTo(0);
  }

  @Test
  public void scan_WithConjunctionWithoutProjections_ShouldHandledWithoutProjections()
      throws ExecutionException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .all()
            .where(ConditionBuilder.column("col2").isLessThanInt(0))
            .build();
    when(selectStatementHandler.handle(scan)).thenReturn(scanner);

    // Act
    Scanner actual = cosmos.scan(scan);

    // Assert
    assertThat(actual).isInstanceOf(FilterableScanner.class);
    ArgumentCaptor<Scan> captor = ArgumentCaptor.forClass(Scan.class);
    verify(selectStatementHandler).handle(captor.capture());
    Scan actualScan = captor.getValue();
    assertThat(actualScan.getProjections()).isEmpty();
  }

  @Test
  public void scan_WithConjunctionAndProjections_ShouldHandledWithExtendedProjections()
      throws ExecutionException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .all()
            .projections("col1")
            .where(ConditionBuilder.column("col2").isLessThanInt(0))
            .build();
    when(selectStatementHandler.handle(scan)).thenReturn(scanner);

    // Act
    Scanner actual = cosmos.scan(scan);

    // Assert
    assertThat(actual).isInstanceOf(FilterableScanner.class);
    ArgumentCaptor<Scan> captor = ArgumentCaptor.forClass(Scan.class);
    verify(selectStatementHandler).handle(captor.capture());
    Scan actualScan = captor.getValue();
    assertThat(actualScan.getProjections()).containsExactlyInAnyOrder("col1", "col2");
  }
}
