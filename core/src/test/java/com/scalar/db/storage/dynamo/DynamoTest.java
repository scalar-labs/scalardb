package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.common.FilterableScanner;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.cassandra.ScannerImpl;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoTest {

  private static final int ANY_LIMIT = 100;

  private Dynamo dynamo;
  @Mock private DynamoDbClient dynamoDbClient;
  @Mock private SelectStatementHandler selectStatementHandler;
  @Mock private PutStatementHandler putStatementHandler;
  @Mock private DeleteStatementHandler deleteStatementHandler;
  @Mock private BatchHandler batchHandler;
  @Mock private OperationChecker operationChecker;
  @Mock private ScannerImpl scanner;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    Properties cosmosConfigProperties = new Properties();
    dynamo =
        new Dynamo(
            new DatabaseConfig(cosmosConfigProperties),
            dynamoDbClient,
            selectStatementHandler,
            putStatementHandler,
            deleteStatementHandler,
            batchHandler,
            operationChecker);
  }

  @Test
  public void scan_WithLimitWithoutConjunction_ShouldHandledWithLimit() throws ExecutionException {
    // Arrange
    Scan scan = Scan.newBuilder().namespace("ns").table("tbl").all().limit(ANY_LIMIT).build();
    when(selectStatementHandler.handle(scan)).thenReturn(scanner);

    // Act
    Scanner actual = dynamo.scan(scan);

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
    Scanner actual = dynamo.scan(scan);

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
    Scanner actual = dynamo.scan(scan);

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
    Scanner actual = dynamo.scan(scan);

    // Assert
    assertThat(actual).isInstanceOf(FilterableScanner.class);
    ArgumentCaptor<Scan> captor = ArgumentCaptor.forClass(Scan.class);
    verify(selectStatementHandler).handle(captor.capture());
    Scan actualScan = captor.getValue();
    assertThat(actualScan.getProjections()).containsExactlyInAnyOrder("col1", "col2");
  }
}
