package com.scalar.db.storage.jdbc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Selection;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import java.util.Collections;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class JdbcOperationCheckerTest {

  @Mock private DatabaseConfig databaseConfig;
  @Mock private TableMetadataManager tableMetadataManager;
  @Mock private StorageInfoProvider storageInfoProvider;
  @Mock private RdbEngineStrategy rdbEngine;
  @Mock private ScanAll scanAll;
  @Mock private Scan scan;
  @Mock private Selection selection;
  @Mock private TableMetadata tableMetadata;
  private JdbcOperationChecker operationChecker;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    operationChecker =
        new JdbcOperationChecker(
            databaseConfig, tableMetadataManager, storageInfoProvider, rdbEngine);
  }

  @Test
  public void checkOrderingsForScanAll_ShouldInvokeAdditionalCheckOnRdbEngine() {
    // Arrange
    // Act
    operationChecker.checkOrderingsForScanAll(scanAll, tableMetadata);

    // Assert
    verify(rdbEngine)
        .throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(scanAll, tableMetadata);
  }

  @Test
  public void checkOrderingsForScanAll_WhenAdditionalCheckThrows_ShouldPropagateException() {
    // Arrange
    Exception exception = new RuntimeException();
    doThrow(exception)
        .when(rdbEngine)
        .throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(
            any(ScanAll.class), any(TableMetadata.class));

    // Act
    Assertions.assertThatThrownBy(
            () -> operationChecker.checkOrderingsForScanAll(scanAll, tableMetadata))
        .isEqualTo(exception);

    // Assert
    verify(rdbEngine)
        .throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(scanAll, tableMetadata);
  }

  @Test
  public void checkConjunctions_ShouldInvokeAdditionalCheckOnRdbEngine() {
    // Arrange
    // Act
    Conjunction conjunction1 = mock(Conjunction.class);
    Conjunction conjunction2 = mock(Conjunction.class);
    Set<Conjunction> conjunctions = Sets.newHashSet(conjunction1, conjunction2);
    when(selection.getConjunctions()).thenReturn(conjunctions);
    operationChecker.checkConjunctions(selection, tableMetadata);

    // Assert
    verify(rdbEngine).throwIfConjunctionsOnBlobColumnNotSupported(conjunctions, tableMetadata);
  }

  @Test
  public void checkConjunctions_WhenAdditionalCheckThrows_ShouldPropagateException() {
    // Arrange
    Exception exception = new RuntimeException();
    doThrow(exception)
        .when(rdbEngine)
        .throwIfConjunctionsOnBlobColumnNotSupported(any(), any(TableMetadata.class));
    Set<Conjunction> conjunctions = Collections.emptySet();
    when(selection.getConjunctions()).thenReturn(conjunctions);

    // Act
    Assertions.assertThatThrownBy(
            () -> operationChecker.checkConjunctions(selection, tableMetadata))
        .isEqualTo(exception);

    // Assert
    verify(rdbEngine).throwIfConjunctionsOnBlobColumnNotSupported(conjunctions, tableMetadata);
  }
}
