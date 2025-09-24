package com.scalar.db.storage.jdbc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.ScanAll;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
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
    doThrow(exception).when(rdbEngine).throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(any(), any());

    // Act
    Assertions.assertThatThrownBy(
            () -> operationChecker.checkOrderingsForScanAll(scanAll, tableMetadata))
        .isEqualTo(exception);

    // Assert
    verify(rdbEngine)
        .throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(scanAll, tableMetadata);
  }
}
