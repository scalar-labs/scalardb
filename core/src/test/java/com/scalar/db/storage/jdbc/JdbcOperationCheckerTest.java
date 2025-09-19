package com.scalar.db.storage.jdbc;

import static org.mockito.Mockito.verify;

import com.scalar.db.api.ScanAll;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
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
  public void
      throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported_ShouldDelegateToRdbEngine() {
    // Arrange
    // Act
    operationChecker.throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(
        scanAll, tableMetadata);

    // Assert
    verify(rdbEngine)
        .throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(scanAll, tableMetadata);
  }
}
