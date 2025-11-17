package com.scalar.db.dataloader.core.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.TransactionMode;
import com.scalar.db.dataloader.core.dataimport.processor.ImportProcessor;
import com.scalar.db.dataloader.core.dataimport.processor.ImportProcessorFactory;
import com.scalar.db.dataloader.core.dataimport.processor.ImportProcessorParams;
import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionManager;
import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class ImportManagerTest {

  private ImportManager importManager;
  private ImportEventListener listener1;
  private ImportEventListener listener2;
  private DistributedTransactionManager distributedTransactionManager;

  @BeforeEach
  void setUp() {
    Map<String, TableMetadata> tableMetadata = new HashMap<>();
    BufferedReader reader = mock(BufferedReader.class);
    ImportOptions options = mock(ImportOptions.class);
    ImportProcessorFactory processorFactory = mock(ImportProcessorFactory.class);

    listener1 = mock(ImportEventListener.class);
    listener2 = mock(ImportEventListener.class);
    distributedTransactionManager = mock(DistributedTransactionManager.class);

    importManager =
        new ImportManager(
            tableMetadata, reader, options, processorFactory, distributedTransactionManager);
    importManager.addListener(listener1);
    importManager.addListener(listener2);
  }

  @Test
  void onAllDataChunksCompleted_shouldNotifyListenersAndCloseStorage() {
    importManager.onAllDataChunksCompleted();

    verify(listener1).onAllDataChunksCompleted();
    verify(listener2).onAllDataChunksCompleted();
    verify(distributedTransactionManager).close();
  }

  @Test
  void onAllDataChunksCompleted_shouldAggregateListenerExceptionAndStillCloseResources() {
    doThrow(new RuntimeException("Listener1 failed")).when(listener1).onAllDataChunksCompleted();

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> importManager.onAllDataChunksCompleted());

    assertTrue(thrown.getMessage().contains("Error during completion"));
    assertEquals("Listener1 failed", thrown.getCause().getMessage());
    verify(distributedTransactionManager).close();
  }

  @Test
  void closeResources_shouldCloseTransactionManagerIfStorageIsNull() {
    ImportManager managerWithTx =
        new ImportManager(
            new HashMap<>(),
            mock(BufferedReader.class),
            mock(ImportOptions.class),
            mock(ImportProcessorFactory.class),
            distributedTransactionManager);

    managerWithTx.closeResources();
    verify(distributedTransactionManager).close();
  }

  @Test
  void closeResources_shouldThrowIfResourceCloseFails() {
    doThrow(new RuntimeException("Close failed")).when(distributedTransactionManager).close();

    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> importManager.closeResources());

    assertEquals("Failed to close the resource", ex.getMessage());
    assertEquals("Close failed", ex.getCause().getMessage());
  }

  @Test
  void startImport_shouldUseSingleCrudMode_whenTransactionManagerIsSingleCrud() throws Exception {
    // Arrange
    Map<String, TableMetadata> tableMetadata = new HashMap<>();
    BufferedReader reader = mock(BufferedReader.class);
    ImportOptions options = mock(ImportOptions.class);
    when(options.getDataChunkSize()).thenReturn(100);
    when(options.getTransactionBatchSize()).thenReturn(10);

    ImportProcessorFactory processorFactory = mock(ImportProcessorFactory.class);
    ImportProcessor processor = mock(ImportProcessor.class);
    when(processorFactory.createImportProcessor(any())).thenReturn(processor);

    SingleCrudOperationTransactionManager singleCrudTxManager =
        mock(SingleCrudOperationTransactionManager.class);

    ImportManager manager =
        new ImportManager(tableMetadata, reader, options, processorFactory, singleCrudTxManager);

    // Act
    manager.startImport();

    // Assert
    ArgumentCaptor<ImportProcessorParams> paramsCaptor =
        ArgumentCaptor.forClass(ImportProcessorParams.class);
    verify(processorFactory).createImportProcessor(paramsCaptor.capture());

    ImportProcessorParams capturedParams = paramsCaptor.getValue();
    assertEquals(TransactionMode.SINGLE_CRUD, capturedParams.getScalarDbMode());
  }

  @Test
  void startImport_shouldUseConsensusCommitMode_whenTransactionManagerIsNotSingleCrud()
      throws Exception {
    // Arrange
    Map<String, TableMetadata> tableMetadata = new HashMap<>();
    BufferedReader reader = mock(BufferedReader.class);
    ImportOptions options = mock(ImportOptions.class);
    when(options.getDataChunkSize()).thenReturn(100);
    when(options.getTransactionBatchSize()).thenReturn(10);

    ImportProcessorFactory processorFactory = mock(ImportProcessorFactory.class);
    ImportProcessor processor = mock(ImportProcessor.class);
    when(processorFactory.createImportProcessor(any())).thenReturn(processor);

    DistributedTransactionManager regularTxManager = mock(DistributedTransactionManager.class);

    ImportManager manager =
        new ImportManager(tableMetadata, reader, options, processorFactory, regularTxManager);

    // Act
    manager.startImport();

    // Assert
    ArgumentCaptor<ImportProcessorParams> paramsCaptor =
        ArgumentCaptor.forClass(ImportProcessorParams.class);
    verify(processorFactory).createImportProcessor(paramsCaptor.capture());

    ImportProcessorParams capturedParams = paramsCaptor.getValue();
    assertEquals(TransactionMode.CONSENSUS_COMMIT, capturedParams.getScalarDbMode());
  }
}
