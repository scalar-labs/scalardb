package com.scalar.db.dataloader.core.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.TransactionMode;
import com.scalar.db.dataloader.core.dataimport.processor.ImportProcessorFactory;
import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
            tableMetadata,
            reader,
            options,
            processorFactory,
            TransactionMode.SINGLE_CRUD,
            distributedTransactionManager);
    importManager.addListener(listener1);
    importManager.addListener(listener2);
  }

  @Test
  void onAllImportsCompleted_shouldNotifyListenersAndCloseStorage() {
    importManager.onAllImportsCompleted();

    verify(listener1).onAllImportsCompleted();
    verify(listener2).onAllImportsCompleted();
    verify(distributedTransactionManager).close();
  }

  @Test
  void onAllImportsCompleted_shouldAggregateListenerExceptionAndStillCloseResources() {
    doThrow(new RuntimeException("Listener1 failed")).when(listener1).onAllImportsCompleted();

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> importManager.onAllImportsCompleted());

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
            TransactionMode.CONSENSUS_COMMIT,
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
}
