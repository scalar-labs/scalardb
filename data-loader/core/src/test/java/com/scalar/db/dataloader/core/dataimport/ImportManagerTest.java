package com.scalar.db.dataloader.core.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.ScalarDbMode;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.processor.ImportProcessor;
import com.scalar.db.dataloader.core.dataimport.processor.ImportProcessorFactory;
import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.scalar.db.dataloader.core.dataimport.processor.ImportProcessorParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ImportManagerTest {

  private ImportManager importManager;
  private ImportEventListener listener1;
  private ImportEventListener listener2;
  private DistributedStorage distributedStorage;
  private DistributedTransactionManager distributedTransactionManager;

  @BeforeEach
  void setUp() {
    Map<String, TableMetadata> tableMetadata = new HashMap<>();
    BufferedReader reader = mock(BufferedReader.class);
    ImportOptions options = mock(ImportOptions.class);
    ImportProcessorFactory processorFactory = mock(ImportProcessorFactory.class);

    listener1 = mock(ImportEventListener.class);
    listener2 = mock(ImportEventListener.class);
    distributedStorage = mock(DistributedStorage.class);
    distributedTransactionManager = mock(DistributedTransactionManager.class);

    importManager =
        new ImportManager(
            tableMetadata,
            reader,
            options,
            processorFactory,
            ScalarDbMode.STORAGE,
            distributedStorage,
            null); // Only one resource present
    importManager.addListener(listener1);
    importManager.addListener(listener2);
  }

  @Test
  void onAllDataChunksCompleted_shouldNotifyListenersAndCloseStorage() {
    importManager.onAllDataChunksCompleted();

    verify(listener1).onAllDataChunksCompleted();
    verify(listener2).onAllDataChunksCompleted();
    verify(distributedStorage).close();
  }

  @Test
  void onAllDataChunksCompleted_shouldAggregateListenerExceptionAndStillCloseResources() {
    doThrow(new RuntimeException("Listener1 failed")).when(listener1).onAllDataChunksCompleted();

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> importManager.onAllDataChunksCompleted());

    assertTrue(thrown.getMessage().contains("Error during completion"));
    assertEquals("Listener1 failed", thrown.getCause().getMessage());
    verify(distributedStorage).close();
  }

  @Test
  void closeResources_shouldCloseTransactionManagerIfStorageIsNull() {
    ImportManager managerWithTx =
        new ImportManager(
            new HashMap<>(),
            mock(BufferedReader.class),
            mock(ImportOptions.class),
            mock(ImportProcessorFactory.class),
            ScalarDbMode.TRANSACTION,
            null,
            distributedTransactionManager);

    managerWithTx.closeResources();
    verify(distributedTransactionManager).close();
  }

  @Test
  void closeResources_shouldThrowIfResourceCloseFails() {
    doThrow(new RuntimeException("Close failed")).when(distributedStorage).close();

    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> importManager.closeResources());

    assertEquals("Failed to close the resource", ex.getMessage());
    assertEquals("Close failed", ex.getCause().getMessage());
  }
}
