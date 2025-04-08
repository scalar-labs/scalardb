package com.scalar.db.dataloader.core.dataimport.processor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScalarDBMode;
import com.scalar.db.dataloader.core.UnitTestUtils;
import com.scalar.db.dataloader.core.dataimport.ImportMode;
import com.scalar.db.dataloader.core.dataimport.ImportOptions;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileValidationLevel;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDBDao;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDBDaoException;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.log.LogMode;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

class JsonImportProcessorTest {
  @Mock private ImportProcessorParams params;
  @Mock ScalarDBMode scalarDBMode;
  @Mock ImportOptions importOptions;
  @Mock Map<String, TableMetadata> tableMetadataByTableName;
  @Mock TableColumnDataTypes tableColumnDataTypes;

  ScalarDBDao dao;
  @Mock DistributedStorage distributedStorage;
  DistributedTransactionManager distributedTransactionManager;
  JsonImportProcessor jsonImportProcessor;

  @BeforeEach
  void setup() throws ScalarDBDaoException, TransactionException {
    dao = Mockito.mock(ScalarDBDao.class);
    distributedTransactionManager = mock(DistributedTransactionManager.class);
    DistributedTransaction distributedTransaction = mock(DistributedTransaction.class);
    when(distributedTransactionManager.start()).thenReturn(distributedTransaction);
    tableMetadataByTableName = new HashMap<>();
    tableMetadataByTableName.put("namespace.table", UnitTestUtils.createTestTableMetadata());
    tableColumnDataTypes = UnitTestUtils.getTableColumnData();
    importOptions =
        ImportOptions.builder()
            .importMode(ImportMode.UPSERT)
            .fileFormat(FileFormat.JSON)
            .controlFile(UnitTestUtils.getControlFile())
            .controlFileValidationLevel(ControlFileValidationLevel.MAPPED)
            .namespace("namespace")
            .transactionBatchSize(1)
            .dataChunkSize(5)
            .tableName("table")
            .logMode(LogMode.SINGLE_FILE)
            .maxThreads(16)
            .dataChunkQueueSize(256)
            .build();
    Mockito.when(
            dao.get(
                "namespace",
                "table",
                UnitTestUtils.getPartitionKey(1),
                UnitTestUtils.getClusteringKey(),
                distributedStorage))
        .thenReturn(UnitTestUtils.getResult(1));
    Mockito.when(
            dao.get(
                "namespace",
                "table",
                UnitTestUtils.getPartitionKey(1),
                UnitTestUtils.getClusteringKey(),
                distributedTransaction))
        .thenReturn(UnitTestUtils.getResult(1));
  }

  @Test
  void test_importProcessWithStorage() {
    params =
        ImportProcessorParams.builder()
            .scalarDBMode(ScalarDBMode.STORAGE)
            .importOptions(importOptions)
            .dao(dao)
            .distributedStorage(distributedStorage)
            .distributedTransactionManager(distributedTransactionManager)
            .scalarDBMode(scalarDBMode)
            .tableColumnDataTypes(tableColumnDataTypes)
            .tableMetadataByTableName(tableMetadataByTableName)
            .build();
    jsonImportProcessor = new JsonImportProcessor(params);
    Map<Integer, ImportDataChunkStatus> statusList =
        jsonImportProcessor.process(5, 1, UnitTestUtils.getJsonReader());
    assert statusList != null;
    Assertions.assertEquals(1, statusList.size());
  }

  @Test
  void test_importProcessWithTransaction() {
    params =
        ImportProcessorParams.builder()
            .scalarDBMode(ScalarDBMode.TRANSACTION)
            .importOptions(importOptions)
            .dao(dao)
            .distributedStorage(distributedStorage)
            .distributedTransactionManager(distributedTransactionManager)
            .scalarDBMode(scalarDBMode)
            .tableColumnDataTypes(tableColumnDataTypes)
            .tableMetadataByTableName(tableMetadataByTableName)
            .build();
    jsonImportProcessor = new JsonImportProcessor(params);
    Map<Integer, ImportDataChunkStatus> statusList =
        jsonImportProcessor.process(5, 1, UnitTestUtils.getJsonReader());
    assert statusList != null;
    Assertions.assertEquals(1, statusList.size());
  }
}
