package com.scalar.db.dataloader.core.dataimport.processor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScalarDbMode;
import com.scalar.db.dataloader.core.UnitTestUtils;
import com.scalar.db.dataloader.core.dataimport.ImportMode;
import com.scalar.db.dataloader.core.dataimport.ImportOptions;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileValidationLevel;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDao;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDaoException;
import com.scalar.db.dataloader.core.dataimport.log.LogMode;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

class CsvImportProcessorTest {
  @Mock private ImportProcessorParams params;
  @Mock ImportOptions importOptions;
  @Mock Map<String, TableMetadata> tableMetadataByTableName;
  @Mock TableColumnDataTypes tableColumnDataTypes;

  ScalarDbDao dao;
  @Mock DistributedStorage distributedStorage;
  DistributedTransactionManager distributedTransactionManager;
  CsvImportProcessor csvImportProcessor;

  @BeforeEach
  void setup() throws ScalarDbDaoException, TransactionException {
    dao = Mockito.mock(ScalarDbDao.class);
    distributedTransactionManager = mock(DistributedTransactionManager.class);
    DistributedTransaction distributedTransaction = mock(DistributedTransaction.class);
    when(distributedTransactionManager.start()).thenReturn(distributedTransaction);
    tableMetadataByTableName = new HashMap<>();
    tableMetadataByTableName.put("namespace.table", UnitTestUtils.createTestTableMetadata());
    tableColumnDataTypes = UnitTestUtils.getTableColumnData();
    importOptions =
        ImportOptions.builder()
            .importMode(ImportMode.UPSERT)
            .fileFormat(FileFormat.CSV)
            .controlFile(UnitTestUtils.getControlFile())
            .controlFileValidationLevel(ControlFileValidationLevel.MAPPED)
            .namespace("namespace")
            .transactionBatchSize(1)
            .dataChunkSize(5)
            .tableName("table")
            .logMode(LogMode.SINGLE_FILE)
            .maxThreads(8)
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
            .scalarDbMode(ScalarDbMode.STORAGE)
            .importOptions(importOptions)
            .dao(dao)
            .distributedStorage(distributedStorage)
            .distributedTransactionManager(distributedTransactionManager)
            .tableColumnDataTypes(tableColumnDataTypes)
            .tableMetadataByTableName(tableMetadataByTableName)
            .build();
    csvImportProcessor = new CsvImportProcessor(params);
    Assertions.assertDoesNotThrow(
        () -> {
          csvImportProcessor.process(5, 1, UnitTestUtils.getCsvReader());
        });
  }

  @Test
  void test_importProcessWithTransaction() {
    params =
        ImportProcessorParams.builder()
            .scalarDbMode(ScalarDbMode.TRANSACTION)
            .importOptions(importOptions)
            .dao(dao)
            .distributedStorage(distributedStorage)
            .distributedTransactionManager(distributedTransactionManager)
            .tableColumnDataTypes(tableColumnDataTypes)
            .tableMetadataByTableName(tableMetadataByTableName)
            .build();
    csvImportProcessor = new CsvImportProcessor(params);
    Assertions.assertDoesNotThrow(
        () -> {
          csvImportProcessor.process(5, 1, UnitTestUtils.getCsvReader());
        });
  }
}
