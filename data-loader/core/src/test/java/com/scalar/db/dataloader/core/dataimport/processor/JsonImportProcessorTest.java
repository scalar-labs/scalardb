package com.scalar.db.dataloader.core.dataimport.processor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.TransactionMode;
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

class JsonImportProcessorTest {
  @Mock private ImportProcessorParams params;
  @Mock ImportOptions importOptions;
  @Mock Map<String, TableMetadata> tableMetadataByTableName;
  @Mock TableColumnDataTypes tableColumnDataTypes;

  ScalarDbDao dao;
  DistributedTransactionManager distributedTransactionManager;
  JsonImportProcessor jsonImportProcessor;

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
            .fileFormat(FileFormat.JSON)
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
                distributedTransactionManager))
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
  void test_importProcessWithSingleCrud() {
    params =
        ImportProcessorParams.builder()
            .transactionMode(TransactionMode.SINGLE_CRUD)
            .importOptions(importOptions)
            .dao(dao)
            .distributedTransactionManager(distributedTransactionManager)
            .tableColumnDataTypes(tableColumnDataTypes)
            .tableMetadataByTableName(tableMetadataByTableName)
            .build();
    jsonImportProcessor = new JsonImportProcessor(params);
    Assertions.assertDoesNotThrow(
        () -> {
          jsonImportProcessor.process(5, 1, UnitTestUtils.getJsonReader());
        });
  }

  @Test
  void test_importProcessWithConsensusCommit() {
    params =
        ImportProcessorParams.builder()
            .transactionMode(TransactionMode.CONSENSUS_COMMIT)
            .importOptions(importOptions)
            .dao(dao)
            .distributedTransactionManager(distributedTransactionManager)
            .tableColumnDataTypes(tableColumnDataTypes)
            .tableMetadataByTableName(tableMetadataByTableName)
            .build();
    jsonImportProcessor = new JsonImportProcessor(params);
    Assertions.assertDoesNotThrow(
        () -> {
          jsonImportProcessor.process(5, 1, UnitTestUtils.getJsonReader());
        });
  }
}
