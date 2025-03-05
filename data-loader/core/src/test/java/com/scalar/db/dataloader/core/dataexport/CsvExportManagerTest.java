package com.scalar.db.dataloader.core.dataexport;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScanRange;
import com.scalar.db.dataloader.core.UnitTestUtils;
import com.scalar.db.dataloader.core.dataexport.producer.ProducerTaskFactory;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDBDao;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDBDaoException;
import com.scalar.db.io.Column;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.Spy;

public class CsvExportManagerTest {
  TableMetadata mockData;
  DistributedStorage storage;
  @Spy ScalarDBDao dao;
  ProducerTaskFactory producerTaskFactory;
  ExportManager exportManager;

  @BeforeEach
  void setup() {
    storage = Mockito.mock(DistributedStorage.class);
    mockData = UnitTestUtils.createTestTableMetadata();
    dao = Mockito.mock(ScalarDBDao.class);
    producerTaskFactory = new ProducerTaskFactory(null, false, true);
  }

  @Test
  void startExport_givenValidDataWithoutPartitionKey_shouldGenerateOutputFile()
      throws IOException, ScalarDBDaoException {
    System.out.println(mockData);
    System.out.println("======");
    exportManager = new JsonLineExportManager(storage, dao, producerTaskFactory);
    Scanner scanner = Mockito.mock(Scanner.class);
    String filePath = Paths.get("").toAbsolutePath() + "/output.csv";
    Map<String, Column<?>> values = UnitTestUtils.createTestValues();
    Result result = new ResultImpl(values, mockData);
    List<Result> results = Collections.singletonList(result);
    ExportOptions exportOptions =
        ExportOptions.builder("namespace", "table", null, FileFormat.CSV)
            .sortOrders(Collections.emptyList())
            .scanRange(new ScanRange(null, null, false, false))
            .build();

    Mockito.when(
            dao.createScanner(
                exportOptions.getNamespace(),
                exportOptions.getTableName(),
                exportOptions.getProjectionColumns(),
                exportOptions.getLimit(),
                storage))
        .thenReturn(scanner);
    Mockito.when(scanner.iterator()).thenReturn(results.iterator());
    try (BufferedWriter writer =
        new BufferedWriter(
            Files.newBufferedWriter(
                Paths.get(filePath),
                Charset.defaultCharset(), // Explicitly use the default charset
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND))) {
      exportManager.startExport(exportOptions, mockData, writer);
    }
    File file = new File(filePath);
    Assertions.assertTrue(file.exists());
    Assertions.assertTrue(file.delete());
  }

  @Test
  void startExport_givenPartitionKey_shouldGenerateOutputFile()
      throws IOException, ScalarDBDaoException {
    producerTaskFactory = new ProducerTaskFactory(",", false, false);
    exportManager = new CsvExportManager(storage, dao, producerTaskFactory);
    Scanner scanner = Mockito.mock(Scanner.class);
    String filePath = Paths.get("").toAbsolutePath() + "/output.csv";
    Map<String, Column<?>> values = UnitTestUtils.createTestValues();
    Result result = new ResultImpl(values, mockData);
    List<Result> results = Collections.singletonList(result);

    ExportOptions exportOptions =
        ExportOptions.builder(
                "namespace",
                "table",
                Key.newBuilder().add(IntColumn.of("col1", 1)).build(),
                FileFormat.CSV)
            .sortOrders(Collections.emptyList())
            .scanRange(new ScanRange(null, null, false, false))
            .build();

    Mockito.when(
            dao.createScanner(
                exportOptions.getNamespace(),
                exportOptions.getTableName(),
                exportOptions.getScanPartitionKey(),
                exportOptions.getScanRange(),
                exportOptions.getSortOrders(),
                exportOptions.getProjectionColumns(),
                exportOptions.getLimit(),
                storage))
        .thenReturn(scanner);
    Mockito.when(scanner.iterator()).thenReturn(results.iterator());
    try (BufferedWriter writer =
        new BufferedWriter(
            Files.newBufferedWriter(
                Paths.get(filePath),
                Charset.defaultCharset(), // Explicitly use the default charset
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND))) {
      exportManager.startExport(exportOptions, mockData, writer);
    }
    File file = new File(filePath);
    Assertions.assertTrue(file.exists());
    Assertions.assertTrue(file.delete());
  }
}
