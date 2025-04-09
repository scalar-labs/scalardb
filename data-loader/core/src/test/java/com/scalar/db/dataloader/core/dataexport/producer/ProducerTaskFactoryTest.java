package com.scalar.db.dataloader.core.dataexport.producer;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.UnitTestUtils;
import com.scalar.db.io.DataType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProducerTaskFactoryTest {

  TableMetadata mockMetadata;
  List<String> projectedColumns;
  Map<String, DataType> columnData;

  @BeforeEach
  void setup() {
    mockMetadata = UnitTestUtils.createTestTableMetadata();
    projectedColumns = UnitTestUtils.getColumnsListOfMetadata();
    columnData = UnitTestUtils.getColumnData();
  }

  @Test
  void createProducerTask_withJsonFileFormat_shouldReturnJsonProducerTask() {
    ProducerTaskFactory producerTaskFactory = new ProducerTaskFactory(null, false, true);
    Assertions.assertEquals(
        JsonProducerTask.class,
        producerTaskFactory
            .createProducerTask(FileFormat.JSON, projectedColumns, mockMetadata, columnData)
            .getClass());
  }

  @Test
  void createProducerTask_withJsonLinesFileFormat_shouldReturnJsonLineProducerTask() {
    ProducerTaskFactory producerTaskFactory = new ProducerTaskFactory(null, false, false);
    Assertions.assertEquals(
        JsonLineProducerTask.class,
        producerTaskFactory
            .createProducerTask(FileFormat.JSONL, projectedColumns, mockMetadata, columnData)
            .getClass());
  }

  @Test
  void createProducerTask_withCsvFileFormat_shouldReturnCsvProducerTask() {
    ProducerTaskFactory producerTaskFactory = new ProducerTaskFactory(",", false, false);
    Assertions.assertEquals(
        CsvProducerTask.class,
        producerTaskFactory
            .createProducerTask(FileFormat.CSV, projectedColumns, mockMetadata, columnData)
            .getClass());
  }
}
