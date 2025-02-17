package com.scalar.db.dataloader.core.dataexport.producer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.dataloader.core.UnitTestUtils;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonLineProducerTaskTest {
  TableMetadata mockMetadata;
  List<String> projectedColumns;
  Map<String, DataType> columnData;
  JsonLineProducerTask jsonLineProducerTask;

  @BeforeEach
  void setup() {
    mockMetadata = UnitTestUtils.createTestTableMetadata();
    projectedColumns = UnitTestUtils.getColumnsListOfMetadata();
    columnData = UnitTestUtils.getColumnData();
    jsonLineProducerTask =
        new JsonLineProducerTask(false, projectedColumns, mockMetadata, columnData);
  }

  @Test
  void process_withEmptyResultList_shouldReturnEmptyString() {
    List<Result> results = Collections.emptyList();
    String output = jsonLineProducerTask.process(results);
    Assertions.assertEquals("", output);
  }

  @Test
  void process_withValidResultList_shouldReturnValidJsonLineString() {
    ObjectNode rootNode = UnitTestUtils.getOutputDataWithoutMetadata();
    Map<String, Column<?>> values = UnitTestUtils.createTestValues();
    Result result = new ResultImpl(values, mockMetadata);
    List<Result> resultList = new ArrayList<>();
    resultList.add(result);
    String output = jsonLineProducerTask.process(resultList);
    Assertions.assertEquals(rootNode.toString(), output.trim());
  }

  @Test
  void process_withValidResultListWithMetadata_shouldReturnValidJsonLineString() {
    jsonLineProducerTask =
        new JsonLineProducerTask(true, projectedColumns, mockMetadata, columnData);
    ObjectNode rootNode = UnitTestUtils.getOutputDataWithMetadata();
    Map<String, Column<?>> values = UnitTestUtils.createTestValues();
    Result result = new ResultImpl(values, mockMetadata);
    List<Result> resultList = new ArrayList<>();
    resultList.add(result);
    String output = jsonLineProducerTask.process(resultList);
    Assertions.assertEquals(rootNode.toString(), output.trim());
  }
}
