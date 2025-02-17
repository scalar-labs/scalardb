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

class JsonProducerTaskTest {
  TableMetadata mockMetadata;
  List<String> projectedColumns;
  Map<String, DataType> columnData;
  JsonProducerTask jsonProducerTask;

  @BeforeEach
  void setup() {
    mockMetadata = UnitTestUtils.createTestTableMetadata();
    projectedColumns = UnitTestUtils.getColumnsListOfMetadata();
    columnData = UnitTestUtils.getColumnData();
    jsonProducerTask =
        new JsonProducerTask(false, projectedColumns, mockMetadata, columnData, true);
  }

  @Test
  void process_withEmptyResultList_shouldReturnEmptyString() {
    List<Result> results = Collections.emptyList();
    String output = jsonProducerTask.process(results);
    Assertions.assertEquals(" ", output);
  }

  @Test
  void process_withValidResultList_shouldReturnValidJsonString() {
    ObjectNode rootNode = UnitTestUtils.getOutputDataWithoutMetadata();
    Map<String, Column<?>> values = UnitTestUtils.createTestValues();
    Result result = new ResultImpl(values, mockMetadata);
    List<Result> resultList = new ArrayList<>();
    resultList.add(result);
    String output = jsonProducerTask.process(resultList);
    Assertions.assertEquals(rootNode.toPrettyString(), output.trim());
  }

  @Test
  void process_withValidResultListWithMetadata_shouldReturnValidJsonString() {
    jsonProducerTask = new JsonProducerTask(true, projectedColumns, mockMetadata, columnData, true);
    ObjectNode rootNode = UnitTestUtils.getOutputDataWithMetadata();
    Map<String, Column<?>> values = UnitTestUtils.createTestValues();
    Result result = new ResultImpl(values, mockMetadata);
    List<Result> resultList = new ArrayList<>();
    resultList.add(result);
    String output = jsonProducerTask.process(resultList);
    Assertions.assertEquals(rootNode.toPrettyString(), output.trim());
  }
}
