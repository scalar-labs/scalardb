package com.scalar.db.dataloader.core.dataexport.producer;

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

class CsvProducerTaskTest {

  TableMetadata mockMetadata;
  List<String> projectedColumns;
  Map<String, DataType> columnData;
  CsvProducerTask csvProducerTask;

  @BeforeEach
  void setup() {
    mockMetadata = UnitTestUtils.createTestTableMetadata();
    projectedColumns = UnitTestUtils.getColumnsListOfMetadata();
    columnData = UnitTestUtils.getColumnData();
    csvProducerTask = new CsvProducerTask(false, projectedColumns, mockMetadata, columnData, ",");
  }

  @Test
  void process_withEmptyResultList_shouldReturnEmptyString() {
    List<Result> results = Collections.emptyList();
    String output = csvProducerTask.process(results);
    Assertions.assertEquals("", output);
  }

  @Test
  void process_withValidResultList_shouldReturnValidCsvString() {
    String expectedOutput =
        "9007199254740992,2147483647,true,0.000000000000000000000000000000000000000000001401298464324817,0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049,test value,YmxvYiB0ZXN0IHZhbHVl,2000-01-01,01:01:01,2000-01-01T01:01,1970-01-21T03:20:41.740Z";
    Map<String, Column<?>> values = UnitTestUtils.createTestValues();
    Result result = new ResultImpl(values, mockMetadata);
    List<Result> resultList = new ArrayList<>();
    resultList.add(result);
    String output = csvProducerTask.process(resultList);
    Assertions.assertEquals(expectedOutput, output.trim());
  }

  @Test
  void process_withValidResultListWithMetadata_shouldReturnValidCsvString() {
    csvProducerTask = new CsvProducerTask(true, projectedColumns, mockMetadata, columnData, ",");
    String expectedOutput =
        "9007199254740992,2147483647,true,0.000000000000000000000000000000000000000000001401298464324817,0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049,test value,YmxvYiB0ZXN0IHZhbHVl,2000-01-01,01:01:01,2000-01-01T01:01,1970-01-21T03:20:41.740Z,0.000000000000000000000000000000000000000000001401298464324817,0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049,test value,YmxvYiB0ZXN0IHZhbHVl,txt value 464654654,2147483647,2147483647,9007199254740992,9007199254740992,test value,2147483647,2147483647,9007199254740992,9007199254740992";
    Map<String, Column<?>> values = UnitTestUtils.createTestValues();
    Result result = new ResultImpl(values, mockMetadata);
    List<Result> resultList = new ArrayList<>();
    resultList.add(result);
    String output = csvProducerTask.process(resultList);
    Assertions.assertEquals(expectedOutput, output.trim());
  }

  @Test
  void process_withValidResultList_withPartialProjections_shouldReturnValidCsvString() {
    projectedColumns = UnitTestUtils.getPartialColumnsListWithoutMetadata();
    csvProducerTask = new CsvProducerTask(false, projectedColumns, mockMetadata, columnData, ",");
    String expectedOutput =
        "9007199254740992,2147483647,true,0.000000000000000000000000000000000000000000001401298464324817,0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049";
    Map<String, Column<?>> values = UnitTestUtils.createTestValues();
    Result result = new ResultImpl(values, mockMetadata);
    List<Result> resultList = new ArrayList<>();
    resultList.add(result);
    String output = csvProducerTask.process(resultList);
    System.out.println(output);
    Assertions.assertEquals(expectedOutput, output.trim());
  }

  @Test
  void
      process_withValidResultList_withPartialProjectionsAndMetadata_shouldReturnValidCsvString() {
    projectedColumns = UnitTestUtils.getPartialColumnsListWithMetadata();
    csvProducerTask = new CsvProducerTask(true, projectedColumns, mockMetadata, columnData, ",");
    String expectedOutput =
        "9007199254740992,2147483647,true,0.000000000000000000000000000000000000000000001401298464324817,0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049,0.000000000000000000000000000000000000000000001401298464324817,0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049,test value,YmxvYiB0ZXN0IHZhbHVl,txt value 464654654,2147483647,2147483647,9007199254740992,9007199254740992,test value,2147483647,2147483647,9007199254740992,9007199254740992";
    Map<String, Column<?>> values = UnitTestUtils.createTestValues();
    Result result = new ResultImpl(values, mockMetadata);
    List<Result> resultList = new ArrayList<>();
    resultList.add(result);
    String output = csvProducerTask.process(resultList);
    Assertions.assertEquals(expectedOutput, output.trim());
  }

  @Test
  void
      process_withValidResultListWithNoProjectionSpecifiedWithoutMetadata_shouldReturnValidCsvString() {
    csvProducerTask =
        new CsvProducerTask(false, Collections.emptyList(), mockMetadata, columnData, ",");
    String expectedOutput =
        "9007199254740992,2147483647,true,0.000000000000000000000000000000000000000000001401298464324817,0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049,test value,YmxvYiB0ZXN0IHZhbHVl,2000-01-01,01:01:01,2000-01-01T01:01,1970-01-21T03:20:41.740Z";
    Map<String, Column<?>> values = UnitTestUtils.createTestValues();
    Result result = new ResultImpl(values, mockMetadata);
    List<Result> resultList = new ArrayList<>();
    resultList.add(result);
    String output = csvProducerTask.process(resultList);
    Assertions.assertEquals(expectedOutput, output.trim());
  }

  @Test
  void
      process_withValidResultListWithNoProjectionSpecifiedWithMetadata_shouldReturnValidCsvString() {
    csvProducerTask =
        new CsvProducerTask(true, Collections.emptyList(), mockMetadata, columnData, ",");
    String expectedOutput =
        "9007199254740992,2147483647,true,0.000000000000000000000000000000000000000000001401298464324817,0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049,test value,YmxvYiB0ZXN0IHZhbHVl,2000-01-01,01:01:01,2000-01-01T01:01,1970-01-21T03:20:41.740Z,0.000000000000000000000000000000000000000000001401298464324817,0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049,test value,YmxvYiB0ZXN0IHZhbHVl,txt value 464654654,2147483647,2147483647,9007199254740992,9007199254740992,test value,2147483647,2147483647,9007199254740992,9007199254740992";
    Map<String, Column<?>> values = UnitTestUtils.createTestValues();
    Result result = new ResultImpl(values, mockMetadata);
    List<Result> resultList = new ArrayList<>();
    resultList.add(result);
    String output = csvProducerTask.process(resultList);
    Assertions.assertEquals(expectedOutput, output.trim());
  }
}
