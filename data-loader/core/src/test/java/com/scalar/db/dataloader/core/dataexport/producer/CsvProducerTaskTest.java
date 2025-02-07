package com.scalar.db.dataloader.core.dataexport.producer;

import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.dataloader.core.UnitTestUtils;
import com.scalar.db.dataloader.core.dataexport.DataChunkProcessResult;
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
    DataChunkProcessResult output = csvProducerTask.process(results);
    Assertions.assertEquals("", output.getProcessedDataChunkOutput());
  }

  @Test
  void process_withValidResultList_shouldReturnValidCsvString() {
    String expectedOutput =
        "9007199254740992,2147483647,true,0.000000000000000000000000000000000000000000001401298464324817,0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049,test value,YmxvYiB0ZXN0IHZhbHVl";
    Map<String, Column<?>> values = UnitTestUtils.createTestValues();
    Result result = new ResultImpl(values, mockMetadata);
    List<Result> resultList = new ArrayList<>();
    resultList.add(result);
    DataChunkProcessResult output = csvProducerTask.process(resultList);
    Assertions.assertEquals(expectedOutput, output.getProcessedDataChunkOutput().trim());
  }

  @Test
  void process_withValidResultListWithMetadata_shouldReturnValidCsvString() {
    csvProducerTask = new CsvProducerTask(true, projectedColumns, mockMetadata, columnData, ",");
    String expectedOutput =
        "9007199254740992,2147483647,true,0.000000000000000000000000000000000000000000001401298464324817,0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049,test value,YmxvYiB0ZXN0IHZhbHVl,0.000000000000000000000000000000000000000000001401298464324817,0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049,test value,YmxvYiB0ZXN0IHZhbHVl,txt value 464654654,2147483647,2147483647,9007199254740992,9007199254740992,test value,2147483647,2147483647,9007199254740992,9007199254740992";
    Map<String, Column<?>> values = UnitTestUtils.createTestValues();
    Result result = new ResultImpl(values, mockMetadata);
    List<Result> resultList = new ArrayList<>();
    resultList.add(result);
    DataChunkProcessResult output = csvProducerTask.process(resultList);
    Assertions.assertEquals(expectedOutput, output.getProcessedDataChunkOutput().trim());
    Assertions.assertEquals(1L, output.getCount());
  }
}
