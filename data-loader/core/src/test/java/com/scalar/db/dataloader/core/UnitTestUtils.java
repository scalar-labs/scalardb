package com.scalar.db.dataloader.core;

import static com.scalar.db.io.DataType.BIGINT;
import static com.scalar.db.io.DataType.BLOB;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFile;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileTable;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileTableFieldMapping;
import com.scalar.db.dataloader.core.dataimport.processor.TableColumnDataTypes;
import com.scalar.db.dataloader.core.util.DecimalUtil;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.transaction.consensuscommit.Attribute;
import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Utils for the service unit tests */
public class UnitTestUtils {
  public static final String TEST_NAMESPACE = "namespace";
  public static final String TEST_TABLE_NAME = "table";
  public static final String TEST_COLUMN_1_PK = "col1";
  public static final String TEST_COLUMN_2_CK = "col2";
  public static final String TEST_COLUMN_3_CK = "col3";
  public static final String TEST_COLUMN_4 = "col4";
  public static final String TEST_COLUMN_5 = "col5";
  public static final String TEST_COLUMN_6 = "col6";
  public static final String TEST_COLUMN_7 = "col7";
  public static final String TEST_COLUMN_8 = "col8";
  public static final String TEST_COLUMN_9 = "col9";
  public static final String TEST_COLUMN_10 = "col10";
  public static final String TEST_COLUMN_11 = "col11";

  public static final String TEST_VALUE_TEXT = "test value";

  public static final String TEST_VALUE_BLOB_STRING = "blob test value";
  static final byte[] TEST_VALUE_BLOB = TEST_VALUE_BLOB_STRING.getBytes(StandardCharsets.UTF_8);
  public static final String TEST_VALUE_BLOB_BASE64 =
      new String(Base64.getEncoder().encode(TEST_VALUE_BLOB), StandardCharsets.UTF_8);
  public static final String TEST_VALUE_TX_ID = "txt value 464654654";
  public static final Float TEST_VALUE_FLOAT = Float.MIN_VALUE;
  public static final int TEST_VALUE_INT = Integer.MAX_VALUE;
  public static final Long TEST_VALUE_LONG = BigIntColumn.MAX_VALUE;
  public static final boolean TEST_VALUE_BOOLEAN = true;
  public static final double TEST_VALUE_DOUBLE = Double.MIN_VALUE;
  public static final LocalDate TEST_VALUE_DATE = LocalDate.of(2000, 1, 1);
  public static final LocalTime TEST_VALUE_TIME = LocalTime.of(1, 1, 1);
  public static final LocalDateTime TEST_VALUE_DATE_TIME = LocalDateTime.of(2000, 1, 1, 1, 1);
  public static final Instant TEST_VALUE_INSTANT = Instant.ofEpochMilli(1740041740);
  public static final String TEST_CSV_DELIMITER = ";";

  public static TableMetadata createTestTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn(TEST_COLUMN_1_PK, BIGINT)
        .addColumn(TEST_COLUMN_2_CK, DataType.INT)
        .addColumn(TEST_COLUMN_3_CK, DataType.BOOLEAN)
        .addColumn(TEST_COLUMN_4, DataType.FLOAT)
        .addColumn(TEST_COLUMN_5, DataType.DOUBLE)
        .addColumn(TEST_COLUMN_6, DataType.TEXT)
        .addColumn(TEST_COLUMN_7, BLOB)
        .addColumn(TEST_COLUMN_8, DataType.DATE)
        .addColumn(TEST_COLUMN_9, DataType.TIME)
        .addColumn(TEST_COLUMN_10, DataType.TIMESTAMP)
        .addColumn(TEST_COLUMN_11, DataType.TIMESTAMPTZ)
        .addColumn(Attribute.BEFORE_PREFIX + TEST_COLUMN_4, DataType.FLOAT)
        .addColumn(Attribute.BEFORE_PREFIX + TEST_COLUMN_5, DataType.DOUBLE)
        .addColumn(Attribute.BEFORE_PREFIX + TEST_COLUMN_6, DataType.TEXT)
        .addColumn(Attribute.BEFORE_PREFIX + TEST_COLUMN_7, BLOB)
        .addColumn(Attribute.ID, DataType.TEXT)
        .addColumn(Attribute.STATE, DataType.INT)
        .addColumn(Attribute.VERSION, DataType.INT)
        .addColumn(Attribute.PREPARED_AT, BIGINT)
        .addColumn(Attribute.COMMITTED_AT, BIGINT)
        .addColumn(Attribute.BEFORE_ID, DataType.TEXT)
        .addColumn(Attribute.BEFORE_STATE, DataType.INT)
        .addColumn(Attribute.BEFORE_VERSION, DataType.INT)
        .addColumn(Attribute.BEFORE_PREPARED_AT, BIGINT)
        .addColumn(Attribute.BEFORE_COMMITTED_AT, BIGINT)
        .addPartitionKey(TEST_COLUMN_1_PK)
        .addClusteringKey(TEST_COLUMN_2_CK)
        .addClusteringKey(TEST_COLUMN_3_CK)
        .build();
  }

  public static ObjectNode getOutputDataWithMetadata() {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode rootNode = mapper.createObjectNode();
    rootNode.put(TEST_COLUMN_1_PK, TEST_VALUE_LONG);
    rootNode.put(TEST_COLUMN_2_CK, TEST_VALUE_INT);
    rootNode.put(TEST_COLUMN_3_CK, TEST_VALUE_BOOLEAN);
    rootNode.put(TEST_COLUMN_4, TEST_VALUE_FLOAT);
    rootNode.put(TEST_COLUMN_5, TEST_VALUE_DOUBLE);
    rootNode.put(TEST_COLUMN_6, TEST_VALUE_TEXT);
    rootNode.put(TEST_COLUMN_7, TEST_VALUE_BLOB);
    rootNode.put(TEST_COLUMN_8, TEST_VALUE_DATE.toString());
    rootNode.put(TEST_COLUMN_9, TEST_VALUE_TIME.toString());
    rootNode.put(TEST_COLUMN_10, TEST_VALUE_DATE_TIME.toString());
    rootNode.put(TEST_COLUMN_11, TEST_VALUE_INSTANT.toString());
    rootNode.put(Attribute.BEFORE_PREFIX + TEST_COLUMN_4, TEST_VALUE_FLOAT);
    rootNode.put(Attribute.BEFORE_PREFIX + TEST_COLUMN_5, TEST_VALUE_DOUBLE);
    rootNode.put(Attribute.BEFORE_PREFIX + TEST_COLUMN_6, TEST_VALUE_TEXT);
    rootNode.put(Attribute.BEFORE_PREFIX + TEST_COLUMN_7, TEST_VALUE_BLOB);
    rootNode.put(Attribute.ID, TEST_VALUE_TX_ID);
    rootNode.put(Attribute.STATE, TEST_VALUE_INT);
    rootNode.put(Attribute.VERSION, TEST_VALUE_INT);
    rootNode.put(Attribute.PREPARED_AT, TEST_VALUE_LONG);
    rootNode.put(Attribute.COMMITTED_AT, TEST_VALUE_LONG);
    rootNode.put(Attribute.BEFORE_ID, TEST_VALUE_TEXT);
    rootNode.put(Attribute.BEFORE_STATE, TEST_VALUE_INT);
    rootNode.put(Attribute.BEFORE_VERSION, TEST_VALUE_INT);
    rootNode.put(Attribute.BEFORE_PREPARED_AT, TEST_VALUE_LONG);
    rootNode.put(Attribute.BEFORE_COMMITTED_AT, TEST_VALUE_LONG);
    return rootNode;
  }

  public static ObjectNode getOutputDataWithoutMetadata() {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode rootNode = mapper.createObjectNode();
    rootNode.put(TEST_COLUMN_1_PK, TEST_VALUE_LONG);
    rootNode.put(TEST_COLUMN_2_CK, TEST_VALUE_INT);
    rootNode.put(TEST_COLUMN_3_CK, TEST_VALUE_BOOLEAN);
    rootNode.put(TEST_COLUMN_4, TEST_VALUE_FLOAT);
    rootNode.put(TEST_COLUMN_5, TEST_VALUE_DOUBLE);
    rootNode.put(TEST_COLUMN_6, TEST_VALUE_TEXT);
    rootNode.put(TEST_COLUMN_7, TEST_VALUE_BLOB);
    rootNode.put(TEST_COLUMN_8, TEST_VALUE_DATE.toString());
    rootNode.put(TEST_COLUMN_9, TEST_VALUE_TIME.toString());
    rootNode.put(TEST_COLUMN_10, TEST_VALUE_DATE_TIME.toString());
    rootNode.put(TEST_COLUMN_11, TEST_VALUE_INSTANT.toString());

    return rootNode;
  }

  public static List<String> getColumnsListOfMetadata() {
    List<String> projectedColumns = new ArrayList<>();
    projectedColumns.add(TEST_COLUMN_1_PK);
    projectedColumns.add(TEST_COLUMN_2_CK);
    projectedColumns.add(TEST_COLUMN_3_CK);
    projectedColumns.add(TEST_COLUMN_4);
    projectedColumns.add(TEST_COLUMN_5);
    projectedColumns.add(TEST_COLUMN_6);
    projectedColumns.add(TEST_COLUMN_7);
    projectedColumns.add(TEST_COLUMN_8);
    projectedColumns.add(TEST_COLUMN_9);
    projectedColumns.add(TEST_COLUMN_10);
    projectedColumns.add(TEST_COLUMN_11);
    projectedColumns.add(Attribute.BEFORE_PREFIX + TEST_COLUMN_4);
    projectedColumns.add(Attribute.BEFORE_PREFIX + TEST_COLUMN_5);
    projectedColumns.add(Attribute.BEFORE_PREFIX + TEST_COLUMN_6);
    projectedColumns.add(Attribute.BEFORE_PREFIX + TEST_COLUMN_7);
    projectedColumns.add(Attribute.ID);
    projectedColumns.add(Attribute.STATE);
    projectedColumns.add(Attribute.VERSION);
    projectedColumns.add(Attribute.PREPARED_AT);
    projectedColumns.add(Attribute.COMMITTED_AT);
    projectedColumns.add(Attribute.BEFORE_ID);
    projectedColumns.add(Attribute.BEFORE_STATE);
    projectedColumns.add(Attribute.BEFORE_VERSION);
    projectedColumns.add(Attribute.BEFORE_PREPARED_AT);
    projectedColumns.add(Attribute.BEFORE_COMMITTED_AT);
    return projectedColumns;
  }

  public static Map<String, DataType> getColumnData() {
    Map<String, DataType> columnData = new HashMap<>();
    columnData.put(TEST_COLUMN_1_PK, BIGINT);
    columnData.put(TEST_COLUMN_2_CK, DataType.INT);
    columnData.put(TEST_COLUMN_3_CK, DataType.BOOLEAN);
    columnData.put(TEST_COLUMN_4, DataType.FLOAT);
    columnData.put(TEST_COLUMN_5, DataType.DOUBLE);
    columnData.put(TEST_COLUMN_6, DataType.TEXT);
    columnData.put(TEST_COLUMN_7, BLOB);
    columnData.put(TEST_COLUMN_8, DataType.DATE);
    columnData.put(TEST_COLUMN_9, DataType.TIME);
    columnData.put(TEST_COLUMN_10, DataType.TIMESTAMP);
    columnData.put(TEST_COLUMN_11, DataType.TIMESTAMPTZ);
    columnData.put(Attribute.BEFORE_PREFIX + TEST_COLUMN_4, DataType.FLOAT);
    columnData.put(Attribute.BEFORE_PREFIX + TEST_COLUMN_5, DataType.DOUBLE);
    columnData.put(Attribute.BEFORE_PREFIX + TEST_COLUMN_6, DataType.TEXT);
    columnData.put(Attribute.BEFORE_PREFIX + TEST_COLUMN_7, BLOB);
    columnData.put(Attribute.ID, DataType.TEXT);
    columnData.put(Attribute.STATE, DataType.INT);
    columnData.put(Attribute.VERSION, DataType.INT);
    columnData.put(Attribute.PREPARED_AT, BIGINT);
    columnData.put(Attribute.COMMITTED_AT, BIGINT);
    columnData.put(Attribute.BEFORE_ID, DataType.TEXT);
    columnData.put(Attribute.BEFORE_STATE, DataType.INT);
    columnData.put(Attribute.BEFORE_VERSION, DataType.INT);
    columnData.put(Attribute.BEFORE_PREPARED_AT, BIGINT);
    columnData.put(Attribute.BEFORE_COMMITTED_AT, BIGINT);
    return columnData;
  }

  public static Map<String, Column<?>> createTestValues() {
    Map<String, Column<?>> values = new HashMap<>();
    values.put(TEST_COLUMN_1_PK, BigIntColumn.of(TEST_COLUMN_1_PK, TEST_VALUE_LONG));
    values.put(TEST_COLUMN_2_CK, IntColumn.of(TEST_COLUMN_2_CK, TEST_VALUE_INT));
    values.put(TEST_COLUMN_3_CK, BooleanColumn.of(TEST_COLUMN_3_CK, TEST_VALUE_BOOLEAN));
    values.put(TEST_COLUMN_4, FloatColumn.of(TEST_COLUMN_4, TEST_VALUE_FLOAT));
    values.put(TEST_COLUMN_5, DoubleColumn.of(TEST_COLUMN_5, TEST_VALUE_DOUBLE));
    values.put(TEST_COLUMN_6, TextColumn.of(TEST_COLUMN_6, TEST_VALUE_TEXT));
    values.put(TEST_COLUMN_7, BlobColumn.of(TEST_COLUMN_7, TEST_VALUE_BLOB));
    values.put(TEST_COLUMN_8, DateColumn.of(TEST_COLUMN_8, TEST_VALUE_DATE));
    values.put(TEST_COLUMN_9, TimeColumn.of(TEST_COLUMN_9, TEST_VALUE_TIME));
    values.put(TEST_COLUMN_10, TimestampColumn.of(TEST_COLUMN_10, TEST_VALUE_DATE_TIME));
    values.put(TEST_COLUMN_11, TimestampTZColumn.of(TEST_COLUMN_11, TEST_VALUE_INSTANT));
    values.put(
        Attribute.BEFORE_PREFIX + TEST_COLUMN_4,
        FloatColumn.of(Attribute.BEFORE_PREFIX + TEST_COLUMN_4, TEST_VALUE_FLOAT));
    values.put(
        Attribute.BEFORE_PREFIX + TEST_COLUMN_5,
        DoubleColumn.of(Attribute.BEFORE_PREFIX + TEST_COLUMN_5, TEST_VALUE_DOUBLE));
    values.put(
        Attribute.BEFORE_PREFIX + TEST_COLUMN_6,
        TextColumn.of(Attribute.BEFORE_PREFIX + TEST_COLUMN_6, TEST_VALUE_TEXT));
    values.put(
        Attribute.BEFORE_PREFIX + TEST_COLUMN_7,
        BlobColumn.of(Attribute.BEFORE_PREFIX + TEST_COLUMN_7, TEST_VALUE_BLOB));
    values.put(Attribute.ID, TextColumn.of(Attribute.ID, TEST_VALUE_TX_ID));
    values.put(Attribute.STATE, IntColumn.of(Attribute.STATE, TEST_VALUE_INT));
    values.put(Attribute.VERSION, IntColumn.of(Attribute.VERSION, TEST_VALUE_INT));
    values.put(Attribute.PREPARED_AT, BigIntColumn.of(Attribute.PREPARED_AT, TEST_VALUE_LONG));
    values.put(Attribute.COMMITTED_AT, BigIntColumn.of(Attribute.COMMITTED_AT, TEST_VALUE_LONG));
    values.put(Attribute.BEFORE_ID, TextColumn.of(Attribute.BEFORE_ID, TEST_VALUE_TEXT));
    values.put(Attribute.BEFORE_STATE, IntColumn.of(Attribute.BEFORE_STATE, TEST_VALUE_INT));
    values.put(Attribute.BEFORE_VERSION, IntColumn.of(Attribute.BEFORE_VERSION, TEST_VALUE_INT));
    values.put(
        Attribute.BEFORE_PREPARED_AT,
        BigIntColumn.of(Attribute.BEFORE_PREPARED_AT, TEST_VALUE_LONG));
    values.put(
        Attribute.BEFORE_COMMITTED_AT,
        BigIntColumn.of(Attribute.BEFORE_COMMITTED_AT, TEST_VALUE_LONG));
    return values;
  }

  public static String getSourceTestValue(DataType dataType) {
    switch (dataType) {
      case INT:
        return Integer.toString(TEST_VALUE_INT);
      case BIGINT:
        return Long.toString(TEST_VALUE_LONG);
      case FLOAT:
        return DecimalUtil.convertToNonScientific(TEST_VALUE_FLOAT);
      case DOUBLE:
        return DecimalUtil.convertToNonScientific(TEST_VALUE_DOUBLE);
      case BLOB:
        return TEST_VALUE_BLOB_BASE64;
      case BOOLEAN:
        return Boolean.toString(TEST_VALUE_BOOLEAN);
      case DATE:
        return TEST_VALUE_DATE.toString();
      case TIME:
        return TEST_VALUE_TIME.toString();
      case TIMESTAMP:
        return TEST_VALUE_DATE_TIME.toString();
      case TIMESTAMPTZ:
        return TEST_VALUE_INSTANT.toString();
      case TEXT:
      default:
        return TEST_VALUE_TEXT;
    }
  }

  public static TableColumnDataTypes getTableColumnData() {
    TableColumnDataTypes tableColumnDataTypes = new TableColumnDataTypes();
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put("namespace.table", createTestTableMetadata());
    tableMetadataMap.forEach(
        (name, metadata) ->
            metadata
                .getColumnDataTypes()
                .forEach((k, v) -> tableColumnDataTypes.addColumnDataType(name, k, v)));
    return tableColumnDataTypes;
  }

  public static ControlFile getControlFile() {
    List<ControlFileTable> controlFileTables = new ArrayList<>();
    List<ControlFileTableFieldMapping> mappings = new ArrayList<>();
    mappings.add(new ControlFileTableFieldMapping("col1", "col1"));
    mappings.add(new ControlFileTableFieldMapping("col2", "col2"));
    mappings.add(new ControlFileTableFieldMapping("col3", "col3"));
    mappings.add(new ControlFileTableFieldMapping("col4", "col4"));
    mappings.add(new ControlFileTableFieldMapping("col5", "col5"));
    mappings.add(new ControlFileTableFieldMapping("col6", "col6"));
    mappings.add(new ControlFileTableFieldMapping("col7", "col7"));
    mappings.add(new ControlFileTableFieldMapping("col8", "col8"));
    mappings.add(new ControlFileTableFieldMapping("col9", "col9"));
    mappings.add(new ControlFileTableFieldMapping("col10", "col10"));
    mappings.add(new ControlFileTableFieldMapping("col11", "col11"));
    mappings.add(new ControlFileTableFieldMapping("col11", "col11"));
    controlFileTables.add(new ControlFileTable("namespace", "table", mappings));
    return new ControlFile(controlFileTables);
  }

  public static BufferedReader getJsonReader() {
    String jsonData =
        "[{\"col1\":1,\"col2\":\"1\",\"col3\":\"1\",\"col4\":\"1.4e-45\",\"col5\":\"5e-324\",\"col6\":\"VALUE!!s\",\"col7\":\"0x626C6F6220746573742076616C7565\",\"col8\":\"2000-01-01\",\"col9\":\"01:01:01.000000\",\"col10\":\"2000-01-01T01:01:00\",\"col11\":\"1970-01-21T03:20:41.740Z\"}]";
    return new BufferedReader(new StringReader(jsonData));
  }

  public static BufferedReader getJsonLinesReader() {
    String jsonLinesData =
        "{\"col1\":1,\"col2\":\"1\",\"col3\":\"1\",\"col4\":\"1.4e-45\",\"col5\":\"5e-324\",\"col6\":\"VALUE!!s\",\"col7\":\"0x626C6F6220746573742076616C7565\",\"col8\":\"2000-01-01\",\"col9\":\"01:01:01.000000\",\"col10\":\"2000-01-01T01:01:00\",\"col11\":\"1970-01-21T03:20:41.740Z\"}\n";
    return new BufferedReader(new StringReader(jsonLinesData));
  }

  public static BufferedReader getCsvReader() {

    String csvData =
        "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11 \n"
            + "1,1,1,1.4E-45,5e-324,VALUE!!s,0x626C6F6220746573742076616C7565,2000-01-01,01:01:01.000000,2000-01-01T01:01:00,1970-01-21T03:20:41.740Z  \n"
            + "2,1,1,1.4E-45,5e-324,VALUE!!s,0x626C6F6220746573742076616C7565,2000-01-01,01:01:01.000000,2000-01-01T01:01:00,1970-01-21T03:20:41.740Z  \n";
    return new BufferedReader(new StringReader(csvData));
  }

  public static Key getClusteringKey() {
    return Key.newBuilder()
        .add(IntColumn.of("col2", 1))
        .add(BooleanColumn.of("col3", true))
        .build();
  }

  public static Key getPartitionKey(int j) {
    return Key.ofBigInt("col1", j);
  }

  public static Optional<Result> getResult(long pk) {
    Result data = new ResultImpl(createTestValues(), createTestTableMetadata());
    return Optional.of(data);
  }
}
