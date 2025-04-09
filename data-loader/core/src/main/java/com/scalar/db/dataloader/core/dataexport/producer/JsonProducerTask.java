package com.scalar.db.dataloader.core.dataexport.producer;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.DataLoaderObjectMapper;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonProducerTask extends ProducerTask {

  private final DataLoaderObjectMapper objectMapper = new DataLoaderObjectMapper();
  private final boolean prettyPrintJson;
  private static final Logger logger = LoggerFactory.getLogger(JsonProducerTask.class);

  /**
   * Class constructor
   *
   * @param includeMetadata Include metadata in the exported data
   * @param tableMetadata Metadata for a single ScalarDB table
   * @param columnDataTypes Map of data types for the all columns in a ScalarDB table
   */
  public JsonProducerTask(
      boolean includeMetadata,
      List<String> projectionColumns,
      TableMetadata tableMetadata,
      Map<String, DataType> columnDataTypes,
      boolean prettyPrintJson) {
    super(includeMetadata, projectionColumns, tableMetadata, columnDataTypes);
    this.prettyPrintJson = prettyPrintJson;
  }

  /**
   * Process ScalarDB scan result data and returns CSV data
   *
   * @param dataChunk list of results
   * @return result converted to string
   */
  @Override
  public String process(List<Result> dataChunk) {
    ArrayNode arrayNode = objectMapper.createArrayNode();

    for (Result result : dataChunk) {
      ObjectNode objectNode = generateJsonForResult(result);
      arrayNode.add(objectNode);
    }

    if (prettyPrintJson) {
      String json = arrayNode.toPrettyString();
      return json.substring(1, json.length() - 1);
    }

    String json = arrayNode.toString();
    // Remove the [] from the json string
    return json.substring(1, json.length() - 1);
  }

  /**
   * Generate a Json Object based on a ScalarDB Result
   *
   * @param result ScalarDB Result object instance
   * @return JsonObject containing the ScalarDB result data
   */
  private ObjectNode generateJsonForResult(Result result) {
    LinkedHashSet<String> tableColumns = tableMetadata.getColumnNames();

    ObjectNode objectNode = objectMapper.createObjectNode();

    // Loop through all the columns and to the json object
    for (String columnName : tableColumns) {
      // Skip the field if it can be ignored based on check
      boolean columnNotProjected = !projectedColumnsSet.contains(columnName);
      boolean isMetadataColumn =
          ConsensusCommitUtils.isTransactionMetaColumn(columnName, tableMetadata);
      if (columnNotProjected || (!includeMetadata && isMetadataColumn)) {
        continue;
      }

      DataType dataType = dataTypeByColumnName.get(columnName);
      addToObjectNode(objectNode, result, columnName, dataType);
    }
    return objectNode;
  }

  /**
   * Add result column name and value to json object node
   *
   * @param result ScalarDB result
   * @param columnName column name
   * @param dataType datatype of the column
   */
  private void addToObjectNode(
      ObjectNode objectNode, Result result, String columnName, DataType dataType) {

    if (result.isNull(columnName)) {
      return;
    }

    switch (dataType) {
      case BOOLEAN:
        objectNode.put(columnName, result.getBoolean(columnName));
        break;
      case INT:
        objectNode.put(columnName, result.getInt(columnName));
        break;
      case BIGINT:
        objectNode.put(columnName, result.getBigInt(columnName));
        break;
      case FLOAT:
        objectNode.put(columnName, result.getFloat(columnName));
        break;
      case DOUBLE:
        objectNode.put(columnName, result.getDouble(columnName));
        break;
      case TEXT:
        objectNode.put(columnName, result.getText(columnName));
        break;
      case BLOB:
        // convert to base64 string
        byte[] encoded = Base64.getEncoder().encode(result.getBlobAsBytes(columnName));
        objectNode.put(columnName, new String(encoded, Charset.defaultCharset()));
        break;
      case DATE:
        LocalDate date = result.getDate(columnName);
        assert date != null;
        objectNode.put(columnName, date.toString());
        break;
      case TIME:
        LocalTime time = result.getTime(columnName);
        assert time != null;
        objectNode.put(columnName, time.toString());
        break;
      case TIMESTAMP:
        LocalDateTime localDateTime = result.getTimestamp(columnName);
        assert localDateTime != null;
        objectNode.put(columnName, localDateTime.toString());
        break;
      case TIMESTAMPTZ:
        Instant instant = result.getTimestampTZ(columnName);
        assert instant != null;
        objectNode.put(columnName, instant.toString());
        break;
      default:
        throw new AssertionError("Unknown data type:" + dataType);
    }
  }
}
