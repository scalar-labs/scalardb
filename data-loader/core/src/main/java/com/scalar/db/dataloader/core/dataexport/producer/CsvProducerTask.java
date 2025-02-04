package com.scalar.db.dataloader.core.dataexport.producer;

import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.dataexport.ExportReport;
import com.scalar.db.dataloader.core.util.CsvUtil;
import com.scalar.db.dataloader.core.util.DecimalUtil;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Producer that converts ScalarDB scan results to csv content. The output is sent to a queue to be
 * processed by a consumer
 */
public class CsvProducerTask extends ProducerTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(CsvProducerTask.class);

  private final String delimiter;

  /**
   * Class constructor
   *
   * @param includeMetadata Include metadata in the exported data
   * @param tableMetadata Metadata for a single ScalarDB table
   * @param columnDataTypes Map of data types for the all columns in a ScalarDB table
   * @param delimiter Delimiter used in csv content
   */
  public CsvProducerTask(
      boolean includeMetadata,
      List<String> projectColumns,
      TableMetadata tableMetadata,
      Map<String, DataType> columnDataTypes,
      String delimiter) {
    super(includeMetadata, projectColumns, tableMetadata, columnDataTypes);
    this.delimiter = delimiter;
  }

  /**
   * * Process scalardb scan result data and returns CSV data
   *
   * @param dataChunk list of results
   * @param exportReport export report
   * @return result converted to string
   */
  @Override
  public String process(List<Result> dataChunk, ExportReport exportReport) {
    StringBuilder csvContent = new StringBuilder();
    for (Result result : dataChunk) {
      String csvRow = convertResultToCsv(result);
      csvContent.append(csvRow);
      exportReport.increaseExportedRowCount();
    }
    return csvContent.toString();
  }

  /**
   * Convert a ScalarDB scan result to CSV
   *
   * @param result ScalarDB scan result
   * @return CSV string
   */
  private String convertResultToCsv(Result result) {
    // Initialization
    StringBuilder stringBuilder = new StringBuilder();
    LinkedHashSet<String> tableColumnNames = tableMetadata.getColumnNames();
    Iterator<String> iterator = tableColumnNames.iterator();

    try {
      // Loop over the result data list
      while (iterator.hasNext()) {
        String columnName = iterator.next();

        // Skip the field if it can be ignored based on check
        boolean columnNotProjected = !projectedColumnsSet.contains(columnName);
        boolean isMetadataColumn =
            ConsensusCommitUtils.isTransactionMetaColumn(columnName, tableMetadata);
        if (columnNotProjected || (!includeMetadata && isMetadataColumn)) {
          continue;
        }

        // Convert each value to a string value and add to the StringBuilder
        stringBuilder.append(
            convertToString(result, columnName, dataTypeByColumnName.get(columnName)));

        if (iterator.hasNext()) {
          stringBuilder.append(delimiter);
        }
      }

      // Double check and remove the character if it's a delimiter. This can occur when the last
      // added column was not the last iterator field and did get a delimiter
      CsvUtil.removeTrailingDelimiter(stringBuilder, delimiter);

      stringBuilder.append(System.lineSeparator());

      return stringBuilder.toString();
    } catch (UnsupportedOperationException e) {
      LOGGER.error(
          CoreError.DATA_LOADER_VALUE_TO_STRING_CONVERSION_FAILED.buildMessage(e.getMessage()));
    }
    return "";
  }

  /**
   * * Convert result column value to string
   *
   * @param result scalardb result
   * @param columnName column name
   * @param dataType datatype of the column
   * @return value of result converted to string
   */
  private String convertToString(Result result, String columnName, DataType dataType) {
    if (result.isNull(columnName)) {
      return null;
    }
    String value = "";
    switch (dataType) {
      case INT:
        value = Integer.toString(result.getInt(columnName));
        break;
      case BIGINT:
        value = Long.toString(result.getBigInt(columnName));
        break;
      case FLOAT:
        value = DecimalUtil.convertToNonScientific(result.getFloat(columnName));
        break;
      case DOUBLE:
        value = DecimalUtil.convertToNonScientific(result.getDouble(columnName));
        break;
      case BLOB:
        byte[] encoded = Base64.getEncoder().encode(result.getBlobAsBytes(columnName));
        value = new String(encoded, Charset.defaultCharset());
        break;
      case BOOLEAN:
        value = Boolean.toString(result.getBoolean(columnName));
        break;
      case TEXT:
        value = result.getText(columnName);
        break;
      default:
        break;
    }
    return value;
  }
}
