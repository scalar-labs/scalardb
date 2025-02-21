package com.scalar.db.dataloader.core.dataexport.producer;

import static com.scalar.db.dataloader.core.ErrorMessage.FILE_FORMAT_NOT_SUPPORTED;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.io.DataType;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ProducerTaskFactory {

  private final String delimiter;
  private final boolean includeMetadata;
  private final boolean prettyPrintJson;

  /**
   * Create a producer task object based on file format
   *
   * @param fileFormat file format
   * @param projectionColumns columns names that are selected
   * @param tableMetadata metadata of the table
   * @param dataTypeByColumnName map of columns with data types
   * @return producer task object of provided file format
   */
  public ProducerTask createProducerTask(
      FileFormat fileFormat,
      List<String> projectionColumns,
      TableMetadata tableMetadata,
      Map<String, DataType> dataTypeByColumnName) {
    ProducerTask producerTask;
    switch (fileFormat) {
      case JSON:
        producerTask =
            new JsonProducerTask(
                includeMetadata,
                projectionColumns,
                tableMetadata,
                dataTypeByColumnName,
                prettyPrintJson);
        break;
      case JSONL:
        producerTask =
            new JsonLineProducerTask(
                includeMetadata, projectionColumns, tableMetadata, dataTypeByColumnName);
        break;
      case CSV:
        producerTask =
            new CsvProducerTask(
                includeMetadata, projectionColumns, tableMetadata, dataTypeByColumnName, delimiter);
        break;
      default:
        throw new IllegalArgumentException(String.format(FILE_FORMAT_NOT_SUPPORTED, fileFormat));
    }
    return producerTask;
  }
}
