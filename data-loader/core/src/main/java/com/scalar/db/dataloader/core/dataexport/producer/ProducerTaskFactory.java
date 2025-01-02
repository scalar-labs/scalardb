package com.scalar.db.dataloader.core.dataexport.producer;

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

  /***
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
    return switch (fileFormat) {
      case JSON ->
          new JsonProducerTask(
              includeMetadata,
              projectionColumns,
              tableMetadata,
              dataTypeByColumnName,
              prettyPrintJson);
      case JSONL ->
          new JsonLineProducerTask(
              includeMetadata, projectionColumns, tableMetadata, dataTypeByColumnName);
      case CSV ->
          new CsvProducerTask(
              includeMetadata, projectionColumns, tableMetadata, dataTypeByColumnName, delimiter);
    };
  }
}
