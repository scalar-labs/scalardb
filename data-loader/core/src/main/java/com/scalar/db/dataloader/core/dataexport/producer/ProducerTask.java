package com.scalar.db.dataloader.core.dataexport.producer;

import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.dataexport.ExportReport;
import com.scalar.db.io.DataType;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ProducerTask {

  protected final TableMetadata tableMetadata;
  protected final Map<String, DataType> dataTypeByColumnName;
  protected final boolean includeMetadata;
  protected final Set<String> projectedColumnsSet;

  /**
   * Class constructor
   *
   * @param includeMetadata Include metadata in the exported data
   * @param projectionColumns List of column name for projection
   * @param tableMetadata Metadata of the ScalarDB table
   * @param columnDataTypes Map of data types for the all columns in a ScalarDB table
   */
  protected ProducerTask(
      boolean includeMetadata,
      List<String> projectionColumns,
      TableMetadata tableMetadata,
      Map<String, DataType> columnDataTypes) {
    this.includeMetadata = includeMetadata;
    this.projectedColumnsSet = new HashSet<>(projectionColumns);
    this.tableMetadata = tableMetadata;
    this.dataTypeByColumnName = columnDataTypes;
  }

  public abstract String process(List<Result> dataChunk, ExportReport exportReport);
}
