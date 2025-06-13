package com.scalar.db.dataloader.core.dataexport.producer;

import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An abstract base class for producer tasks that process chunks of data retrieved from a ScalarDB
 * table.
 *
 * <p>Subclasses are expected to implement the {@link #process(List)} method, which transforms a
 * chunk of {@link Result} objects into a specific format (e.g., CSV, JSON).
 *
 * <p>This class manages metadata and column projection logic that can be used by all concrete
 * implementations.
 */
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

  public abstract String process(List<Result> dataChunk);
}
