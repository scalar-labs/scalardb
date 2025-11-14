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

  /**
   * The metadata of the table from which the data is being exported. Used to understand schema
   * details such as column types and primary keys.
   */
  protected final TableMetadata tableMetadata;

  /**
   * A map of column names to their corresponding data types. Derived from the table metadata to
   * assist in formatting output correctly.
   */
  protected final Map<String, DataType> dataTypeByColumnName;

  /**
   * A set of column names to include in the exported output. If empty, all columns are included by
   * default.
   */
  protected final Set<String> projectedColumnsSet;

  /**
   * Class constructor
   *
   * @param projectionColumns List of column name for projection
   * @param tableMetadata Metadata of the ScalarDB table
   * @param columnDataTypes Map of data types for the all columns in a ScalarDB table
   */
  protected ProducerTask(
      List<String> projectionColumns,
      TableMetadata tableMetadata,
      Map<String, DataType> columnDataTypes) {
    this.projectedColumnsSet = new HashSet<>(projectionColumns);
    this.tableMetadata = tableMetadata;
    this.dataTypeByColumnName = columnDataTypes;
  }

  /**
   * Processes a chunk of export data and returns a formatted string representation of the chunk.
   *
   * @param dataChunk the list of {@link Result} objects representing a chunk of data to be exported
   * @return a formatted string representing the processed data chunk, ready to be written to the
   *     output
   */
  public abstract String process(List<Result> dataChunk);
}
