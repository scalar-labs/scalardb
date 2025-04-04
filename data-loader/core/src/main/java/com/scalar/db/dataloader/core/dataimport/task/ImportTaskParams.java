package com.scalar.db.dataloader.core.dataimport.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.dataimport.ImportOptions;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDao;
import com.scalar.db.dataloader.core.dataimport.processor.TableColumnDataTypes;
import java.util.Map;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Parameters required for executing an import task in the data loader. This class encapsulates all
 * necessary information needed to process and import a single record into ScalarDB.
 */
@Builder
@Value
public class ImportTaskParams {

  /** The source record to be imported, represented as a JSON node */
  @NonNull JsonNode sourceRecord;

  /** Identifier for the current chunk of data being processed */
  int dataChunkId;

  /** The row number of the current record in the source data */
  int rowNumber;

  /** Configuration options for the import process */
  @NonNull ImportOptions importOptions;

  /** Mapping of table names to their corresponding metadata */
  @NonNull Map<String, TableMetadata> tableMetadataByTableName;

  /** Data type information for table columns */
  @NonNull TableColumnDataTypes tableColumnDataTypes;

  /** Data Access Object for interacting with ScalarDB */
  @NonNull ScalarDbDao dao;
}
