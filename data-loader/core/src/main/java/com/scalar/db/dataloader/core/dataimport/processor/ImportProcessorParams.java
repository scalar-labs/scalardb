package com.scalar.db.dataloader.core.dataimport.processor;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.ScalarDbMode;
import com.scalar.db.dataloader.core.dataimport.ImportOptions;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDao;
import java.util.Map;
import lombok.Builder;
import lombok.Value;

/**
 * Parameters class for the import processor containing all necessary components for data import
 * operations.
 *
 * <p>This class is immutable and uses the Builder pattern for construction. It encapsulates all
 * required parameters and dependencies for processing data imports in ScalarDB.
 */
@Builder
@Value
public class ImportProcessorParams {
  /** The operational mode of ScalarDB (transaction or storage mode). */
  ScalarDbMode scalarDbMode;

  /** Configuration options for the import operation. */
  ImportOptions importOptions;

  /** Mapping of table names to their corresponding metadata definitions. */
  Map<String, TableMetadata> tableMetadataByTableName;

  /** Data type information for table columns. */
  TableColumnDataTypes tableColumnDataTypes;

  /** Data Access Object for ScalarDB operations. */
  ScalarDbDao dao;

  /** Storage interface for non-transactional operations. */
  DistributedStorage distributedStorage;

  /** Transaction manager for handling transactional operations. */
  DistributedTransactionManager distributedTransactionManager;
}
