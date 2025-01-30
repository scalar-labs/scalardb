package com.scalar.db.dataloader.core.dataimport.processor;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.ScalarDBMode;
import com.scalar.db.dataloader.core.dataimport.ImportOptions;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDBDao;
import java.util.Map;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class ImportProcessorParams {
  ScalarDBMode scalarDBMode;
  ImportOptions importOptions;
  Map<String, TableMetadata> tableMetadataByTableName;
  TableColumnDataTypes tableColumnDataTypes;
  ScalarDBDao dao;
  DistributedStorage distributedStorage;
  DistributedTransactionManager distributedTransactionManager;
}
