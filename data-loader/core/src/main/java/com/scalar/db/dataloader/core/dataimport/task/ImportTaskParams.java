package com.scalar.db.dataloader.core.dataimport.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.dataimport.ImportOptions;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDBDao;
import com.scalar.db.dataloader.core.dataimport.processor.TableColumnDataTypes;
import java.util.Map;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Builder
@Value
public class ImportTaskParams {

  @NonNull JsonNode sourceRecord;
  int dataChunkId;
  int rowNumber;
  @NonNull ImportOptions importOptions;
  @NonNull Map<String, TableMetadata> tableMetadataByTableName;
  @NonNull TableColumnDataTypes tableColumnDataTypes;
  @NonNull ScalarDBDao dao;
}
