package com.scalar.db.dataloader.core.dataimport.task.result;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.dataloader.core.dataimport.task.ImportTaskAction;
import java.util.List;
import lombok.Builder;
import lombok.Value;

/** To store import target result. */
@SuppressWarnings({"SameNameButDifferent", "MissingSummary"})
@Builder
@Value
public class ImportTargetResult {
  String namespace;
  String tableName;
  ImportTaskAction importAction;
  List<String> errors;
  boolean dataMapped;
  JsonNode importedRecord;
  ImportTargetResultStatus status;
}
