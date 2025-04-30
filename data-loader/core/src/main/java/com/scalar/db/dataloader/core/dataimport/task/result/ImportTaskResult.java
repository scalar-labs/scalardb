package com.scalar.db.dataloader.core.dataimport.task.result;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import lombok.Builder;
import lombok.Value;

@SuppressWarnings({"SameNameButDifferent", "MissingSummary"})
@Builder
@Value
@JsonDeserialize(builder = ImportTaskResult.ImportTaskResultBuilder.class)
public class ImportTaskResult {
  @JsonProperty("rowNumber")
  int rowNumber;

  @JsonProperty("targets")
  List<ImportTargetResult> targets;

  @JsonProperty("rawRecord")
  JsonNode rawRecord;

  @JsonProperty("dataChunkId")
  int dataChunkId;
}
