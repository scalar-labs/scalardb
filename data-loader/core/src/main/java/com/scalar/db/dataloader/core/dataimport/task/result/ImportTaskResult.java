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

  /**
   * Explicit builder class declaration required for Javadoc generation.
   *
   * <p>This class is normally generated automatically by Lombok's {@code @Builder} annotation.
   * However, when using a custom builder method name (e.g., {@code hiddenBuilder()}), Javadoc may
   * fail to resolve references to this builder unless it is explicitly declared.
   */
  public static class ImportTaskResultBuilder {}
}
