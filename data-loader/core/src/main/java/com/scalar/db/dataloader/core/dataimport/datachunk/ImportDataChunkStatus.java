package com.scalar.db.dataloader.core.dataimport.datachunk;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.time.Instant;
import lombok.Builder;
import lombok.Data;

/** * A DTO to store import data chunk details */
@Data
@Builder
@JsonDeserialize(builder = ImportDataChunkStatus.ImportDataChunkStatusBuilder.class)
public class ImportDataChunkStatus {

  @JsonProperty("dataChunkId")
  private final int dataChunkId;

  @JsonProperty("startTime")
  private final Instant startTime;

  @JsonProperty("endTime")
  private final Instant endTime;

  @JsonProperty("totalRecords")
  private final int totalRecords;

  @JsonProperty("successCount")
  private final int successCount;

  @JsonProperty("failureCount")
  private final int failureCount;

  @JsonProperty("batchCount")
  private final int batchCount;

  @JsonProperty("totalDurationInMilliSeconds")
  private final int totalDurationInMilliSeconds;

  @JsonProperty("status")
  private final ImportDataChunkStatusState status;
}
