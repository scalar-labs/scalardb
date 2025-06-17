package com.scalar.db.dataloader.core.dataimport.datachunk;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.time.Instant;
import lombok.Builder;
import lombok.Data;

/** A DTO to store import data chunk details. */
@SuppressWarnings({"SameNameButDifferent", "MissingSummary"})
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

  /**
   * Explicit builder class declaration required for Javadoc generation.
   *
   * <p>This class is normally generated automatically by Lombok's {@code @Builder} annotation.
   * However, when using a custom builder method name (e.g., {@code hiddenBuilder()}), Javadoc may
   * fail to resolve references to this builder unless it is explicitly declared.
   */
  public static class ImportDataChunkStatusBuilder {}
}
