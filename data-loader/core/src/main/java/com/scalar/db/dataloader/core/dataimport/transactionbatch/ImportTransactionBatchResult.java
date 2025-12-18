package com.scalar.db.dataloader.core.dataimport.transactionbatch;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import java.util.List;
import lombok.Builder;
import lombok.Value;

/** Transaction batch result. */
@SuppressWarnings({"SameNameButDifferent", "MissingSummary"})
@Builder
@Value
@JsonDeserialize(builder = ImportTransactionBatchResult.ImportTransactionBatchResultBuilder.class)
public class ImportTransactionBatchResult {
  @JsonProperty("transactionBatchId")
  int transactionBatchId;

  @JsonProperty("transactionId")
  String transactionId;

  @JsonProperty("records")
  List<ImportTaskResult> records;

  @JsonProperty("errors")
  List<String> errors;

  @JsonProperty("success")
  boolean success;

  /**
   * Explicit builder class declaration required for Javadoc generation.
   *
   * <p>This class is normally generated automatically by Lombok's {@code @Builder} annotation.
   * However, when using a custom builder method name (e.g., {@code hiddenBuilder()}), Javadoc may
   * fail to resolve references to this builder unless it is explicitly declared.
   */
  public static class ImportTransactionBatchResultBuilder {}
}
