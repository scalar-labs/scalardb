package com.scalar.db.dataloader.core.dataimport.transactionbatch;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import java.util.List;
import lombok.Builder;
import lombok.Value;

/** Transaction batch result */
@Builder
@Value
@JsonDeserialize(builder = ImportTransactionBatchResult.ImportTransactionBatchResultBuilder.class)
public class ImportTransactionBatchResult {
  @JsonProperty("dataChunkId")
  int dataChunkId;

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
}
