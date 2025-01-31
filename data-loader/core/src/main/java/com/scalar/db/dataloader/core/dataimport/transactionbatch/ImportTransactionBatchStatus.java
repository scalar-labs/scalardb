package com.scalar.db.dataloader.core.dataimport.transactionbatch;

import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import java.util.List;
import lombok.Builder;
import lombok.Value;

/** Batch status details */
@Builder
@Value
public class ImportTransactionBatchStatus {
  int dataChunkId;
  int transactionBatchId;
  String transactionId;
  List<ImportTaskResult> records;
  List<String> errors;
  boolean success;
}
