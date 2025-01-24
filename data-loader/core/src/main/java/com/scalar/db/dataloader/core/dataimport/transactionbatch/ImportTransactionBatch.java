package com.scalar.db.dataloader.core.dataimport.transactionbatch;

import com.scalar.db.dataloader.core.dataimport.datachunk.ImportRow;
import java.util.List;
import lombok.Builder;
import lombok.Value;

/** Transaction batch details */
@Builder
@Value
public class ImportTransactionBatch {
  int transactionBatchId;
  List<ImportRow> sourceData;
}
