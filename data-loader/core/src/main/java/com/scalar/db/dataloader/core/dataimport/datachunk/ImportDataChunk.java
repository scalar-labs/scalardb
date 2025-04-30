package com.scalar.db.dataloader.core.dataimport.datachunk;

import java.util.List;
import lombok.Builder;
import lombok.Data;

/** * Import data chunk data. */
@SuppressWarnings({"SameNameButDifferent", "MissingSummary"})
@Data
@Builder
public class ImportDataChunk {

  int dataChunkId;
  List<ImportRow> sourceData;
}
