package com.scalar.db.dataloader.core.dataexport;

import lombok.Builder;
import lombok.Getter;

/**
 * A Data Transfer Object (DTO) representing the result of processing a data chunk.
 *
 * <p>This class encapsulates:
 *
 * <ul>
 *   <li><strong>count</strong> - the number of results processed within the data chunk.
 *   <li><strong>processedDataChunkOutput</strong> - the processed data as a string, intended to be
 *       written to an output file.
 * </ul>
 */
@Getter
@Builder(builderMethodName = "hiddenBuilder")
public class DataChunkProcessResult {
  private long count;
  private String processedDataChunkOutput;
}
