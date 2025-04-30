package com.scalar.db.dataloader.core.dataimport.datachunk;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Value;

/** Stores data related to a single row on import file. */
@SuppressWarnings("SameNameButDifferent")
@Value
public class ImportRow {
  int rowNumber;
  JsonNode sourceData;
}
