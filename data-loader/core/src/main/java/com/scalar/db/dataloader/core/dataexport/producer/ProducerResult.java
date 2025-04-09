package com.scalar.db.dataloader.core.dataexport.producer;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class ProducerResult {
  JsonNode jsonNode;
  String csvSource;
  boolean poisonPill;
}
