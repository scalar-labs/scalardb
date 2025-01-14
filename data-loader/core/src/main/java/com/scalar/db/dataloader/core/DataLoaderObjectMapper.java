package com.scalar.db.dataloader.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class DataLoaderObjectMapper extends ObjectMapper {

  public DataLoaderObjectMapper() {
    super();
    this.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    this.registerModule(new JavaTimeModule());
  }
}
