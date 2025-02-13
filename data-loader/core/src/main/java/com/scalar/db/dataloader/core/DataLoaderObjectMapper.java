package com.scalar.db.dataloader.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * A custom {@link ObjectMapper} for data loading purposes.
 *
 * <p>This class configures the Jackson {@link ObjectMapper} to:
 *
 * <ul>
 *   <li>Exclude {@code null} values from serialization.
 *   <li>Register the {@link JavaTimeModule} to handle Java 8 date/time types.
 * </ul>
 */
public class DataLoaderObjectMapper extends ObjectMapper {

  public DataLoaderObjectMapper() {
    super();
    this.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    this.registerModule(new JavaTimeModule());
  }
}
