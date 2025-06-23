package com.scalar.db.dataloader.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * A custom {@link ObjectMapper} used for data loading operations.
 *
 * <p>This mapper is configured to:
 *
 * <ul>
 *   <li>Exclude {@code null} values during serialization
 *   <li>Support Java 8 date/time types via {@link JavaTimeModule}
 * </ul>
 *
 * <p>It can be reused wherever consistent JSON serialization/deserialization behavior is needed.
 */
public class DataLoaderObjectMapper extends ObjectMapper {

  /**
   * Constructs a {@code DataLoaderObjectMapper} with default settings, including:
   *
   * <ul>
   *   <li>{@link com.fasterxml.jackson.annotation.JsonInclude.Include#NON_NULL} to skip {@code
   *       null} values
   *   <li>{@link JavaTimeModule} registration to handle Java 8 date/time types
   * </ul>
   */
  public DataLoaderObjectMapper() {
    super();
    this.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    this.registerModule(new JavaTimeModule());
  }
}
