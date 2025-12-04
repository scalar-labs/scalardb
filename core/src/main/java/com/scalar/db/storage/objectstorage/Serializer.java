package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class Serializer {
  private static final ObjectMapper mapper = new ObjectMapper();

  static {
    mapper
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder().maxStringLength(Integer.MAX_VALUE).build());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false);
    mapper.registerModule(new JavaTimeModule());
  }

  public static <T> T deserialize(String json, TypeReference<T> typeReference) {
    try {
      return mapper.readValue(json, typeReference);
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize the object.", e);
    }
  }

  public static <T> String serialize(T object) {
    try {
      return mapper.writeValueAsString(object);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize the object.", e);
    }
  }
}
