package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.charset.StandardCharsets;

public class Serializer {
  public static final int MAX_STRING_LENGTH_ALLOWED = Integer.MAX_VALUE;
  private static final ObjectMapper jsonMapper = new ObjectMapper();
  private static final ObjectMapper cborMapper = new ObjectMapper(new CBORFactory());

  static {
    configureMapper(jsonMapper);
    configureMapper(cborMapper);
  }

  private static void configureMapper(ObjectMapper mapper) {
    mapper
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder().maxStringLength(MAX_STRING_LENGTH_ALLOWED).build());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false);
    mapper.registerModule(new JavaTimeModule());
  }

  public static <T> T deserialize(String json, TypeReference<T> typeReference) {
    try {
      return jsonMapper.readValue(json, typeReference);
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize the object.", e);
    }
  }

  public static <T> T deserialize(byte[] data, TypeReference<T> typeReference) {
    try {
      if (data.length > 0 && data[0] == '{') {
        // JSON format (backward compatibility)
        return jsonMapper.readValue(new String(data, StandardCharsets.UTF_8), typeReference);
      }
      // CBOR format
      return cborMapper.readValue(data, typeReference);
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize the object.", e);
    }
  }

  public static <T> String serialize(T object) {
    try {
      return jsonMapper.writeValueAsString(object);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize the object.", e);
    }
  }

  public static <T> byte[] serializeAsBytes(T object) {
    try {
      return cborMapper.writeValueAsBytes(object);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize the object.", e);
    }
  }
}
