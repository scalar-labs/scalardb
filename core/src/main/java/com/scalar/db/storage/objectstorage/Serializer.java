package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;

public class Serializer {
  public static final int MAX_STRING_LENGTH_ALLOWED = Integer.MAX_VALUE;
  private static final ObjectMapper jsonMapper = new ObjectMapper();

  static {
    jsonMapper
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder().maxStringLength(MAX_STRING_LENGTH_ALLOWED).build());
    jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    jsonMapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false);
    jsonMapper.registerModule(new JavaTimeModule());
  }

  public static <T> T deserialize(byte[] data, TypeReference<T> typeReference) {
    try {
      if (data.length > 0 && data[0] == '{') {
        // JSON format (backward compatibility)
        return jsonMapper.readValue(new String(data, StandardCharsets.UTF_8), typeReference);
      }
      // Java Serializable format
      try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
        @SuppressWarnings("unchecked")
        T result = (T) ois.readObject();
        return result;
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize the object.", e);
    }
  }

  public static <T> byte[] serialize(T object) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
        oos.writeObject(object);
      }
      return baos.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize the object.", e);
    }
  }
}
