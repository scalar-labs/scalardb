package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Serializer {
  public static final int MAX_STRING_LENGTH_ALLOWED = Integer.MAX_VALUE;
  private static final ObjectMapper jsonMapper = new ObjectMapper();

  static {
    configureMapper();
  }

  private static void configureMapper() {
    Serializer.jsonMapper
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder().maxStringLength(MAX_STRING_LENGTH_ALLOWED).build());
    Serializer.jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    Serializer.jsonMapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false);
    Serializer.jsonMapper.registerModule(new JavaTimeModule());
  }

  public static <T> T deserialize(byte[] data, TypeReference<T> typeReference) {
    try {
      // GZIP-compressed JSON format
      byte[] decompressed = decompress(data);
      return jsonMapper.readValue(new String(decompressed, StandardCharsets.UTF_8), typeReference);
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize the object.", e);
    }
  }

  public static <T> byte[] serialize(T object) {
    try {
      return compress(jsonMapper.writeValueAsBytes(object));
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize the object.", e);
    }
  }

  private static byte[] compress(byte[] data) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
    try (GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
      gzip.write(data);
    }
    return bos.toByteArray();
  }

  private static byte[] decompress(byte[] data) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    try (GZIPInputStream gzip = new GZIPInputStream(bis)) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      byte[] buffer = new byte[1024];
      int len;
      while ((len = gzip.read(buffer)) != -1) {
        bos.write(buffer, 0, len);
      }
      return bos.toByteArray();
    }
  }
}
