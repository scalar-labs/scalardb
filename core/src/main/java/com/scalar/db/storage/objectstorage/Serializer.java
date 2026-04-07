package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import de.undercouch.bson4jackson.BsonFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Serializer {
  public static final int MAX_STRING_LENGTH_ALLOWED = Integer.MAX_VALUE;
  private static final ObjectMapper bsonMapper = new ObjectMapper(new BsonFactory());

  static {
    bsonMapper
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder().maxStringLength(MAX_STRING_LENGTH_ALLOWED).build());
    bsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    bsonMapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false);
    bsonMapper.registerModule(new JavaTimeModule());
  }

  public static <T> T deserialize(byte[] data, TypeReference<T> typeReference) {
    try {
      return bsonMapper.readValue(decompress(data), typeReference);
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize the object.", e);
    }
  }

  public static <T> byte[] serialize(T object) {
    try {
      return compress(bsonMapper.writeValueAsBytes(object));
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize the object.", e);
    }
  }

  private static byte[] compress(byte[] data) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
      gzos.write(data);
    }
    return baos.toByteArray();
  }

  private static byte[] decompress(byte[] data) throws IOException {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
        GZIPInputStream gzis = new GZIPInputStream(bais);
        ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[1024];
      int len;
      while ((len = gzis.read(buffer)) != -1) {
        baos.write(buffer, 0, len);
      }
      return baos.toByteArray();
    }
  }
}
