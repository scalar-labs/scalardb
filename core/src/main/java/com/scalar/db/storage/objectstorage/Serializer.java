package com.scalar.db.storage.objectstorage;

import org.apache.fory.Fory;
import org.apache.fory.ThreadSafeFory;
import org.apache.fory.config.Language;

public class Serializer {
  private static final ThreadSafeFory fory =
      Fory.builder()
          .withLanguage(Language.JAVA)
          .requireClassRegistration(false)
          .withNumberCompressed(true)
          .withStringCompressed(true)
          .buildThreadSafeFory();

  @SuppressWarnings("unchecked")
  public static <T> T deserialize(byte[] data) {
    try {
      return (T) fory.deserialize(data);
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize the object.", e);
    }
  }

  public static <T> byte[] serialize(T object) {
    try {
      return fory.serialize(object);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize the object.", e);
    }
  }
}
