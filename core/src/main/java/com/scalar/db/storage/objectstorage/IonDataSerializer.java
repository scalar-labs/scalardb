package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.ion.IonFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class IonDataSerializer implements ObjectStorageDataSerializer {
  private static final ObjectMapper mapper =
      new ObjectMapper(IonFactory.builderForBinaryWriters().build());

  static {
    mapper
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder()
                .maxStringLength(Serializer.MAX_STRING_LENGTH_ALLOWED)
                .build());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false);
    mapper.registerModule(new JavaTimeModule());
  }

  @Override
  public byte[] serialize(ObjectStoragePartition partition) {
    try {
      return mapper.writeValueAsBytes(partition);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize the partition to Ion.", e);
    }
  }

  @Override
  public ObjectStoragePartition deserialize(byte[] data) {
    try {
      return mapper.readValue(data, new TypeReference<ObjectStoragePartition>() {});
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize the partition from Ion.", e);
    }
  }
}
