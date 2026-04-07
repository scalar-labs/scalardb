package com.scalar.db.storage.objectstorage;

import javax.annotation.Nullable;

public class ObjectStorageDataSerializerFactory {

  public static ObjectStorageDataSerializer create(
      ObjectStorageFormat format, ObjectStorageCompression compression) {
    return create(format, compression, null);
  }

  public static ObjectStorageDataSerializer create(
      ObjectStorageFormat format,
      ObjectStorageCompression compression,
      @Nullable ObjectStorageTableMetadata metadata) {
    switch (format) {
      case CBOR:
        return wrapWithCompression(new CborDataSerializer(), compression);
      case FORY:
        return wrapWithCompression(new ForyDataSerializer(metadata), compression);
      case ION:
        return wrapWithCompression(new IonDataSerializer(), compression);
      case JSON:
        return wrapWithCompression(new JsonDataSerializer(), compression);
      case SMILE:
        return wrapWithCompression(new SmileDataSerializer(), compression);
      default:
        throw new IllegalArgumentException("Unsupported format: " + format);
    }
  }

  private static ObjectStorageDataSerializer wrapWithCompression(
      ObjectStorageDataSerializer serializer, ObjectStorageCompression compression) {
    switch (compression) {
      case GZIP:
        return new GzipCompressingSerializer(serializer);
      case ZSTD:
        return new ZstdCompressingSerializer(serializer);
      case NONE:
      default:
        return serializer;
    }
  }
}
