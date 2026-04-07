package com.scalar.db.storage.objectstorage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCompressingSerializer implements ObjectStorageDataSerializer {
  private final ObjectStorageDataSerializer delegate;

  public GzipCompressingSerializer(ObjectStorageDataSerializer delegate) {
    this.delegate = delegate;
  }

  @Override
  public byte[] serialize(ObjectStoragePartition partition) {
    byte[] raw = delegate.serialize(partition);
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
        gzipOut.write(raw);
      }
      return baos.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException("Failed to compress the serialized partition.", e);
    }
  }

  @Override
  public ObjectStoragePartition deserialize(byte[] data) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(data);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (GZIPInputStream gzipIn = new GZIPInputStream(bais)) {
        byte[] buffer = new byte[8192];
        int len;
        while ((len = gzipIn.read(buffer)) != -1) {
          baos.write(buffer, 0, len);
        }
      }
      return delegate.deserialize(baos.toByteArray());
    } catch (Exception e) {
      throw new RuntimeException("Failed to decompress and deserialize the partition.", e);
    }
  }
}
