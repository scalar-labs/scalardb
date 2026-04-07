package com.scalar.db.storage.objectstorage;

import com.github.luben.zstd.Zstd;

public class ZstdCompressingSerializer implements ObjectStorageDataSerializer {
  private final ObjectStorageDataSerializer delegate;

  public ZstdCompressingSerializer(ObjectStorageDataSerializer delegate) {
    this.delegate = delegate;
  }

  @Override
  public byte[] serialize(ObjectStoragePartition partition) {
    byte[] raw = delegate.serialize(partition);
    try {
      return Zstd.compress(raw);
    } catch (Exception e) {
      throw new RuntimeException("Failed to compress the serialized partition with Zstd.", e);
    }
  }

  @Override
  public ObjectStoragePartition deserialize(byte[] data) {
    try {
      long decompressedSize = Zstd.decompressedSize(data);
      byte[] decompressed = new byte[(int) decompressedSize];
      Zstd.decompress(decompressed, data);
      return delegate.deserialize(decompressed);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to decompress and deserialize the partition with Zstd.", e);
    }
  }
}
