package com.scalar.db.storage.objectstorage;

public interface ObjectStorageDataSerializer {
  byte[] serialize(ObjectStoragePartition partition);

  ObjectStoragePartition deserialize(byte[] data);
}
