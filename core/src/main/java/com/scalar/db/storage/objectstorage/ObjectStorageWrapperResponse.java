package com.scalar.db.storage.objectstorage;

import java.util.Arrays;

public class ObjectStorageWrapperResponse {
  private final byte[] payload;
  private final String version;

  public ObjectStorageWrapperResponse(byte[] payload, String version) {
    this.payload = Arrays.copyOf(payload, payload.length);
    this.version = version;
  }

  public byte[] getPayload() {
    return Arrays.copyOf(payload, payload.length);
  }

  public String getVersion() {
    return version;
  }
}
