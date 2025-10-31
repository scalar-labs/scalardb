package com.scalar.db.storage.objectstorage;

public class ObjectStorageWrapperResponse {
  private final String payload;
  private final String version;

  public ObjectStorageWrapperResponse(String payload, String version) {
    this.payload = payload;
    this.version = version;
  }

  public String getPayload() {
    return payload;
  }

  public String getVersion() {
    return version;
  }
}
