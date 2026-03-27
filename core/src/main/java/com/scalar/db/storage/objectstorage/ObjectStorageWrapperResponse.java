package com.scalar.db.storage.objectstorage;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ObjectStorageWrapperResponse {
  private final byte[] payload;
  private final String version;

  public ObjectStorageWrapperResponse(byte[] payload, String version) {
    this.payload = payload;
    this.version = version;
  }

  public byte[] getPayload() {
    return payload;
  }

  public String getVersion() {
    return version;
  }
}
