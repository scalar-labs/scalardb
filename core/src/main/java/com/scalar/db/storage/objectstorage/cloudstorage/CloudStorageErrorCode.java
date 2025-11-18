package com.scalar.db.storage.objectstorage.cloudstorage;

enum CloudStorageErrorCode {
  NOT_FOUND(404),
  PRECONDITION_FAILED(412);

  private final int code;

  CloudStorageErrorCode(int code) {
    this.code = code;
  }

  public int get() {
    return this.code;
  }
}
