package com.scalar.db.dataloader.core.tablemetadata;

import lombok.Getter;

/** Represents the request for metadata for a single ScalarDB table */
@Getter
public class TableMetadataRequest {

  private final String namespace;
  private final String tableName;

  /**
   * Class constructor
   *
   * @param namespace ScalarDB namespace
   * @param tableName ScalarDB table name
   */
  public TableMetadataRequest(String namespace, String tableName) {
    this.namespace = namespace;
    this.tableName = tableName;
  }
}
