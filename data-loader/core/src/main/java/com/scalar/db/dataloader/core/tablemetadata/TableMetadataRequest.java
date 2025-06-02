package com.scalar.db.dataloader.core.tablemetadata;

import lombok.Getter;

/** Represents the request for metadata for a single ScalarDB table */
@SuppressWarnings("SameNameButDifferent")
@Getter
public class TableMetadataRequest {

  private final String namespace;
  private final String table;

  /**
   * Class constructor
   *
   * @param namespace ScalarDB namespace
   * @param table ScalarDB table name
   */
  public TableMetadataRequest(String namespace, String table) {
    this.namespace = namespace;
    this.table = table;
  }
}
