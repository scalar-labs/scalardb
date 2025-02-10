package com.scalar.db.dataloader.core.dataimport.task;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ImportTaskConstants {
  public static final String ERROR_COULD_NOT_FIND_PARTITION_KEY =
      "could not find the partition key";
  public static final String ERROR_UPSERT_INSERT_MISSING_COLUMNS =
      "the source record needs to contain all fields if the UPSERT turns into an INSERT";
  public static final String ERROR_DATA_ALREADY_EXISTS = "record already exists";
  public static final String ERROR_DATA_NOT_FOUND = "record was not found";
  public static final String ERROR_COULD_NOT_FIND_CLUSTERING_KEY =
      "could not find the clustering key";
  public static final String ERROR_TABLE_METADATA_MISSING = "No table metadata found";
}
