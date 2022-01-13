package com.scalar.db.storage.cosmos;

public final class CosmosUtils {

  private CosmosUtils() {}

  public static String quoteKeyword(String keyword) {
    return "[\"" + keyword + "\"]";
  }
}
