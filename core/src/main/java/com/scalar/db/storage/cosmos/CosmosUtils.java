package com.scalar.db.storage.cosmos;

public final class CosmosUtils {
  public static String quoteKeyword(String keyword) {
    return "[\"" + keyword + "\"]";
  }
}
