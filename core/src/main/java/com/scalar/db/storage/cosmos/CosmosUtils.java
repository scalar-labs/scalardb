package com.scalar.db.storage.cosmos;

public class CosmosUtils {
  public static String quoteKeyword(String keyword) {
    return "[\"" + keyword + "\"]";
  }
}
