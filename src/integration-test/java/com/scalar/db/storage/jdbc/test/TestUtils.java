package com.scalar.db.storage.jdbc.test;

import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;

import java.util.Optional;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclose;
import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;

public final class TestUtils {
  private TestUtils() {}

  public static String getMetadataFullSchema(
      Optional<String> namespacePrefix, RdbEngine rdbEngine) {
    return enclose(namespacePrefix.orElse("") + TableMetadataManager.SCHEMA, rdbEngine);
  }

  public static String getMetadataFullTableName(
      Optional<String> namespacePrefix, RdbEngine rdbEngine) {
    return enclosedFullTableName(
        namespacePrefix.orElse("") + TableMetadataManager.SCHEMA,
        TableMetadataManager.TABLE,
        rdbEngine);
  }
}
