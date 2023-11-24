package com.scalar.db.schemaloader;

import com.google.gson.JsonObject;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ImportTableSchema {
  private static final String TRANSACTION = "transaction";
  private final String namespace;
  private final String tableName;
  private final boolean isTransactionTable;

  public ImportTableSchema(String tableFullName, JsonObject tableDefinition)
      throws SchemaLoaderException {
    String[] fullName = tableFullName.split("\\.", -1);
    if (fullName.length != 2) {
      throw new SchemaLoaderException(
          "Parsing the schema JSON failed. Table full name must contains namespace and table: "
              + tableFullName);
    }
    namespace = fullName[0];
    tableName = fullName[1];
    if (tableDefinition.keySet().contains(TRANSACTION)) {
      isTransactionTable = tableDefinition.get(TRANSACTION).getAsBoolean();
    } else {
      isTransactionTable = true;
    }
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public String getNamespace() {
    return namespace;
  }

  public String getTable() {
    return tableName;
  }

  public boolean isTransactionTable() {
    return isTransactionTable;
  }
}
