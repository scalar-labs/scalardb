package com.scalar.db.schemaloader;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

public abstract class SchemaLoaderImportWithMetadataDecouplingIntegrationTestBase
    extends SchemaLoaderImportIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "schema_loader_import_decoupling";
  }

  @Override
  protected Map<String, Object> getImportSchemaJsonMap() {
    return ImmutableMap.of(
        namespace1 + "." + TABLE_1,
        ImmutableMap.<String, Object>builder()
            .put("transaction", true)
            .put("override-columns-type", getImportableTableOverrideColumnsType())
            .put("transaction_metadata_decoupling", true)
            .build(),
        namespace2 + "." + TABLE_2,
        ImmutableMap.<String, Object>builder().put("transaction", false).build());
  }

  @Override
  protected String getImportedTableName1() {
    return super.getImportedTableName1() + "_scalardb";
  }
}
