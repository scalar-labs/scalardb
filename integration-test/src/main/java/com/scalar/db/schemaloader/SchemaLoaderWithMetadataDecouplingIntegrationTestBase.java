package com.scalar.db.schemaloader;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public abstract class SchemaLoaderWithMetadataDecouplingIntegrationTestBase
    extends SchemaLoaderIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "schema_loader_decoupling";
  }

  @Override
  protected Map<String, Object> getSchemaJsonMap() {
    return ImmutableMap.of(
        namespace1 + "." + TABLE_1,
        ImmutableMap.<String, Object>builder()
            .put("transaction", true)
            .put("partition-key", Collections.singletonList("pk1"))
            .put("clustering-key", Arrays.asList("ck1 DESC", "ck2 ASC"))
            .put(
                "columns",
                ImmutableMap.<String, Object>builder()
                    .put("pk1", "INT")
                    .put("ck1", "INT")
                    .put("ck2", "TEXT")
                    .put("col1", "INT")
                    .put("col2", "BIGINT")
                    .put("col3", "FLOAT")
                    .put("col4", "DOUBLE")
                    .put("col5", "TEXT")
                    .put("col6", "BLOB")
                    .put("col7", "BOOLEAN")
                    .put("col8", "DATE")
                    .put("col9", "TIME")
                    .putAll(
                        isTimestampTypeSupported()
                            ? ImmutableMap.of("col10", "TIMESTAMPTZ", "col11", "TIMESTAMP")
                            : ImmutableMap.of("col10", "TIMESTAMPTZ"))
                    .build())
            .put("secondary-index", Arrays.asList("col1", "col5"))
            .put("transaction-metadata-decoupling", true)
            .build(),
        namespace2 + "." + TABLE_2,
        ImmutableMap.<String, Object>builder()
            .put("transaction", false)
            .put("partition-key", Collections.singletonList("pk1"))
            .put("clustering-key", Collections.singletonList("ck1"))
            .put(
                "columns",
                ImmutableMap.of(
                    "pk1", "INT", "ck1", "INT", "col1", "INT", "col2", "BIGINT", "col3", "FLOAT"))
            .build());
  }

  @Disabled("Repairing Transaction metadata decoupling tables is not supported")
  @Test
  @Override
  public void createTablesThenDropTablesThenRepairAllWithoutCoordinator_ShouldExecuteProperly() {}

  @Disabled("Repairing Transaction metadata decoupling tables is not supported")
  @Test
  @Override
  public void createTablesThenDropTablesThenRepairAllWithCoordinator_ShouldExecuteProperly() {}

  @Disabled("Altering Transaction metadata decoupling tables is not supported")
  @Test
  @Override
  public void createTableThenAlterTables_ShouldExecuteProperly() {}
}
