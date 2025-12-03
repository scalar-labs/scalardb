package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorageVirtualTablesIntegrationTestBase;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.VirtualTableJoinType;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDatabaseVirtualTablesIntegrationTest
    extends DistributedStorageVirtualTablesIntegrationTestBase {

  private JdbcAdminImportTestUtils testUtils;
  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    testUtils = new JdbcAdminImportTestUtils(properties);
    return properties;
  }

  @DisabledIf("com.scalar.db.storage.jdbc.JdbcEnv#isSqlite")
  @Test
  public void createVirtualTable_WithImportedTableHavingVariousPrimaryKeyTypes_ShouldWorkProperly()
      throws Exception {
    for (Map.Entry<String, DataType> entry :
        testUtils.getSupportedDataTypeMapForPrimaryKey().entrySet()) {
      String dataTypeName = entry.getKey();
      DataType dataType = entry.getValue();
      String tableBaseName =
          dataTypeName.replaceAll("[()]", "").replaceAll("[\\s,]", "_").toLowerCase();
      String importedTableName = tableBaseName + "_imported";
      String anotherTableName = tableBaseName + "_another";
      String vtableInnerTableName = tableBaseName + "_vtable_inner";
      String vtableLeftOuterTableName = tableBaseName + "_vtable_left_outer";

      String createTableSql =
          "CREATE TABLE "
              + rdbEngine.encloseFullTableName(namespace, importedTableName)
              + " ("
              + rdbEngine.enclose("pk")
              + " "
              + dataTypeName
              + " PRIMARY KEY"
              + (JdbcEnv.isDb2() ? " NOT NULL" : "")
              + ","
              + rdbEngine.enclose("col1")
              + " VARCHAR(100))";

      try {
        // Create a left source table to be imported
        testUtils.execute(createTableSql);

        // Import the left source table
        admin.importTable(namespace, importedTableName, Collections.emptyMap());

        // Create a right source table
        admin.createTable(
            namespace,
            anotherTableName,
            TableMetadata.newBuilder()
                .addColumn("pk", dataType)
                .addColumn("col2", DataType.TEXT)
                .addPartitionKey("pk")
                .build());

        // Create a virtual table that joins the above two source tables with different join types
        admin.createVirtualTable(
            namespace,
            vtableInnerTableName,
            namespace,
            importedTableName,
            namespace,
            anotherTableName,
            VirtualTableJoinType.INNER);
        admin.createVirtualTable(
            namespace,
            vtableLeftOuterTableName,
            namespace,
            importedTableName,
            namespace,
            anotherTableName,
            VirtualTableJoinType.LEFT_OUTER);

        // Verify that the virtual tables are created successfully
        TableMetadata expectedMetadata =
            TableMetadata.newBuilder()
                .addColumn("pk", dataType)
                .addColumn("col1", DataType.TEXT)
                .addColumn("col2", DataType.TEXT)
                .addPartitionKey("pk")
                .build();
        assertThat(admin.getTableMetadata(namespace, vtableInnerTableName))
            .isEqualTo(expectedMetadata);
        assertThat(admin.getTableMetadata(namespace, vtableLeftOuterTableName))
            .isEqualTo(expectedMetadata);

        // Put data into the virtual table
        Key partitionKey1 = getPartitionKey(dataType, 1);
        Key partitionKey2 = getPartitionKey(dataType, 2);
        storage.put(
            Put.newBuilder()
                .namespace(namespace)
                .table(vtableInnerTableName)
                .partitionKey(partitionKey1)
                .textValue("col1", "value1")
                .textValue("col2", "value2")
                .build());
        storage.put(
            Put.newBuilder()
                .namespace(namespace)
                .table(vtableInnerTableName)
                .partitionKey(partitionKey2)
                .textValue("col1", "value3")
                .textValue("col2", "value4")
                .build());

        // Scan data from the virtual table and verify
        try (Scanner scanner =
            storage.scan(
                Scan.newBuilder().namespace(namespace).table(vtableInnerTableName).all().build())) {
          List<Result> results = scanner.all();
          assertThat(results).hasSize(2);

          // Verify results in any order
          assertThat(results)
              .anySatisfy(
                  result -> {
                    Assertions.<Key>assertThat(
                            ScalarDbUtils.getPartitionKey(result, expectedMetadata))
                        .isEqualTo(partitionKey1);
                    assertThat(result.getText("col1")).isEqualTo("value1");
                    assertThat(result.getText("col2")).isEqualTo("value2");
                  })
              .anySatisfy(
                  result -> {
                    Assertions.<Key>assertThat(
                            ScalarDbUtils.getPartitionKey(result, expectedMetadata))
                        .isEqualTo(partitionKey2);
                    assertThat(result.getText("col1")).isEqualTo("value3");
                    assertThat(result.getText("col2")).isEqualTo("value4");
                  });
        }
      } finally {
        // Drop the created tables
        admin.dropTable(namespace, vtableInnerTableName, true);
        admin.dropTable(namespace, vtableLeftOuterTableName, true);
        admin.dropTable(namespace, anotherTableName, true);
        admin.dropTable(namespace, importedTableName, true);
      }
    }
  }

  private Key getPartitionKey(DataType dataType, int index) {
    switch (dataType) {
      case BOOLEAN:
        return Key.ofBoolean("pk", index == 1);
      case INT:
        return Key.ofInt("pk", index);
      case BIGINT:
        return Key.ofBigInt("pk", index);
      case FLOAT:
        return Key.ofFloat("pk", (float) index);
      case DOUBLE:
        return Key.ofDouble("pk", index);
      case TEXT:
        return Key.ofText("pk", String.valueOf(index * 100));
      default:
        throw new AssertionError("Unsupported data type: " + dataType);
    }
  }
}
