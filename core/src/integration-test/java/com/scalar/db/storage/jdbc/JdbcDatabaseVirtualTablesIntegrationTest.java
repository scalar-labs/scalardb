package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.DistributedStorageVirtualTablesIntegrationTestBase;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.VirtualTableJoinType;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDatabaseVirtualTablesIntegrationTest
    extends DistributedStorageVirtualTablesIntegrationTestBase {

  private static final Logger logger =
      LoggerFactory.getLogger(JdbcDatabaseVirtualTablesIntegrationTest.class);

  private static final String LEFT_OUTER_LEFT_SOURCE_TABLE = "left_outer_left_source_table";
  private static final String LEFT_OUTER_RIGHT_SOURCE_TABLE = "left_outer_right_source_table";
  private static final String LEFT_OUTER_VIRTUAL_TABLE = "left_outer_virtual_table";

  private JdbcAdminImportTestUtils testUtils;
  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    if (testUtils == null) {
      testUtils = new JdbcAdminImportTestUtils(properties);
    }
    return properties;
  }

  @AfterAll
  @Override
  public void afterAll() {
    try {
      super.afterAll();
    } catch (Exception e) {
      logger.warn("Failed to call super.afterAll", e);
    }

    try {
      if (testUtils != null) {
        testUtils.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close test utils", e);
    }
  }

  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
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

        // Create a right source table whose TEXT key takes the collation of the imported table's
        // key. On Oracle 23, joining a USING_NLS_COMP key to a BINARY key failed with ORA-43915,
        // "unable to determine collation: arguments have conflicting collations", see
        // https://docs.oracle.com/en/error-help/db/ora-43915/
        admin.createTable(
            namespace,
            anotherTableName,
            TableMetadata.newBuilder()
                .addColumn("pk", dataType)
                .addColumn("col2", DataType.TEXT)
                .addPartitionKey("pk")
                .build(),
            Collections.singletonMap(JdbcAdmin.COLLATION_SOURCE_TABLE, importedTableName));

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

  @Test
  public void
      put_ForLeftOuterVirtualTableSettingAllRightColumnsToNull_ShouldDeleteRightSourceRecord()
          throws Exception {
    try {
      // Arrange
      createLeftOuterVirtualTable();
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(LEFT_OUTER_VIRTUAL_TABLE)
              .partitionKey(Key.ofInt("pk", 1))
              .textValue("col1", "value1")
              .textValue("col2", "value2")
              .intValue("col3", 3)
              .build());

      // Act
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(LEFT_OUTER_VIRTUAL_TABLE)
              .partitionKey(Key.ofInt("pk", 1))
              .textValue("col1", "value1")
              .textValue("col2", null)
              .intValue("col3", null)
              .build());

      // Assert
      assertThat(getFromLeftOuterRightSourceTable(1)).isEmpty();
      Optional<Result> result = getFromLeftOuterVirtualTable(1);
      assertThat(result).isPresent();
      assertThat(result.get().getText("col1")).isEqualTo("value1");
      assertThat(result.get().isNull("col2")).isTrue();
      assertThat(result.get().isNull("col3")).isTrue();
    } finally {
      dropLeftOuterVirtualTable();
    }
  }

  @Test
  public void put_ForLeftOuterVirtualTableWithoutRightColumns_ShouldNotCreateRightSourceRecord()
      throws Exception {
    try {
      // Arrange
      createLeftOuterVirtualTable();

      // Act
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(LEFT_OUTER_VIRTUAL_TABLE)
              .partitionKey(Key.ofInt("pk", 1))
              .textValue("col1", "value1")
              .build());

      // Assert
      assertThat(getFromLeftOuterRightSourceTable(1)).isEmpty();
      Optional<Result> result = getFromLeftOuterVirtualTable(1);
      assertThat(result).isPresent();
      assertThat(result.get().getText("col1")).isEqualTo("value1");
      assertThat(result.get().isNull("col2")).isTrue();
      assertThat(result.get().isNull("col3")).isTrue();
    } finally {
      dropLeftOuterVirtualTable();
    }
  }

  @Test
  public void
      putIf_ForLeftOuterVirtualTableWithIsNullConditionsOnRightColumns_WhenRecordDoesNotExist_ShouldThrowNoMutationException()
          throws Exception {
    try {
      // Arrange
      createLeftOuterVirtualTable();
      Put putWithLeftColumns =
          Put.newBuilder()
              .namespace(namespace)
              .table(LEFT_OUTER_VIRTUAL_TABLE)
              .partitionKey(Key.ofInt("pk", 1))
              .textValue("col1", "value1")
              .textValue("col2", "value2")
              .condition(
                  ConditionBuilder.putIf(ConditionBuilder.column("col2").isNullText()).build())
              .build();
      Put putWithoutLeftColumns =
          Put.newBuilder()
              .namespace(namespace)
              .table(LEFT_OUTER_VIRTUAL_TABLE)
              .partitionKey(Key.ofInt("pk", 2))
              .textValue("col2", "value2")
              .condition(
                  ConditionBuilder.putIf(ConditionBuilder.column("col2").isNullText()).build())
              .build();

      // Act Assert
      assertThatThrownBy(() -> storage.put(putWithLeftColumns))
          .isInstanceOf(NoMutationException.class);
      assertThatThrownBy(() -> storage.put(putWithoutLeftColumns))
          .isInstanceOf(NoMutationException.class);
      assertThat(getFromLeftOuterLeftSourceTable(1)).isEmpty();
      assertThat(getFromLeftOuterRightSourceTable(1)).isEmpty();
      assertThat(getFromLeftOuterLeftSourceTable(2)).isEmpty();
      assertThat(getFromLeftOuterRightSourceTable(2)).isEmpty();
    } finally {
      dropLeftOuterVirtualTable();
    }
  }

  @Test
  public void
      putIf_ForLeftOuterVirtualTableWithIsNullConditionsOnRightColumns_WhenOnlyLeftSourceRecordExists_ShouldPutProperly()
          throws Exception {
    try {
      // Arrange
      createLeftOuterVirtualTable();
      for (int pk = 1; pk <= 2; pk++) {
        storage.put(
            Put.newBuilder()
                .namespace(namespace)
                .table(LEFT_OUTER_VIRTUAL_TABLE)
                .partitionKey(Key.ofInt("pk", pk))
                .textValue("col1", "value1")
                .build());
      }

      // Act
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(LEFT_OUTER_VIRTUAL_TABLE)
              .partitionKey(Key.ofInt("pk", 1))
              .textValue("col1", "value1-2")
              .textValue("col2", "value2")
              .intValue("col3", 3)
              .condition(
                  ConditionBuilder.putIf(ConditionBuilder.column("col2").isNullText()).build())
              .build());
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(LEFT_OUTER_VIRTUAL_TABLE)
              .partitionKey(Key.ofInt("pk", 2))
              .textValue("col2", "value2")
              .intValue("col3", 3)
              .condition(
                  ConditionBuilder.putIf(ConditionBuilder.column("col2").isNullText()).build())
              .build());

      // Assert
      Optional<Result> result1 = getFromLeftOuterVirtualTable(1);
      assertThat(result1).isPresent();
      assertThat(result1.get().getText("col1")).isEqualTo("value1-2");
      assertThat(result1.get().getText("col2")).isEqualTo("value2");
      assertThat(result1.get().getInt("col3")).isEqualTo(3);
      Optional<Result> result2 = getFromLeftOuterVirtualTable(2);
      assertThat(result2).isPresent();
      assertThat(result2.get().getText("col1")).isEqualTo("value1");
      assertThat(result2.get().getText("col2")).isEqualTo("value2");
      assertThat(result2.get().getInt("col3")).isEqualTo(3);
    } finally {
      dropLeftOuterVirtualTable();
    }
  }

  private void createLeftOuterVirtualTable() throws ExecutionException {
    admin.createTable(
        namespace,
        LEFT_OUTER_LEFT_SOURCE_TABLE,
        TableMetadata.newBuilder()
            .addColumn("pk", DataType.INT)
            .addColumn("col1", DataType.TEXT)
            .addPartitionKey("pk")
            .build(),
        true);
    admin.createTable(
        namespace,
        LEFT_OUTER_RIGHT_SOURCE_TABLE,
        TableMetadata.newBuilder()
            .addColumn("pk", DataType.INT)
            .addColumn("col2", DataType.TEXT)
            .addColumn("col3", DataType.INT)
            .addPartitionKey("pk")
            .build(),
        true);
    admin.createVirtualTable(
        namespace,
        LEFT_OUTER_VIRTUAL_TABLE,
        namespace,
        LEFT_OUTER_LEFT_SOURCE_TABLE,
        namespace,
        LEFT_OUTER_RIGHT_SOURCE_TABLE,
        VirtualTableJoinType.LEFT_OUTER,
        true);
  }

  private void dropLeftOuterVirtualTable() throws ExecutionException {
    admin.dropTable(namespace, LEFT_OUTER_VIRTUAL_TABLE, true);
    admin.dropTable(namespace, LEFT_OUTER_RIGHT_SOURCE_TABLE, true);
    admin.dropTable(namespace, LEFT_OUTER_LEFT_SOURCE_TABLE, true);
  }

  private Optional<Result> getFromLeftOuterVirtualTable(int pk) throws ExecutionException {
    return get(LEFT_OUTER_VIRTUAL_TABLE, pk);
  }

  private Optional<Result> getFromLeftOuterLeftSourceTable(int pk) throws ExecutionException {
    return get(LEFT_OUTER_LEFT_SOURCE_TABLE, pk);
  }

  private Optional<Result> getFromLeftOuterRightSourceTable(int pk) throws ExecutionException {
    return get(LEFT_OUTER_RIGHT_SOURCE_TABLE, pk);
  }

  private Optional<Result> get(String table, int pk) throws ExecutionException {
    return storage.get(
        Get.newBuilder()
            .namespace(namespace)
            .table(table)
            .partitionKey(Key.ofInt("pk", pk))
            .build());
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
