package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import java.util.Collections;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class JdbcAdminCollationIntegrationTest {
  private static final String NAMESPACE = "collation_ns";
  private static final String TABLE = "collation_tbl";
  private static final String TEXT_PK = "pk";
  private static final String TEXT_PLAIN = "plain";

  private Properties properties;
  private RdbEngineStrategy rdbEngine;
  private DistributedStorageAdmin admin;
  private JdbcAdminTestUtils testUtils;

  @SuppressWarnings("unused")
  private static boolean isNotMysqlOrPostgresqlFamily() {
    return JdbcEnv.isOracle()
        || JdbcEnv.isSqlServer()
        || JdbcEnv.isSqlite()
        || JdbcEnv.isDb2()
        || JdbcEnv.isSpanner();
  }

  @BeforeEach
  void setUp() throws Exception {
    properties = JdbcEnv.getProperties("collation");
    rdbEngine = RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    admin = StorageFactory.create(properties).getStorageAdmin();
    testUtils = new JdbcAdminTestUtils(properties);
    cleanUp();
  }

  @AfterEach
  void tearDown() throws Exception {
    cleanUp();
    admin.close();
    testUtils.close();
  }

  private void cleanUp() throws Exception {
    if (testUtils.tableExists(NAMESPACE, TABLE)) {
      admin.dropTable(NAMESPACE, TABLE);
    }
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      testUtils.executeSql("DROP DATABASE IF EXISTS `" + NAMESPACE + "`");
    } else {
      admin.dropNamespace(NAMESPACE, true);
    }
    String metadataSchema = new JdbcConfig(new DatabaseConfig(properties)).getMetadataSchema();
    if (testUtils.tableExists(metadataSchema, TableMetadataService.TABLE_NAME)) {
      testUtils.dropMetadataTable();
    }
    if (testUtils.tableExists(metadataSchema, NamespaceMetadataService.TABLE_NAME)) {
      testUtils.dropNamespacesTable();
    }
  }

  private String nonBinaryCollation() {
    return JdbcTestUtils.isPostgresql(rdbEngine) ? "und-x-icu" : "utf8mb4_general_ci";
  }

  private void createTableWithNonBinaryCollationOnPlainColumn() throws Exception {
    admin.createNamespace(NAMESPACE, true);
    admin.createTable(
        NAMESPACE,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(TEXT_PK, DataType.TEXT)
            .addColumn(TEXT_PLAIN, DataType.TEXT)
            .addPartitionKey(TEXT_PK)
            .build(),
        true);
    String collationClause =
        JdbcTestUtils.isPostgresql(rdbEngine)
            ? "COLLATE \"" + nonBinaryCollation() + "\""
            : "CHARACTER SET utf8mb4 COLLATE " + nonBinaryCollation();
    String type = rdbEngine.getDataTypeForEngine(DataType.TEXT) + " " + collationClause;
    for (String sql : rdbEngine.alterColumnTypeSql(NAMESPACE, TABLE, TEXT_PLAIN, type)) {
      testUtils.executeSql(sql);
    }
  }

  @Test
  @DisabledIf("isNotMysqlOrPostgresqlFamily")
  void textColumnCollationSql_ColumnWithDefaultCollation_ShouldReturnNoRow() throws Exception {
    // Arrange
    admin.createNamespace(NAMESPACE, true);
    admin.createTable(
        NAMESPACE,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(TEXT_PK, DataType.TEXT)
            .addColumn(TEXT_PLAIN, DataType.TEXT)
            .addPartitionKey(TEXT_PK)
            .build(),
        true);

    // Act
    String collation = testUtils.readTextColumnCollation(NAMESPACE, TABLE, TEXT_PLAIN);

    // Assert
    assertThat(collation).isNull();
  }

  @Test
  @DisabledIf("isNotMysqlOrPostgresqlFamily")
  void createIndex_ColumnWithDefaultCollation_ShouldKeepCollation() throws Exception {
    // Arrange
    admin.createNamespace(NAMESPACE, true);
    admin.createTable(
        NAMESPACE,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(TEXT_PK, DataType.TEXT)
            .addColumn(TEXT_PLAIN, DataType.TEXT)
            .addPartitionKey(TEXT_PK)
            .build(),
        true);
    String before = testUtils.getColumnCollation(NAMESPACE, TABLE, TEXT_PLAIN);

    // Act
    admin.createIndex(NAMESPACE, TABLE, TEXT_PLAIN, Collections.emptyMap());

    // Assert
    assertThat(testUtils.getColumnCollation(NAMESPACE, TABLE, TEXT_PLAIN)).isEqualTo(before);
  }

  @Test
  @DisabledIf("isNotMysqlOrPostgresqlFamily")
  void createIndex_ColumnWithNonBinaryCollation_ShouldKeepCollation() throws Exception {
    // Arrange
    createTableWithNonBinaryCollationOnPlainColumn();

    // Act
    admin.createIndex(NAMESPACE, TABLE, TEXT_PLAIN, Collections.emptyMap());

    // Assert
    assertThat(testUtils.getColumnCollation(NAMESPACE, TABLE, TEXT_PLAIN))
        .isEqualTo(nonBinaryCollation());
  }

  @Test
  @DisabledIf("isNotMysqlOrPostgresqlFamily")
  void dropIndex_ColumnWithNonBinaryCollation_ShouldKeepCollation() throws Exception {
    // Arrange
    createTableWithNonBinaryCollationOnPlainColumn();
    admin.createIndex(NAMESPACE, TABLE, TEXT_PLAIN, Collections.emptyMap());

    // Act
    admin.dropIndex(NAMESPACE, TABLE, TEXT_PLAIN);

    // Assert
    assertThat(testUtils.getColumnCollation(NAMESPACE, TABLE, TEXT_PLAIN))
        .isEqualTo(nonBinaryCollation());
  }

  @Test
  @DisabledIf("isNotMysqlOrPostgresqlFamily")
  void renameColumn_ColumnWithNonBinaryCollation_ShouldKeepCollation() throws Exception {
    // Arrange
    createTableWithNonBinaryCollationOnPlainColumn();

    // Act
    admin.renameColumn(NAMESPACE, TABLE, TEXT_PLAIN, "renamed");

    // Assert
    assertThat(testUtils.getColumnCollation(NAMESPACE, TABLE, "renamed"))
        .isEqualTo(nonBinaryCollation());
  }

  @Test
  void renameColumn_TidbPrimaryKeyWithNonBinaryCollation_ShouldKeepCollation() throws Exception {
    // Arrange
    assumeTrue(JdbcTestUtils.isTidb(rdbEngine));
    admin.createNamespace(NAMESPACE, true);
    testUtils.executeSql(
        "CREATE TABLE "
            + rdbEngine.encloseFullTableName(NAMESPACE, TABLE)
            + "(`pk` VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci PRIMARY KEY,"
            + "`c` INT)");
    admin.importTable(NAMESPACE, TABLE, Collections.emptyMap());

    // Act
    Throwable thrown =
        catchThrowable(() -> admin.renameColumn(NAMESPACE, TABLE, TEXT_PK, "renamed"));

    // Assert
    assertThat(thrown).isNull();
    assertThat(testUtils.getColumnCollation(NAMESPACE, TABLE, "renamed"))
        .isEqualTo("utf8mb4_general_ci");
  }
}
