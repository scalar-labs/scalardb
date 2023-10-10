package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils.getBeforeImageColumnName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Abstraction that defines unit tests for the {@link ConsensusCommitAdmin}. The class purpose is to
 * be able to run the {@link ConsensusCommitAdmin} unit tests with different values for the {@link
 * ConsensusCommitConfig}, notably {@link ConsensusCommitConfig#COORDINATOR_NAMESPACE}.
 */
public abstract class ConsensusCommitAdminTestBase {

  private static final String NAMESPACE = "test_namespace";
  private static final String TABLE = "test_table";

  @Mock private DistributedStorageAdmin distributedStorageAdmin;
  @Mock private ConsensusCommitConfig config;
  private ConsensusCommitAdmin admin;
  private String coordinatorNamespaceName;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    when(config.getCoordinatorNamespace()).thenReturn(getCoordinatorNamespaceConfig());
    admin = new ConsensusCommitAdmin(distributedStorageAdmin, config, false);
    coordinatorNamespaceName = getCoordinatorNamespaceConfig().orElse(Coordinator.NAMESPACE);
  }

  protected abstract Optional<String> getCoordinatorNamespaceConfig();

  @Test
  public void createCoordinatorTables_shouldCreateCoordinatorTableProperly()
      throws ExecutionException {
    // Arrange

    // Act
    admin.createCoordinatorTables();

    // Assert
    verify(distributedStorageAdmin)
        .createNamespace(coordinatorNamespaceName, Collections.emptyMap());
    verify(distributedStorageAdmin)
        .createTable(
            coordinatorNamespaceName,
            Coordinator.TABLE,
            Coordinator.TABLE_METADATA,
            Collections.emptyMap());
  }

  @Test
  public void
      createCoordinatorTables_CoordinatorTablesAlreadyExist_shouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    when(distributedStorageAdmin.tableExists(coordinatorNamespaceName, Coordinator.TABLE))
        .thenReturn(true);

    // Act Assert
    assertThatThrownBy(() -> admin.createCoordinatorTables())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createCoordinatorTables_WithOptions_shouldCreateCoordinatorTableProperly()
      throws ExecutionException {
    // Arrange
    Map<String, String> options = ImmutableMap.of("name", "value");

    // Act
    admin.createCoordinatorTables(options);

    // Assert
    verify(distributedStorageAdmin).createNamespace(coordinatorNamespaceName, options);
    verify(distributedStorageAdmin)
        .createTable(
            coordinatorNamespaceName, Coordinator.TABLE, Coordinator.TABLE_METADATA, options);
  }

  @Test
  public void truncateCoordinatorTables_shouldTruncateCoordinatorTableProperly()
      throws ExecutionException {
    // Arrange
    when(distributedStorageAdmin.tableExists(coordinatorNamespaceName, Coordinator.TABLE))
        .thenReturn(true);

    // Act
    admin.truncateCoordinatorTables();

    // Assert
    verify(distributedStorageAdmin).truncateTable(coordinatorNamespaceName, Coordinator.TABLE);
  }

  @Test
  public void
      truncateCoordinatorTables_CoordinatorTablesNotExist_shouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    when(distributedStorageAdmin.tableExists(coordinatorNamespaceName, Coordinator.TABLE))
        .thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> admin.truncateCoordinatorTables())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void dropCoordinatorTables_shouldDropCoordinatorTableProperly() throws ExecutionException {
    // Arrange
    when(distributedStorageAdmin.tableExists(coordinatorNamespaceName, Coordinator.TABLE))
        .thenReturn(true);

    // Act
    admin.dropCoordinatorTables();

    // Assert
    verify(distributedStorageAdmin).dropTable(coordinatorNamespaceName, Coordinator.TABLE);
    verify(distributedStorageAdmin).dropNamespace(coordinatorNamespaceName);
  }

  @Test
  public void dropCoordinatorTables_CoordinatorTablesNotExist_shouldThrowIllegalArgumentException()
      throws ExecutionException {
    // Arrange
    when(distributedStorageAdmin.tableExists(coordinatorNamespaceName, Coordinator.TABLE))
        .thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> admin.dropCoordinatorTables())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void coordinatorTablesExist_WhenCoordinatorTableNotExist_shouldReturnFalse()
      throws ExecutionException {
    // Arrange
    when(distributedStorageAdmin.tableExists(any(), any())).thenReturn(false);

    // Act
    boolean actual = admin.coordinatorTablesExist();

    // Assert
    verify(distributedStorageAdmin).tableExists(coordinatorNamespaceName, Coordinator.TABLE);
    assertThat(actual).isFalse();
  }

  @Test
  public void coordinatorTablesExist_WhenCoordinatorTableExists_shouldReturnTrue()
      throws ExecutionException {
    // Arrange
    when(distributedStorageAdmin.tableExists(any(), any())).thenReturn(true);

    // Act
    boolean actual = admin.coordinatorTablesExist();

    // Assert
    verify(distributedStorageAdmin).tableExists(coordinatorNamespaceName, Coordinator.TABLE);
    assertThat(actual).isTrue();
  }

  @Test
  public void createTable_tableMetadataGiven_shouldCreateTransactionTableProperly()
      throws ExecutionException {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String BALANCE = "balance";

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();

    TableMetadata expected =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addColumn(Attribute.ID, DataType.TEXT)
            .addColumn(Attribute.STATE, DataType.INT)
            .addColumn(Attribute.VERSION, DataType.INT)
            .addColumn(Attribute.PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.COMMITTED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_PREFIX + BALANCE, DataType.INT)
            .addColumn(Attribute.BEFORE_ID, DataType.TEXT)
            .addColumn(Attribute.BEFORE_STATE, DataType.INT)
            .addColumn(Attribute.BEFORE_VERSION, DataType.INT)
            .addColumn(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();

    // Act
    admin.createTable(NAMESPACE, TABLE, tableMetadata);

    // Assert
    verify(distributedStorageAdmin).createTable(NAMESPACE, TABLE, expected, Collections.emptyMap());
  }

  @Test
  public void
      createTable_tableMetadataThatHasTransactionMetaColumnGiven_shouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                admin.createTable(
                    NAMESPACE,
                    TABLE,
                    TableMetadata.newBuilder()
                        .addColumn("col1", DataType.INT)
                        .addColumn("col2", DataType.INT)
                        .addColumn(Attribute.ID, DataType.TEXT) // transaction meta column
                        .addPartitionKey("col1")
                        .build()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      createTable_tableMetadataThatHasNonPrimaryKeyColumnWithBeforePrefixGiven_shouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                admin.createTable(
                    NAMESPACE,
                    TABLE,
                    TableMetadata.newBuilder()
                        .addColumn("col1", DataType.INT)
                        .addColumn("col2", DataType.INT)
                        .addColumn(
                            Attribute.BEFORE_PREFIX + "col2",
                            DataType.INT) // non-primary key column with the "before_" prefix
                        .addPartitionKey("col1")
                        .build()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createNamespace_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.createNamespace("ns", Collections.emptyMap());

    // Assert
    verify(distributedStorageAdmin).createNamespace("ns", Collections.emptyMap());
  }

  @Test
  public void dropTable_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.dropTable("ns", "tbl");

    // Assert
    verify(distributedStorageAdmin).dropTable("ns", "tbl");
  }

  @Test
  public void dropNamespace_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.dropNamespace("ns");

    // Assert
    verify(distributedStorageAdmin).dropNamespace("ns");
  }

  @Test
  public void truncateTable_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.truncateTable("ns", "tbl");

    // Assert
    verify(distributedStorageAdmin).truncateTable("ns", "tbl");
  }

  @Test
  public void createIndex_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.createIndex("ns", "tbl", "col", Collections.emptyMap());

    // Assert
    verify(distributedStorageAdmin).createIndex("ns", "tbl", "col", Collections.emptyMap());
  }

  @Test
  public void dropIndex_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.dropIndex("ns", "tbl", "col");

    // Assert
    verify(distributedStorageAdmin).dropIndex("ns", "tbl", "col");
  }

  @Test
  public void getTableMetadata_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String BALANCE = "balance";

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addColumn(Attribute.ID, DataType.TEXT)
            .addColumn(Attribute.STATE, DataType.INT)
            .addColumn(Attribute.VERSION, DataType.INT)
            .addColumn(Attribute.PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.COMMITTED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_PREFIX + BALANCE, DataType.INT)
            .addColumn(Attribute.BEFORE_ID, DataType.TEXT)
            .addColumn(Attribute.BEFORE_STATE, DataType.INT)
            .addColumn(Attribute.BEFORE_VERSION, DataType.INT)
            .addColumn(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();

    TableMetadata expected =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();

    when(distributedStorageAdmin.getTableMetadata(any(), any())).thenReturn(tableMetadata);

    // Act
    TableMetadata actual = admin.getTableMetadata("ns", "tbl");

    // Assert
    verify(distributedStorageAdmin).getTableMetadata("ns", "tbl");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void getTableMetadata_WithIncludeMetadataEnabled_ShouldCallJdbcAdminProperly()
      throws ExecutionException {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String BALANCE = "balance";

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addColumn(Attribute.ID, DataType.TEXT)
            .addColumn(Attribute.STATE, DataType.INT)
            .addColumn(Attribute.VERSION, DataType.INT)
            .addColumn(Attribute.PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.COMMITTED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_PREFIX + BALANCE, DataType.INT)
            .addColumn(Attribute.BEFORE_ID, DataType.TEXT)
            .addColumn(Attribute.BEFORE_STATE, DataType.INT)
            .addColumn(Attribute.BEFORE_VERSION, DataType.INT)
            .addColumn(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();

    when(distributedStorageAdmin.getTableMetadata(any(), any())).thenReturn(tableMetadata);
    ConsensusCommitAdmin adminWithIncludeMetadataEnabled =
        new ConsensusCommitAdmin(distributedStorageAdmin, config, true);

    // Act
    TableMetadata actual = adminWithIncludeMetadataEnabled.getTableMetadata("ns", "tbl");

    // Assert
    verify(distributedStorageAdmin).getTableMetadata("ns", "tbl");
    assertThat(actual).isEqualTo(tableMetadata);
  }

  @Test
  public void getTableMetadata_ForNonTransactionTable_ShouldReturnNull() throws ExecutionException {
    // Arrange
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("c1", DataType.BOOLEAN)
            .build();
    when(distributedStorageAdmin.getTableMetadata(any(), any())).thenReturn(tableMetadata);

    // Act
    TableMetadata actual = admin.getTableMetadata(NAMESPACE, TABLE);

    // Assert
    assertThat(actual).isNull();
    verify(distributedStorageAdmin).getTableMetadata(NAMESPACE, TABLE);
  }

  @Test
  public void getTableMetadata_ForNonExistingTable_ShouldReturnNull() throws ExecutionException {
    // Arrange
    when(distributedStorageAdmin.getTableMetadata(any(), any())).thenReturn(null);

    // Act
    TableMetadata actual = admin.getTableMetadata(NAMESPACE, TABLE);

    // Assert
    assertThat(actual).isNull();
    verify(distributedStorageAdmin).getTableMetadata(NAMESPACE, TABLE);
  }

  @Test
  public void
      getNamespaceTableNames_ForNamespaceContainingTransactionAndStorageTables_ShouldCallReturnOnlyTransactionTables()
          throws ExecutionException {
    // Arrange
    TableMetadata storageTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("id")
            .addColumn("id", DataType.TEXT)
            .addColumn("created_at", DataType.BIGINT)
            .build();
    TableMetadata transactionTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("account_id")
            .addColumn("account_id", DataType.INT)
            .addColumn("balance", DataType.INT)
            .addColumn(Attribute.ID, DataType.TEXT)
            .addColumn(Attribute.STATE, DataType.INT)
            .addColumn(Attribute.VERSION, DataType.INT)
            .addColumn(Attribute.PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.COMMITTED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_PREFIX + "balance", DataType.INT)
            .addColumn(Attribute.BEFORE_ID, DataType.TEXT)
            .addColumn(Attribute.BEFORE_STATE, DataType.INT)
            .addColumn(Attribute.BEFORE_VERSION, DataType.INT)
            .addColumn(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
            .build();

    when(distributedStorageAdmin.getNamespaceTableNames(any()))
        .thenReturn(ImmutableSet.of("tbl1", "tbl2", "tbl3"));
    when(distributedStorageAdmin.getTableMetadata(NAMESPACE, "tbl1"))
        .thenReturn(transactionTableMetadata);
    when(distributedStorageAdmin.getTableMetadata(NAMESPACE, "tbl2"))
        .thenReturn(storageTableMetadata);
    when(distributedStorageAdmin.getTableMetadata(NAMESPACE, "tbl3"))
        .thenReturn(transactionTableMetadata);

    // Act
    Set<String> actual = admin.getNamespaceTableNames(NAMESPACE);

    // Assert
    assertThat(actual).containsExactlyInAnyOrder("tbl1", "tbl3");
    verify(distributedStorageAdmin).getNamespaceTableNames(NAMESPACE);
    verify(distributedStorageAdmin).getTableMetadata(NAMESPACE, "tbl1");
    verify(distributedStorageAdmin).getTableMetadata(NAMESPACE, "tbl2");
    verify(distributedStorageAdmin).getTableMetadata(NAMESPACE, "tbl3");
  }

  @Test
  public void namespaceExists_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange
    when(distributedStorageAdmin.namespaceExists(any())).thenReturn(true);

    // Act
    boolean actual = admin.namespaceExists("ns");

    // Assert
    verify(distributedStorageAdmin).namespaceExists("ns");
    assertThat(actual).isTrue();
  }

  @Test
  public void repairTable_withMetadataGiven_shouldRepairWithTransactionColumnsAdded()
      throws ExecutionException {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String BALANCE = "balance";

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();

    TableMetadata expected =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addColumn(Attribute.ID, DataType.TEXT)
            .addColumn(Attribute.STATE, DataType.INT)
            .addColumn(Attribute.VERSION, DataType.INT)
            .addColumn(Attribute.PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.COMMITTED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_PREFIX + BALANCE, DataType.INT)
            .addColumn(Attribute.BEFORE_ID, DataType.TEXT)
            .addColumn(Attribute.BEFORE_STATE, DataType.INT)
            .addColumn(Attribute.BEFORE_VERSION, DataType.INT)
            .addColumn(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();
    Map<String, String> options = ImmutableMap.of("foo", "bar");

    // Act
    admin.repairTable(NAMESPACE, TABLE, tableMetadata, options);

    // Assert
    verify(distributedStorageAdmin).repairTable(NAMESPACE, TABLE, expected, options);
  }

  @Test
  public void repairCoordinatorTables_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange
    Map<String, String> options = ImmutableMap.of("foo", "bar");

    // Act
    admin.repairCoordinatorTables(options);

    // Assert
    verify(distributedStorageAdmin).repairNamespace(coordinatorNamespaceName, options);
    verify(distributedStorageAdmin)
        .repairTable(
            coordinatorNamespaceName, Coordinator.TABLE, Coordinator.TABLE_METADATA, options);
  }

  @Test
  public void addNewColumnToTable_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange
    String newColumn = "c2";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder().addColumn("col1", DataType.INT).addPartitionKey("col1").build();
    when(distributedStorageAdmin.getTableMetadata(any(), any()))
        .thenReturn(ConsensusCommitUtils.buildTransactionTableMetadata(tableMetadata));

    // Act
    admin.addNewColumnToTable(NAMESPACE, TABLE, newColumn, DataType.TEXT);

    // Assert
    verify(distributedStorageAdmin).getTableMetadata(NAMESPACE, TABLE);
    verify(distributedStorageAdmin).addNewColumnToTable(NAMESPACE, TABLE, newColumn, DataType.TEXT);
    verify(distributedStorageAdmin)
        .addNewColumnToTable(NAMESPACE, TABLE, Attribute.BEFORE_PREFIX + newColumn, DataType.TEXT);
  }

  @Test
  public void importTable_ShouldCallStorageAdminProperly() throws ExecutionException {
    // Arrange
    String primaryKeyColumn = "pk";
    String column = "col";
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn(primaryKeyColumn, DataType.INT)
            .addColumn(column, DataType.INT)
            .addPartitionKey(primaryKeyColumn)
            .build();
    when(distributedStorageAdmin.getTableMetadata(NAMESPACE, TABLE)).thenReturn(null);
    when(distributedStorageAdmin.getImportTableMetadata(NAMESPACE, TABLE)).thenReturn(metadata);
    doNothing()
        .when(distributedStorageAdmin)
        .addRawColumnToTable(anyString(), anyString(), anyString(), any(DataType.class));

    // Act
    admin.importTable(NAMESPACE, TABLE);

    // Assert
    verify(distributedStorageAdmin).getTableMetadata(NAMESPACE, TABLE);
    verify(distributedStorageAdmin).getImportTableMetadata(NAMESPACE, TABLE);
    for (Entry<String, DataType> entry :
        ConsensusCommitUtils.getTransactionMetaColumns().entrySet()) {
      verify(distributedStorageAdmin)
          .addRawColumnToTable(NAMESPACE, TABLE, entry.getKey(), entry.getValue());
    }
    verify(distributedStorageAdmin)
        .addRawColumnToTable(
            NAMESPACE, TABLE, getBeforeImageColumnName(column, metadata), DataType.INT);
    verify(distributedStorageAdmin, never())
        .addRawColumnToTable(NAMESPACE, TABLE, primaryKeyColumn, DataType.INT);
  }

  @Test
  public void importTable_WithTableAlreadyExists_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    // Arrange
    String primaryKeyColumn = "pk";
    String column = "col";
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn(primaryKeyColumn, DataType.INT)
            .addColumn(column, DataType.INT)
            .addColumn(Attribute.ID, DataType.TEXT)
            .addColumn(Attribute.STATE, DataType.INT)
            .addColumn(Attribute.VERSION, DataType.INT)
            .addColumn(Attribute.PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.COMMITTED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_PREFIX + column, DataType.INT)
            .addColumn(Attribute.BEFORE_ID, DataType.TEXT)
            .addColumn(Attribute.BEFORE_STATE, DataType.INT)
            .addColumn(Attribute.BEFORE_VERSION, DataType.INT)
            .addColumn(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
            .addPartitionKey(primaryKeyColumn)
            .build();
    when(distributedStorageAdmin.getTableMetadata(NAMESPACE, TABLE)).thenReturn(metadata);

    // Act
    Throwable thrown = catchThrowable(() -> admin.importTable(NAMESPACE, TABLE));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getNamespacesNames_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange
    Set<String> namespaces = ImmutableSet.of("n1", "n2", coordinatorNamespaceName);
    when(distributedStorageAdmin.getNamespaceNames()).thenReturn(namespaces);

    // Act
    Set<String> actualNamespaces = admin.getNamespaceNames();

    // Assert
    verify(distributedStorageAdmin).getNamespaceNames();
    assertThat(actualNamespaces).containsOnly("n1", "n2");
  }

  @Test
  public void repairNamespace_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.repairNamespace("ns", Collections.emptyMap());

    // Assert
    verify(distributedStorageAdmin).repairNamespace("ns", Collections.emptyMap());
  }
}
