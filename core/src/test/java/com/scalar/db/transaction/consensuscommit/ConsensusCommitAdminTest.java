package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
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
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ConsensusCommitAdminTest {

  private static final String NAMESPACE = "test_namespace";
  private static final String TABLE = "test_table";

  @Mock private DistributedStorageAdmin distributedStorageAdmin;
  @Mock private ConsensusCommitConfig config;
  private ConsensusCommitAdmin admin;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    admin = new ConsensusCommitAdmin(distributedStorageAdmin, config);
  }

  @Test
  public void createCoordinatorNamespaceAndTable_shouldCreateCoordinatorTableProperly()
      throws ExecutionException {
    createCoordinatorNamespaceAndTable_shouldCreateCoordinatorTableProperly(Optional.empty());
  }

  @Test
  public void
      createCoordinatorNamespaceAndTable_WithCoordinatorNamespaceChanged_shouldCreateWithChangedNamespace()
          throws ExecutionException {
    createCoordinatorNamespaceAndTable_shouldCreateCoordinatorTableProperly(
        Optional.of("changed_coordinator"));
  }

  private void createCoordinatorNamespaceAndTable_shouldCreateCoordinatorTableProperly(
      Optional<String> coordinatorNamespace) throws ExecutionException {
    // Arrange
    String coordinatorNamespaceName = coordinatorNamespace.orElse(Coordinator.NAMESPACE);
    if (coordinatorNamespace.isPresent()) {
      when(config.getCoordinatorNamespace()).thenReturn(coordinatorNamespace);
      admin = new ConsensusCommitAdmin(distributedStorageAdmin, config);
    }

    // Act
    admin.createCoordinatorNamespaceAndTable();

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
  public void createCoordinatorNamespaceAndTable_WithOptions_shouldCreateCoordinatorTableProperly()
      throws ExecutionException {
    createCoordinatorNamespaceAndTable_WithOptions_shouldCreateCoordinatorTableProperly(
        Optional.empty());
  }

  @Test
  public void
      createCoordinatorNamespaceAndTable_WithOptionsWithCoordinatorNamespaceChanged_shouldCreateWithChangedNamespace()
          throws ExecutionException {
    createCoordinatorNamespaceAndTable_WithOptions_shouldCreateCoordinatorTableProperly(
        Optional.of("changed_coordinator"));
  }

  private void createCoordinatorNamespaceAndTable_WithOptions_shouldCreateCoordinatorTableProperly(
      Optional<String> coordinatorNamespace) throws ExecutionException {
    // Arrange
    String coordinatorNamespaceName = coordinatorNamespace.orElse(Coordinator.NAMESPACE);
    if (coordinatorNamespace.isPresent()) {
      when(config.getCoordinatorNamespace()).thenReturn(coordinatorNamespace);
      admin = new ConsensusCommitAdmin(distributedStorageAdmin, config);
    }

    Map<String, String> options = ImmutableMap.of("name", "value");

    // Act
    admin.createCoordinatorNamespaceAndTable(options);

    // Assert
    verify(distributedStorageAdmin).createNamespace(coordinatorNamespaceName, options);
    verify(distributedStorageAdmin)
        .createTable(
            coordinatorNamespaceName, Coordinator.TABLE, Coordinator.TABLE_METADATA, options);
  }

  @Test
  public void truncateCoordinatorTable_shouldTruncateCoordinatorTableProperly()
      throws ExecutionException {
    truncateCoordinatorTable_shouldTruncateCoordinatorTableProperly(Optional.empty());
  }

  @Test
  public void
      truncateCoordinatorTable_WithCoordinatorNamespaceChanged_shouldTruncateCoordinatorTableProperly()
          throws ExecutionException {
    truncateCoordinatorTable_shouldTruncateCoordinatorTableProperly(
        Optional.of("changed_coordinator"));
  }

  private void truncateCoordinatorTable_shouldTruncateCoordinatorTableProperly(
      Optional<String> coordinatorNamespace) throws ExecutionException {
    // Arrange
    String coordinatorNamespaceName = coordinatorNamespace.orElse(Coordinator.NAMESPACE);
    if (coordinatorNamespace.isPresent()) {
      when(config.getCoordinatorNamespace()).thenReturn(coordinatorNamespace);
      admin = new ConsensusCommitAdmin(distributedStorageAdmin, config);
    }

    // Act
    admin.truncateCoordinatorTable();

    // Assert
    verify(distributedStorageAdmin).truncateTable(coordinatorNamespaceName, Coordinator.TABLE);
  }

  @Test
  public void dropCoordinatorNamespaceAndTable_shouldDropCoordinatorTableProperly()
      throws ExecutionException {
    dropCoordinatorNamespaceAndTable_shouldDropCoordinatorTableProperly(Optional.empty());
  }

  @Test
  public void
      dropCoordinatorNamespaceAndTable_WithCoordinatorNamespaceChanged_shouldDropCoordinatorTableProperly()
          throws ExecutionException {
    dropCoordinatorNamespaceAndTable_shouldDropCoordinatorTableProperly(
        Optional.of("changed_coordinator"));
  }

  private void dropCoordinatorNamespaceAndTable_shouldDropCoordinatorTableProperly(
      Optional<String> coordinatorNamespace) throws ExecutionException {
    // Arrange
    String coordinatorNamespaceName = coordinatorNamespace.orElse(Coordinator.NAMESPACE);
    if (coordinatorNamespace.isPresent()) {
      when(config.getCoordinatorNamespace()).thenReturn(coordinatorNamespace);
      admin = new ConsensusCommitAdmin(distributedStorageAdmin, config);
    }

    // Act
    admin.dropCoordinatorNamespaceAndTable();

    // Assert
    verify(distributedStorageAdmin).dropTable(coordinatorNamespaceName, Coordinator.TABLE);
    verify(distributedStorageAdmin).dropNamespace(coordinatorNamespaceName);
  }

  @Test
  public void coordinatorTableExists_WhenCoordinatorTableNotExist_shouldReturnFalse()
      throws ExecutionException {
    // Arrange
    when(distributedStorageAdmin.tableExists(Coordinator.NAMESPACE, Coordinator.TABLE))
        .thenReturn(false);

    // Act
    boolean actual = admin.coordinatorTableExists();

    // Assert
    verify(distributedStorageAdmin).tableExists(Coordinator.NAMESPACE, Coordinator.TABLE);
    assertThat(actual).isFalse();
  }

  @Test
  public void coordinatorTableExists_WhenCoordinatorTableExists_shouldReturnTrue()
      throws ExecutionException {
    // Arrange
    when(distributedStorageAdmin.tableExists(Coordinator.NAMESPACE, Coordinator.TABLE))
        .thenReturn(true);

    // Act
    boolean actual = admin.coordinatorTableExists();

    // Assert
    verify(distributedStorageAdmin).tableExists(Coordinator.NAMESPACE, Coordinator.TABLE);
    assertThat(actual).isTrue();
  }

  @Test
  public void createTable_tableMetadataGiven_shouldCreateTransactionalTableProperly()
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
  public void getNamespaceTableNames_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange
    Set<String> tableNames = ImmutableSet.of("tbl1", "tbl2", "tbl3");
    when(distributedStorageAdmin.getNamespaceTableNames(any())).thenReturn(tableNames);

    // Act
    Set<String> actual = admin.getNamespaceTableNames("ns");

    // Assert
    verify(distributedStorageAdmin).getNamespaceTableNames("ns");
    assertThat(actual).isEqualTo(tableNames);
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
}
