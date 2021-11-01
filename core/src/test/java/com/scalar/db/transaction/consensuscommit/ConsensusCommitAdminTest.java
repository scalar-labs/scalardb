package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ConsensusCommitAdminTest {

  private static final String NAMESPACE = "test_namespace";
  private static final String TABLE = "test_table";

  @Mock private DistributedStorageAdmin distributedStorageAdmin;
  @Mock private ConsensusCommitConfig config;
  private ConsensusCommitAdmin admin;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    admin = new ConsensusCommitAdmin(distributedStorageAdmin, config);
  }

  @Test
  public void createCoordinatorTable_shouldCreateCoordinatorTableProperly()
      throws ExecutionException {
    createCoordinatorTable_shouldCreateCoordinatorTableProperly(Optional.empty());
  }

  @Test
  public void
      createCoordinatorTable_WithCoordinatorNamespaceChanged_shouldCreateWithChangedNamespace()
          throws ExecutionException {
    createCoordinatorTable_shouldCreateCoordinatorTableProperly(Optional.of("changed_coordinator"));
  }

  private void createCoordinatorTable_shouldCreateCoordinatorTableProperly(
      Optional<String> coordinatorNamespace) throws ExecutionException {
    // Arrange
    String coordinatorNamespaceName = coordinatorNamespace.orElse(Coordinator.NAMESPACE);
    if (coordinatorNamespace.isPresent()) {
      when(config.getCoordinatorNamespace()).thenReturn(coordinatorNamespace);
      admin = new ConsensusCommitAdmin(distributedStorageAdmin, config);
    }

    // Act
    admin.createCoordinatorTable();

    // Assert
    verify(distributedStorageAdmin).createNamespace(coordinatorNamespaceName, true);
    verify(distributedStorageAdmin)
        .createTable(coordinatorNamespaceName, Coordinator.TABLE, Coordinator.TABLE_METADATA, true);
  }

  @Test
  public void createCoordinatorTable_WithOptions_shouldCreateCoordinatorTableProperly()
      throws ExecutionException {
    createCoordinatorTable_WithOptions_shouldCreateCoordinatorTableProperly(Optional.empty());
  }

  @Test
  public void
      createCoordinatorTable_WithOptionsWithCoordinatorNamespaceChanged_shouldCreateWithChangedNamespace()
          throws ExecutionException {
    createCoordinatorTable_WithOptions_shouldCreateCoordinatorTableProperly(
        Optional.of("changed_coordinator"));
  }

  private void createCoordinatorTable_WithOptions_shouldCreateCoordinatorTableProperly(
      Optional<String> coordinatorNamespace) throws ExecutionException {
    // Arrange
    String coordinatorNamespaceName = coordinatorNamespace.orElse(Coordinator.NAMESPACE);
    if (coordinatorNamespace.isPresent()) {
      when(config.getCoordinatorNamespace()).thenReturn(coordinatorNamespace);
      admin = new ConsensusCommitAdmin(distributedStorageAdmin, config);
    }

    Map<String, String> options = ImmutableMap.of("name", "value");

    // Act
    admin.createCoordinatorTable(options);

    // Assert
    verify(distributedStorageAdmin).createNamespace(coordinatorNamespaceName, true, options);
    verify(distributedStorageAdmin)
        .createTable(
            coordinatorNamespaceName, Coordinator.TABLE, Coordinator.TABLE_METADATA, true, options);
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
  public void dropCoordinatorTable_shouldDropCoordinatorTableProperly() throws ExecutionException {
    dropCoordinatorTable_shouldDropCoordinatorTableProperly(Optional.empty());
  }

  @Test
  public void
      dropCoordinatorTable_WithCoordinatorNamespaceChanged_shouldDropCoordinatorTableProperly()
          throws ExecutionException {
    dropCoordinatorTable_shouldDropCoordinatorTableProperly(Optional.of("changed_coordinator"));
  }

  private void dropCoordinatorTable_shouldDropCoordinatorTableProperly(
      Optional<String> coordinatorNamespace) throws ExecutionException {
    // Arrange
    String coordinatorNamespaceName = coordinatorNamespace.orElse(Coordinator.NAMESPACE);
    if (coordinatorNamespace.isPresent()) {
      when(config.getCoordinatorNamespace()).thenReturn(coordinatorNamespace);
      admin = new ConsensusCommitAdmin(distributedStorageAdmin, config);
    }

    // Act
    admin.dropCoordinatorTable();

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
  public void createTransactionalTable_tableMetadataGiven_shouldCreateTransactionalTableProperly()
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
    admin.createTransactionalTable(NAMESPACE, TABLE, tableMetadata);

    // Assert
    verify(distributedStorageAdmin).createTable(NAMESPACE, TABLE, expected, Collections.emptyMap());
  }

  @Test
  public void
      createTransactionalTable_tableMetadataThatHasTransactionMetaColumnGiven_shouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                admin.createTransactionalTable(
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
      createTransactionalTable_tableMetadataThatHasNonPrimaryKeyColumnWithBeforePrefixGiven_shouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                admin.createTransactionalTable(
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
}
