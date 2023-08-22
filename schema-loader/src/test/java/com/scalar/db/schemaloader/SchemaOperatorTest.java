package com.scalar.db.schemaloader;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SchemaOperatorTest {
  @Mock private DistributedStorageAdmin storageAdmin;
  @Mock private DistributedTransactionAdmin transactionAdmin;
  @Mock private TableSchema tableSchema;
  @Mock private Map<String, String> options;

  private SchemaOperator operator;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    operator = new SchemaOperator(storageAdmin, transactionAdmin);
  }

  @Test
  public void createTables_WithTransactionTables_ShouldCallProperMethods() throws Exception {
    // Arrange
    List<TableSchema> tableSchemaList = Arrays.asList(tableSchema, tableSchema, tableSchema);
    when(tableSchema.getNamespace()).thenReturn("ns");
    when(tableSchema.getOptions()).thenReturn(options);
    when(tableSchema.isTransactionTable()).thenReturn(true);
    when(tableSchema.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableSchema.getTableMetadata()).thenReturn(tableMetadata);

    // Act
    operator.createTables(tableSchemaList);

    // Assert
    verify(storageAdmin, times(3)).createNamespace("ns", true, options);
    verify(storageAdmin, times(3)).tableExists("ns", "tb");
    verify(transactionAdmin, times(3)).createTable("ns", "tb", tableMetadata, options);
  }

  @Test
  public void createTables_WithNonTransactionTables_ShouldCallProperMethods() throws Exception {
    // Arrange
    List<TableSchema> tableSchemaList = Arrays.asList(tableSchema, tableSchema, tableSchema);
    when(tableSchema.getNamespace()).thenReturn("ns");
    when(tableSchema.getOptions()).thenReturn(options);
    when(tableSchema.isTransactionTable()).thenReturn(false);
    when(tableSchema.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableSchema.getTableMetadata()).thenReturn(tableMetadata);

    // Act
    operator.createTables(tableSchemaList);

    // Assert
    verify(storageAdmin, times(3)).createNamespace("ns", true, options);
    verify(storageAdmin, times(3)).tableExists("ns", "tb");
    verify(storageAdmin, times(3)).createTable("ns", "tb", tableMetadata, options);
  }

  @Test
  public void
      createCoordinatorTables_IfCoordinatorTablesNotExist_ShouldCallCreateCoordinatorTables()
          throws Exception {
    // Arrange
    when(transactionAdmin.coordinatorTablesExist()).thenReturn(false);

    // Act
    operator.createCoordinatorTables(options);

    // Assert
    verify(transactionAdmin).coordinatorTablesExist();
    verify(transactionAdmin).createCoordinatorTables(options);
  }

  @Test
  public void
      createCoordinatorTables_IfCoordinatorTablesExist_ShouldNotCallCreateCoordinatorTables()
          throws Exception {
    // Arrange
    when(transactionAdmin.coordinatorTablesExist()).thenReturn(true);

    // Act
    operator.createCoordinatorTables(options);

    // Assert
    verify(transactionAdmin).coordinatorTablesExist();
    verify(transactionAdmin, never()).createCoordinatorTables(options);
  }

  @Test
  public void deleteTables_WithTableList_ShouldCallProperMethods() throws Exception {
    // Arrange
    List<TableSchema> tableSchemaList = Arrays.asList(tableSchema, tableSchema, tableSchema);
    when(tableSchema.getNamespace()).thenReturn("ns");
    when(tableSchema.getOptions()).thenReturn(options);
    when(tableSchema.isTransactionTable()).thenReturn(true);
    when(tableSchema.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableSchema.getTableMetadata()).thenReturn(tableMetadata);
    when(storageAdmin.getNamespaceTableNames("ns")).thenReturn(ImmutableSet.of());
    when(storageAdmin.tableExists("ns", "tb")).thenReturn(true);

    // Act
    operator.deleteTables(tableSchemaList);

    // Assert
    verify(storageAdmin, times(3)).tableExists("ns", "tb");
    verify(storageAdmin, times(3)).dropTable("ns", "tb");
    verify(storageAdmin).getNamespaceTableNames("ns");
    verify(storageAdmin).dropNamespace("ns", true);
  }

  @Test
  public void deleteTables_TableStillInNamespace_ShouldNotDropNamespace() throws Exception {
    // Arrange
    List<TableSchema> tableSchemaList = Arrays.asList(tableSchema, tableSchema, tableSchema);
    when(tableSchema.getNamespace()).thenReturn("ns");
    when(tableSchema.getOptions()).thenReturn(options);
    when(tableSchema.isTransactionTable()).thenReturn(true);
    when(tableSchema.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableSchema.getTableMetadata()).thenReturn(tableMetadata);
    when(storageAdmin.tableExists("ns", "tb")).thenReturn(true);
    when(storageAdmin.getNamespaceTableNames("ns")).thenReturn(ImmutableSet.of("tbl"));

    // Act
    operator.deleteTables(tableSchemaList);

    // Assert
    verify(storageAdmin, times(3)).tableExists("ns", "tb");
    verify(storageAdmin, times(3)).dropTable("ns", "tb");
    verify(storageAdmin).getNamespaceTableNames("ns");
    verify(storageAdmin, never()).dropNamespace("ns", true);
  }

  @Test
  public void dropCoordinatorTables_IfCoordinatorTablesExist_ShouldCallDropCoordinatorTables()
      throws Exception {
    // Arrange
    when(transactionAdmin.coordinatorTablesExist()).thenReturn(true);

    // Act
    operator.dropCoordinatorTables();

    // Assert
    verify(transactionAdmin).coordinatorTablesExist();
    verify(transactionAdmin).dropCoordinatorTables();
  }

  @Test
  public void dropCoordinatorTables_IfCoordinatorTablesNotExist_ShouldCallNotDropCoordinatorTables()
      throws Exception {
    // Arrange
    when(transactionAdmin.coordinatorTablesExist()).thenReturn(false);

    // Act
    operator.dropCoordinatorTables();

    // Assert
    verify(transactionAdmin).coordinatorTablesExist();
    verify(transactionAdmin, never()).dropCoordinatorTables();
  }

  @Test
  public void repairTables_WithTransactionTables_ShouldCallProperMethods() throws Exception {
    // Arrange
    List<TableSchema> tableSchemaList = Arrays.asList(tableSchema, tableSchema, tableSchema);
    when(tableSchema.getNamespace()).thenReturn("ns");
    when(tableSchema.getOptions()).thenReturn(options);
    when(tableSchema.isTransactionTable()).thenReturn(true);
    when(tableSchema.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableSchema.getTableMetadata()).thenReturn(tableMetadata);

    // Act
    operator.repairTables(tableSchemaList);

    // Assert
    verify(transactionAdmin, times(3)).repairTable("ns", "tb", tableMetadata, options);
    verifyNoInteractions(storageAdmin);
  }

  @Test
  public void repairTables_WithoutTransactionTables_ShouldCallProperMethods() throws Exception {
    // Arrange
    List<TableSchema> tableSchemaList = Arrays.asList(tableSchema, tableSchema, tableSchema);
    when(tableSchema.getNamespace()).thenReturn("ns");
    when(tableSchema.getOptions()).thenReturn(options);
    when(tableSchema.isTransactionTable()).thenReturn(false);
    when(tableSchema.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableSchema.getTableMetadata()).thenReturn(tableMetadata);

    // Act
    operator.repairTables(tableSchemaList);

    // Assert
    verify(storageAdmin, times(3)).repairTable("ns", "tb", tableMetadata, options);
    verifyNoInteractions(transactionAdmin);
  }

  @Test
  public void repairCoordinatorTables_ShouldCallProperMethods() throws Exception {
    // Arrange

    // Act
    operator.repairCoordinatorTables(options);

    // Assert
    verify(transactionAdmin).repairCoordinatorTables(options);
    verifyNoInteractions(storageAdmin);
  }
}
