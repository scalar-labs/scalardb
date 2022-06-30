package com.scalar.db.schemaloader;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SchemaOperatorTest {
  @Mock private DistributedStorageAdmin admin;
  @Mock private ConsensusCommitAdmin consensusCommitAdmin;
  @Mock private TableSchema tableSchema;
  @Mock private Map<String, String> options;

  private SchemaOperator operator;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    operator = new SchemaOperator(admin, consensusCommitAdmin);
  }

  @Test
  public void createTables_WithTransactionalTables_ShouldCallProperMethods() throws Exception {
    // Arrange
    List<TableSchema> tableSchemaList = Arrays.asList(tableSchema, tableSchema, tableSchema);
    when(tableSchema.getNamespace()).thenReturn("ns");
    when(tableSchema.getOptions()).thenReturn(options);
    when(tableSchema.isTransactionalTable()).thenReturn(true);
    when(tableSchema.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableSchema.getTableMetadata()).thenReturn(tableMetadata);

    // Act
    operator.createTables(tableSchemaList);

    // Assert
    verify(admin, times(3)).createNamespace("ns", true, options);
    verify(admin, times(3)).tableExists("ns", "tb");
    verify(consensusCommitAdmin, times(3)).createTable("ns", "tb", tableMetadata, options);
  }

  @Test
  public void createTables_WithNonTransactionalTables_ShouldCallProperMethods() throws Exception {
    // Arrange
    List<TableSchema> tableSchemaList = Arrays.asList(tableSchema, tableSchema, tableSchema);
    when(tableSchema.getNamespace()).thenReturn("ns");
    when(tableSchema.getOptions()).thenReturn(options);
    when(tableSchema.isTransactionalTable()).thenReturn(false);
    when(tableSchema.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableSchema.getTableMetadata()).thenReturn(tableMetadata);

    // Act
    operator.createTables(tableSchemaList);

    // Assert
    verify(admin, times(3)).createNamespace("ns", true, options);
    verify(admin, times(3)).tableExists("ns", "tb");
    verify(admin, times(3)).createTable("ns", "tb", tableMetadata, options);
  }

  @Test
  public void
      createCoordinatorTables_IfCoordinatorTablesNotExist_ShouldCallCreateCoordinatorTables()
          throws Exception {
    // Arrange
    when(consensusCommitAdmin.coordinatorTablesExist()).thenReturn(false);

    // Act
    operator.createCoordinatorTables(options);

    // Assert
    verify(consensusCommitAdmin).coordinatorTablesExist();
    verify(consensusCommitAdmin).createCoordinatorTables(options);
  }

  @Test
  public void
      createCoordinatorTables_IfCoordinatorTablesExist_ShouldNotCallCreateCoordinatorTables()
          throws Exception {
    // Arrange
    when(consensusCommitAdmin.coordinatorTablesExist()).thenReturn(true);

    // Act
    operator.createCoordinatorTables(options);

    // Assert
    verify(consensusCommitAdmin).coordinatorTablesExist();
    verify(consensusCommitAdmin, never()).createCoordinatorTables(options);
  }

  @Test
  public void deleteTables_WithTableList_ShouldCallProperMethods() throws Exception {
    // Arrange
    List<TableSchema> tableSchemaList = Arrays.asList(tableSchema, tableSchema, tableSchema);
    when(tableSchema.getNamespace()).thenReturn("ns");
    when(tableSchema.getOptions()).thenReturn(options);
    when(tableSchema.isTransactionalTable()).thenReturn(true);
    when(tableSchema.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableSchema.getTableMetadata()).thenReturn(tableMetadata);
    when(admin.tableExists("ns", "tb")).thenReturn(true);

    // Act
    operator.deleteTables(tableSchemaList);

    // Assert
    verify(admin, times(3)).tableExists("ns", "tb");
    verify(admin, times(3)).dropTable("ns", "tb");
    verify(admin).dropNamespace("ns", true);
  }

  @Test
  public void dropCoordinatorTables_IfCoordinatorTablesExist_ShouldCallDropCoordinatorTables()
      throws Exception {
    // Arrange
    when(consensusCommitAdmin.coordinatorTablesExist()).thenReturn(true);

    // Act
    operator.dropCoordinatorTables();

    // Assert
    verify(consensusCommitAdmin).coordinatorTablesExist();
    verify(consensusCommitAdmin).dropCoordinatorTables();
  }

  @Test
  public void dropCoordinatorTables_IfCoordinatorTablesNotExist_ShouldCallNotDropCoordinatorTables()
      throws Exception {
    // Arrange
    when(consensusCommitAdmin.coordinatorTablesExist()).thenReturn(false);

    // Act
    operator.dropCoordinatorTables();

    // Assert
    verify(consensusCommitAdmin).coordinatorTablesExist();
    verify(consensusCommitAdmin, never()).dropCoordinatorTables();
  }

  @Test
  public void repairTables_WithTransactionalTables_ShouldCallProperMethods() throws Exception {
    // Arrange
    List<TableSchema> tableSchemaList = Arrays.asList(tableSchema, tableSchema, tableSchema);
    when(tableSchema.getNamespace()).thenReturn("ns");
    when(tableSchema.getOptions()).thenReturn(options);
    when(tableSchema.isTransactionalTable()).thenReturn(true);
    when(tableSchema.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableSchema.getTableMetadata()).thenReturn(tableMetadata);

    // Act
    operator.repairTables(tableSchemaList);

    // Assert
    verify(consensusCommitAdmin, times(3)).repairTable("ns", "tb", tableMetadata, options);
    verifyNoInteractions(admin);
  }

  @Test
  public void repairTables_WithoutTransactionalTables_ShouldCallProperMethods() throws Exception {
    // Arrange
    List<TableSchema> tableSchemaList = Arrays.asList(tableSchema, tableSchema, tableSchema);
    when(tableSchema.getNamespace()).thenReturn("ns");
    when(tableSchema.getOptions()).thenReturn(options);
    when(tableSchema.isTransactionalTable()).thenReturn(false);
    when(tableSchema.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableSchema.getTableMetadata()).thenReturn(tableMetadata);

    // Act
    operator.repairTables(tableSchemaList);

    // Assert
    verify(admin, times(3)).repairTable("ns", "tb", tableMetadata, options);
    verifyNoInteractions(consensusCommitAdmin);
  }

  @Test
  public void repairCoordinatorTables_ShouldCallProperMethods() throws Exception {
    // Arrange

    // Act
    operator.repairCoordinatorTables(options);

    // Assert
    verify(consensusCommitAdmin).repairCoordinatorTables(options);
    verifyNoInteractions(admin);
  }
}
