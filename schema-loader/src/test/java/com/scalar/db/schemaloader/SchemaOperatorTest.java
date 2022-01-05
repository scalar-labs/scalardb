package com.scalar.db.schemaloader;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SchemaOperatorTest {
  @Mock private DistributedStorageAdmin admin;
  @Mock private ConsensusCommitAdmin consensusCommitAdmin;
  @Mock private TableSchema tableSchema;
  @Mock private Map<String, String> options;

  private SchemaOperator operator;

  @Before
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
    verify(consensusCommitAdmin, times(3))
        .createTransactionalTable("ns", "tb", tableMetadata, options);
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
  public void createCoordinatorTable_IfCoordinatorTableNotExist_ShouldCallCreateCoordinatorTable()
      throws Exception {
    // Arrange
    when(consensusCommitAdmin.coordinatorTableExists()).thenReturn(false);

    // Act
    operator.createCoordinatorTable(options);

    // Assert
    verify(consensusCommitAdmin).coordinatorTableExists();
    verify(consensusCommitAdmin).createCoordinatorTable(options);
  }

  @Test
  public void createCoordinatorTable_IfCoordinatorTableExists_ShouldNotCallCreateCoordinatorTable()
      throws Exception {
    // Arrange
    when(consensusCommitAdmin.coordinatorTableExists()).thenReturn(true);

    // Act
    operator.createCoordinatorTable(options);

    // Assert
    verify(consensusCommitAdmin).coordinatorTableExists();
    verify(consensusCommitAdmin, never()).createCoordinatorTable(options);
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
  public void dropCoordinatorTable_IfCoordinatorTableExists_ShouldCallDropCoordinatorTable()
      throws Exception {
    // Arrange
    when(consensusCommitAdmin.coordinatorTableExists()).thenReturn(true);

    // Act
    operator.dropCoordinatorTable();

    // Assert
    verify(consensusCommitAdmin).coordinatorTableExists();
    verify(consensusCommitAdmin).dropCoordinatorTable();
  }

  @Test
  public void dropCoordinatorTable_IfCoordinatorTableNotExist_ShouldCallNotDropCoordinatorTable()
      throws Exception {
    // Arrange
    when(consensusCommitAdmin.coordinatorTableExists()).thenReturn(false);

    // Act
    operator.dropCoordinatorTable();

    // Assert
    verify(consensusCommitAdmin).coordinatorTableExists();
    verify(consensusCommitAdmin, never()).dropCoordinatorTable();
  }
}
