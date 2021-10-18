package com.scalar.db.schemaloader.core;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.schemaloader.schema.Table;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SchemaOperatorTest {
  @Mock private DistributedStorageAdmin admin;
  @Mock private ConsensusCommitAdmin consensusCommitAdmin;
  @Mock private Table table;
  private SchemaOperator operator;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @After
  public void tearDown() {
    operator.close();
  }

  @Test
  public void createTables_WithProperTableList_ShouldCallAdminCreateNameSpace()
      throws ExecutionException {
    // Arrange
    operator = new SchemaOperator(admin, consensusCommitAdmin, true);
    List<Table> tableList = Arrays.asList(table, table, table);
    when(table.getNamespace()).thenReturn("ns");
    when(table.getOptions()).thenReturn(Collections.emptyMap());

    // Act
    operator.createTables(tableList, Collections.emptyMap());

    // Assert
    verify(admin, times(3)).createNamespace("ns", true, Collections.emptyMap());
  }

  @Test
  public void
      createTables_WithTableListContainTransactionalTable_ShouldCallConsensusCommitAdminCreateTransactionalTable()
          throws ExecutionException {
    // Arrange
    operator = new SchemaOperator(admin, consensusCommitAdmin, true);
    List<Table> tableList = Arrays.asList(table, table, table);
    when(table.getNamespace()).thenReturn("ns");
    when(table.getOptions()).thenReturn(Collections.emptyMap());
    when(table.isTransactionTable()).thenReturn(true);
    when(table.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(table.getTableMetadata()).thenReturn(tableMetadata);

    // Act
    operator.createTables(tableList, Collections.emptyMap());

    // Assert
    verify(consensusCommitAdmin, times(3))
        .createTransactionalTable("ns", "tb", tableMetadata, Collections.emptyMap());
  }

  @Test
  public void createTables_WithTableListContainNonTransactionalTable_ShouldCallAdminCreateTable()
      throws ExecutionException {
    // Arrange
    operator = new SchemaOperator(admin, consensusCommitAdmin, true);
    List<Table> tableList = Arrays.asList(table, table, table);
    when(table.getNamespace()).thenReturn("ns");
    when(table.getOptions()).thenReturn(Collections.emptyMap());
    when(table.isTransactionTable()).thenReturn(false);
    when(table.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(table.getTableMetadata()).thenReturn(tableMetadata);

    // Act
    operator.createTables(tableList, Collections.emptyMap());

    // Assert
    verify(admin, times(3)).createTable("ns", "tb", tableMetadata, Collections.emptyMap());
  }

  @Test
  public void
      createTables_WithTableListContainTransactionalTableAndIsStorageCommandSpecific_ShouldCallConsensusCommitAdminCreateCoordinatorTable()
          throws ExecutionException {
    // Arrange
    operator = new SchemaOperator(admin, consensusCommitAdmin, true);
    List<Table> tableList = Arrays.asList(table, table, table);
    when(table.getNamespace()).thenReturn("ns");
    when(table.getOptions()).thenReturn(Collections.emptyMap());
    when(table.isTransactionTable()).thenReturn(true);
    when(table.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(table.getTableMetadata()).thenReturn(tableMetadata);

    // Act
    operator.createTables(tableList, Collections.emptyMap());

    // Assert
    verify(consensusCommitAdmin).createCoordinatorTable(Collections.emptyMap());
  }

  @Test
  public void
      deleteTables_WithTableListAndNamespaceContainsOnlyProvidedTables_ShouldCallAdminDropTableAndDropNamespace()
          throws ExecutionException {
    // Arrange
    operator = new SchemaOperator(admin, consensusCommitAdmin, true);
    List<Table> tableList = Arrays.asList(table, table, table);
    when(table.getNamespace()).thenReturn("ns");
    when(table.getOptions()).thenReturn(Collections.emptyMap());
    when(table.isTransactionTable()).thenReturn(true);
    when(table.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(table.getTableMetadata()).thenReturn(tableMetadata);
    when(admin.getNamespaceTableNames("ns")).thenReturn(Collections.emptySet());
    // Act
    operator.deleteTables(tableList);

    // Assert
    verify(admin, times(3)).dropTable("ns", "tb");
    verify(admin).dropNamespace("ns");
  }

  @Test
  public void
      deleteTables_WithTableListContainTransactionalTableAndIsStorageCommandSpecific_ShouldCallConsensusCommitAdminDropCoordinatorTable()
          throws ExecutionException {
    // Arrange
    operator = new SchemaOperator(admin, consensusCommitAdmin, true);
    List<Table> tableList = Arrays.asList(table, table, table);
    when(table.getNamespace()).thenReturn("ns");
    when(table.getOptions()).thenReturn(Collections.emptyMap());
    when(table.isTransactionTable()).thenReturn(true);
    when(table.getTable()).thenReturn("tb");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(table.getTableMetadata()).thenReturn(tableMetadata);

    // Act
    operator.deleteTables(tableList);

    // Assert
    verify(consensusCommitAdmin).dropCoordinatorTable();
  }
}
