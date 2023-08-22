package com.scalar.db.schemaloader;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.schemaloader.alteration.TableMetadataAlteration;
import com.scalar.db.schemaloader.alteration.TableMetadataAlterationProcessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SchemaOperatorTest {

  @Mock private DistributedStorageAdmin storageAdmin;
  @Mock private DistributedTransactionAdmin transactionAdmin;
  @Mock private TableMetadataAlterationProcessor alterationProcessor;
  @Mock private TableSchema tableSchema;
  @Mock private Map<String, String> options;
  @Mock private TableMetadataAlteration metadataAlteration;
  private SchemaOperator operator;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    operator = new SchemaOperator(storageAdmin, transactionAdmin, alterationProcessor);
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

  @Test
  public void alterTables_WithoutChanges_ShouldDoNothing()
      throws SchemaLoaderException, ExecutionException {
    // Arrange
    List<TableSchema> tableSchemaList = Collections.singletonList(tableSchema);
    String namespace = "ns";
    String table = "tb";
    when(storageAdmin.tableExists(anyString(), anyString())).thenReturn(true);
    when(tableSchema.getNamespace()).thenReturn(namespace);
    when(tableSchema.getOptions()).thenReturn(options);
    when(tableSchema.isTransactionTable()).thenReturn(true);
    when(tableSchema.getTable()).thenReturn(table);
    TableMetadata oldMetadata = mock(TableMetadata.class);
    when(tableSchema.getTableMetadata()).thenReturn(oldMetadata);
    when(metadataAlteration.hasAlterations()).thenReturn(false);
    when(alterationProcessor.computeAlteration(anyString(), anyString(), any(), any()))
        .thenReturn(metadataAlteration);
    TableMetadata newMetadata = mock(TableMetadata.class);
    when(transactionAdmin.getTableMetadata(anyString(), anyString())).thenReturn(newMetadata);

    // Act
    operator.alterTables(tableSchemaList, options);

    // Assert
    verify(storageAdmin).tableExists(namespace, table);
    verify(transactionAdmin).getTableMetadata(namespace, table);
    verifyNoMoreInteractions(transactionAdmin);
    verifyNoMoreInteractions(storageAdmin);
    verify(alterationProcessor)
        .computeAlteration(eq("ns"), eq("tb"), refEq(oldMetadata), refEq(newMetadata));
  }

  @Test
  public void alterTables_WithTwoTablesButChangesOnOneTableOnly_ShouldCallAdminCorrectly()
      throws SchemaLoaderException, ExecutionException {
    // Arrange
    TableSchema tableSchema1 = mock(TableSchema.class);
    TableSchema tableSchema2 = mock(TableSchema.class);
    List<TableSchema> tableSchemaList = Arrays.asList(tableSchema1, tableSchema2);

    // Table 1
    String namespace1 = "ns1";
    String table1 = "tb1";

    when(storageAdmin.tableExists(anyString(), anyString())).thenReturn(true);
    when(tableSchema1.getNamespace()).thenReturn(namespace1);
    when(tableSchema1.isTransactionTable()).thenReturn(false);
    when(tableSchema1.getTable()).thenReturn(table1);
    TableMetadata oldMetadataForTable1 = mock(TableMetadata.class);
    TableMetadata newMetadataForTable1 = mock(TableMetadata.class);
    when(tableSchema1.getTableMetadata()).thenReturn(oldMetadataForTable1);
    TableMetadataAlteration metadataAlteration1 = mock(TableMetadataAlteration.class);
    when(metadataAlteration1.hasAlterations()).thenReturn(false);

    // Table 2
    String namespace2 = "ns2";
    String table2 = "tb2";

    when(tableSchema2.getNamespace()).thenReturn(namespace2);
    when(tableSchema2.isTransactionTable()).thenReturn(true);
    when(tableSchema2.getTable()).thenReturn(table2);
    TableMetadata oldMetadataForTable2 = mock(TableMetadata.class);
    TableMetadata newMetadataForTable2 = mock(TableMetadata.class);
    when(tableSchema2.getTableMetadata()).thenReturn(oldMetadataForTable2);
    TableMetadataAlteration metadataAlteration2 = mock(TableMetadataAlteration.class);
    when(metadataAlteration2.hasAlterations()).thenReturn(true);
    LinkedHashSet<String> addedColumnsOfTable2 = new LinkedHashSet<>();
    addedColumnsOfTable2.add("c4");
    addedColumnsOfTable2.add("c5");
    when(metadataAlteration2.getAddedColumnNames()).thenReturn(addedColumnsOfTable2);
    when(metadataAlteration2.getAddedColumnDataTypes())
        .thenReturn(ImmutableMap.of("c4", DataType.FLOAT, "c5", DataType.INT));
    when(metadataAlteration2.getAddedSecondaryIndexNames()).thenReturn(ImmutableSet.of("c4", "c5"));
    when(metadataAlteration2.getDeletedSecondaryIndexNames())
        .thenReturn(ImmutableSet.of("c2", "c3"));

    when(storageAdmin.getTableMetadata(anyString(), anyString())).thenReturn(oldMetadataForTable1);
    when(transactionAdmin.getTableMetadata(anyString(), anyString()))
        .thenReturn(newMetadataForTable2);

    when(alterationProcessor.computeAlteration(anyString(), anyString(), any(), any()))
        .thenReturn(metadataAlteration1)
        .thenReturn(metadataAlteration2);

    // Act
    operator.alterTables(tableSchemaList, options);

    // Assert
    // Table 1
    verify(storageAdmin).tableExists(namespace1, table1);
    verify(storageAdmin).getTableMetadata(namespace1, table1);
    verify(alterationProcessor)
        .computeAlteration(
            eq(namespace1), eq(table1), refEq(oldMetadataForTable1), refEq(newMetadataForTable1));

    // Table 2
    verify(storageAdmin).tableExists(namespace2, table2);
    verify(transactionAdmin).getTableMetadata(namespace2, table2);
    verify(alterationProcessor)
        .computeAlteration(
            eq(namespace2), eq(table2), refEq(oldMetadataForTable2), refEq(newMetadataForTable2));
    verify(transactionAdmin).addNewColumnToTable(namespace2, table2, "c4", DataType.FLOAT);
    verify(transactionAdmin).addNewColumnToTable(namespace2, table2, "c5", DataType.INT);
    verify(transactionAdmin).createIndex(namespace2, table2, "c4", options);
    verify(transactionAdmin).createIndex(namespace2, table2, "c5", options);
    verify(transactionAdmin).dropIndex(namespace2, table2, "c2");
    verify(transactionAdmin).dropIndex(namespace2, table2, "c3");
    verifyNoMoreInteractions(transactionAdmin);
    verifyNoMoreInteractions(storageAdmin);
  }

  @Test
  public void alterTables_WithChangesOnTwoTables_ShouldCallAdminCorrectly()
      throws SchemaLoaderException, ExecutionException {
    // Arrange
    TableSchema tableSchema1 = mock(TableSchema.class);
    TableSchema tableSchema2 = mock(TableSchema.class);
    List<TableSchema> tableSchemaList = Arrays.asList(tableSchema1, tableSchema2);

    // Table 1
    String namespace1 = "ns1";
    String table1 = "tb1";

    when(storageAdmin.tableExists(anyString(), anyString())).thenReturn(true);
    when(tableSchema1.getNamespace()).thenReturn(namespace1);
    when(tableSchema1.isTransactionTable()).thenReturn(false);
    when(tableSchema1.getTable()).thenReturn(table1);
    TableMetadata oldMetadataForTable1 = mock(TableMetadata.class);
    TableMetadata newMetadataForTable1 = mock(TableMetadata.class);
    when(tableSchema1.getTableMetadata()).thenReturn(oldMetadataForTable1);
    TableMetadataAlteration metadataAlteration1 = mock(TableMetadataAlteration.class);
    when(metadataAlteration1.hasAlterations()).thenReturn(true);
    LinkedHashSet<String> addedColumnsOfTable1 = new LinkedHashSet<>();
    addedColumnsOfTable1.add("c2");
    when(metadataAlteration1.getAddedColumnNames()).thenReturn(addedColumnsOfTable1);
    when(metadataAlteration1.getAddedColumnDataTypes())
        .thenReturn(ImmutableMap.of("c2", DataType.BOOLEAN));
    when(metadataAlteration1.getAddedSecondaryIndexNames()).thenReturn(Collections.emptySet());
    when(metadataAlteration1.getDeletedSecondaryIndexNames()).thenReturn(Collections.emptySet());

    // Table 2
    String namespace2 = "ns2";
    String table2 = "tb2";

    when(tableSchema2.getNamespace()).thenReturn(namespace2);
    when(tableSchema2.isTransactionTable()).thenReturn(true);
    when(tableSchema2.getTable()).thenReturn(table2);
    TableMetadata oldMetadataForTable2 = mock(TableMetadata.class);
    TableMetadata newMetadataForTable2 = mock(TableMetadata.class);
    when(tableSchema2.getTableMetadata()).thenReturn(oldMetadataForTable2);
    TableMetadataAlteration metadataAlteration2 = mock(TableMetadataAlteration.class);
    when(metadataAlteration2.hasAlterations()).thenReturn(true);
    LinkedHashSet<String> addedColumnsOfTable2 = new LinkedHashSet<>();
    addedColumnsOfTable2.add("c4");
    when(metadataAlteration2.getAddedColumnNames()).thenReturn(addedColumnsOfTable2);
    when(metadataAlteration2.getAddedColumnDataTypes())
        .thenReturn(ImmutableMap.of("c4", DataType.FLOAT));
    when(metadataAlteration2.getAddedSecondaryIndexNames()).thenReturn(ImmutableSet.of("c4"));
    when(metadataAlteration2.getDeletedSecondaryIndexNames()).thenReturn(ImmutableSet.of("c3"));

    when(storageAdmin.getTableMetadata(anyString(), anyString())).thenReturn(oldMetadataForTable1);
    when(transactionAdmin.getTableMetadata(anyString(), anyString()))
        .thenReturn(newMetadataForTable2);

    when(alterationProcessor.computeAlteration(anyString(), anyString(), any(), any()))
        .thenReturn(metadataAlteration1)
        .thenReturn(metadataAlteration2);

    // Act
    operator.alterTables(tableSchemaList, options);

    // Assert
    // Table 1
    verify(storageAdmin).tableExists(namespace1, table1);
    verify(storageAdmin).getTableMetadata(namespace1, table1);
    verify(alterationProcessor)
        .computeAlteration(
            eq(namespace1), eq(table1), refEq(oldMetadataForTable1), refEq(newMetadataForTable1));
    verify(storageAdmin).addNewColumnToTable(namespace1, table1, "c2", DataType.BOOLEAN);

    // Table 2
    verify(storageAdmin).tableExists(namespace2, table2);
    verify(transactionAdmin).getTableMetadata(namespace2, table2);
    verify(alterationProcessor)
        .computeAlteration(
            eq(namespace2), eq(table2), refEq(oldMetadataForTable2), refEq(newMetadataForTable2));
    verify(transactionAdmin).addNewColumnToTable(namespace2, table2, "c4", DataType.FLOAT);
    verify(transactionAdmin).createIndex(namespace2, table2, "c4", options);
    verify(transactionAdmin).dropIndex(namespace2, table2, "c3");
    verifyNoMoreInteractions(transactionAdmin);
    verifyNoMoreInteractions(storageAdmin);
  }
}
