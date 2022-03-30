package com.scalar.db.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MetadataTest {

  @Mock private DistributedTransactionAdmin admin;
  private Metadata metadata;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    metadata = new Metadata(admin);
  }

  @Test
  public void getNamespace_NonExistingNamespaceGiven_ShouldReturnProperNamespace() {
    // Arrange

    // Act
    Optional<NamespaceMetadata> namespace = metadata.getNamespace("ns");

    // Assert
    assertThat(namespace).isNotPresent();
  }

  @Test
  public void getNamespace_ExistingNamespaceGiven_ShouldReturnProperNamespace()
      throws ExecutionException {
    // Arrange
    when(admin.namespaceExists("ns")).thenReturn(true);

    // Act
    Optional<NamespaceMetadata> namespace = metadata.getNamespace("ns");

    // Assert
    assertThat(namespace).isPresent();
    assertThat(namespace.get().getName()).isEqualTo("ns");
  }

  @Test
  public void getTables_ShouldReturnProperTables() throws ExecutionException {
    // Arrange
    when(admin.namespaceExists("ns")).thenReturn(true);
    when(admin.getNamespaceTableNames("ns")).thenReturn(ImmutableSet.of("tbl1", "tbl2", "tbl3"));

    com.scalar.db.api.TableMetadata metadata =
        com.scalar.db.api.TableMetadata.newBuilder()
            .addColumn("col1", com.scalar.db.io.DataType.INT)
            .addColumn("col2", com.scalar.db.io.DataType.TEXT)
            .addPartitionKey("col1")
            .build();
    when(admin.getTableMetadata("ns", "tbl1")).thenReturn(metadata);
    when(admin.getTableMetadata("ns", "tbl2")).thenReturn(metadata);
    when(admin.getTableMetadata("ns", "tbl3")).thenReturn(metadata);

    Optional<NamespaceMetadata> namespace = this.metadata.getNamespace("ns");
    assertThat(namespace).isPresent();

    // Act
    Map<String, TableMetadata> tables = namespace.get().getTables();
    Optional<TableMetadata> table1 = namespace.get().getTable("tbl1");
    Optional<TableMetadata> table2 = namespace.get().getTable("tbl2");
    Optional<TableMetadata> table3 = namespace.get().getTable("tbl3");
    Optional<TableMetadata> table4 = namespace.get().getTable("tbl4");

    // Assert
    assertThat(tables.keySet()).isEqualTo(ImmutableSet.of("tbl1", "tbl2", "tbl3"));
    assertThat(tables.get("tbl1").getNamespaceName()).isEqualTo("ns");
    assertThat(tables.get("tbl1").getName()).isEqualTo("tbl1");
    assertThat(tables.get("tbl2").getNamespaceName()).isEqualTo("ns");
    assertThat(tables.get("tbl2").getName()).isEqualTo("tbl2");
    assertThat(tables.get("tbl3").getNamespaceName()).isEqualTo("ns");
    assertThat(tables.get("tbl3").getName()).isEqualTo("tbl3");

    assertThat(table1).isPresent();
    assertThat(table1.get().getNamespaceName()).isEqualTo("ns");
    assertThat(table1.get().getName()).isEqualTo("tbl1");
    assertThat(table2).isPresent();
    assertThat(table2.get().getNamespaceName()).isEqualTo("ns");
    assertThat(table2.get().getName()).isEqualTo("tbl2");
    assertThat(table3).isPresent();
    assertThat(table3.get().getNamespaceName()).isEqualTo("ns");
    assertThat(table3.get().getName()).isEqualTo("tbl3");
    assertThat(table4).isNotPresent();
  }

  @Test
  public void getTable_ShouldReturnProperTables() throws ExecutionException {
    // Arrange
    when(admin.namespaceExists("ns")).thenReturn(true);
    when(admin.getTableMetadata("ns", "tbl"))
        .thenReturn(
            com.scalar.db.api.TableMetadata.newBuilder()
                .addColumn("col1", com.scalar.db.io.DataType.INT)
                .addColumn("col2", com.scalar.db.io.DataType.TEXT)
                .addColumn("col3", com.scalar.db.io.DataType.INT)
                .addPartitionKey("col1")
                .addClusteringKey("col2", Scan.Ordering.Order.DESC)
                .addSecondaryIndex("col3")
                .build());

    Optional<NamespaceMetadata> namespace = metadata.getNamespace("ns");
    assertThat(namespace).isPresent();

    // Act
    Optional<TableMetadata> table = namespace.get().getTable("tbl");

    // Assert
    assertThat(table).isPresent();
    assertThat(table.get().getNamespaceName()).isEqualTo("ns");
    assertThat(table.get().getName()).isEqualTo("tbl");

    assertThat(table.get().getPrimaryKey().size()).isEqualTo(2);
    assertThat(table.get().getPrimaryKey().get(0).getNamespaceName()).isEqualTo("ns");
    assertThat(table.get().getPrimaryKey().get(0).getTableName()).isEqualTo("tbl");
    assertThat(table.get().getPrimaryKey().get(0).getName()).isEqualTo("col1");
    assertThat(table.get().getPrimaryKey().get(0).getDataType()).isEqualTo(DataType.INT);
    assertThat(table.get().getPrimaryKey().get(1).getNamespaceName()).isEqualTo("ns");
    assertThat(table.get().getPrimaryKey().get(1).getTableName()).isEqualTo("tbl");
    assertThat(table.get().getPrimaryKey().get(1).getName()).isEqualTo("col2");
    assertThat(table.get().getPrimaryKey().get(1).getDataType()).isEqualTo(DataType.TEXT);

    assertThat(table.get().getPartitionKey().size()).isEqualTo(1);
    assertThat(table.get().getPartitionKey().get(0).getNamespaceName()).isEqualTo("ns");
    assertThat(table.get().getPartitionKey().get(0).getTableName()).isEqualTo("tbl");
    assertThat(table.get().getPartitionKey().get(0).getName()).isEqualTo("col1");
    assertThat(table.get().getPartitionKey().get(0).getDataType()).isEqualTo(DataType.INT);

    assertThat(table.get().getClusteringKey().size()).isEqualTo(1);
    Entry<ColumnMetadata, ClusteringOrder> entry =
        table.get().getClusteringKey().entrySet().iterator().next();
    assertThat(entry.getKey().getNamespaceName()).isEqualTo("ns");
    assertThat(entry.getKey().getTableName()).isEqualTo("tbl");
    assertThat(entry.getKey().getName()).isEqualTo("col2");
    assertThat(entry.getKey().getDataType()).isEqualTo(DataType.TEXT);
    assertThat(entry.getValue()).isEqualTo(ClusteringOrder.DESC);

    Map<String, ColumnMetadata> columns = table.get().getColumns();
    assertThat(columns.keySet()).isEqualTo(ImmutableSet.of("col1", "col2", "col3"));
    assertThat(columns.get("col1").getNamespaceName()).isEqualTo("ns");
    assertThat(columns.get("col1").getTableName()).isEqualTo("tbl");
    assertThat(columns.get("col1").getName()).isEqualTo("col1");
    assertThat(columns.get("col1").getDataType()).isEqualTo(DataType.INT);
    assertThat(columns.get("col2").getNamespaceName()).isEqualTo("ns");
    assertThat(columns.get("col2").getTableName()).isEqualTo("tbl");
    assertThat(columns.get("col2").getName()).isEqualTo("col2");
    assertThat(columns.get("col2").getDataType()).isEqualTo(DataType.TEXT);
    assertThat(columns.get("col3").getNamespaceName()).isEqualTo("ns");
    assertThat(columns.get("col3").getTableName()).isEqualTo("tbl");
    assertThat(columns.get("col3").getName()).isEqualTo("col3");
    assertThat(columns.get("col3").getDataType()).isEqualTo(DataType.INT);

    Optional<ColumnMetadata> column1 = table.get().getColumn("col1");
    assertThat(column1).isPresent();
    assertThat(column1.get().getNamespaceName()).isEqualTo("ns");
    assertThat(column1.get().getTableName()).isEqualTo("tbl");
    assertThat(column1.get().getName()).isEqualTo("col1");
    assertThat(column1.get().getDataType()).isEqualTo(DataType.INT);

    Optional<ColumnMetadata> column2 = table.get().getColumn("col2");
    assertThat(column2).isPresent();
    assertThat(column2.get().getNamespaceName()).isEqualTo("ns");
    assertThat(column2.get().getTableName()).isEqualTo("tbl");
    assertThat(column2.get().getName()).isEqualTo("col2");
    assertThat(column2.get().getDataType()).isEqualTo(DataType.TEXT);

    Optional<ColumnMetadata> column3 = table.get().getColumn("col3");
    assertThat(column3).isPresent();
    assertThat(column3.get().getNamespaceName()).isEqualTo("ns");
    assertThat(column3.get().getTableName()).isEqualTo("tbl");
    assertThat(column3.get().getName()).isEqualTo("col3");
    assertThat(column3.get().getDataType()).isEqualTo(DataType.INT);

    assertThat(table.get().getColumn("col4")).isNotPresent();

    assertThat(table.get().getIndexes().keySet()).isEqualTo(ImmutableSet.of("col3"));
    assertThat(table.get().getIndexes().get("col3").getNamespaceName()).isEqualTo("ns");
    assertThat(table.get().getIndexes().get("col3").getTableName()).isEqualTo("tbl");
    assertThat(table.get().getIndexes().get("col3").getColumnName()).isEqualTo("col3");

    Optional<IndexMetadata> index = table.get().getIndex("col3");
    assertThat(index).isPresent();
    assertThat(index.get().getNamespaceName()).isEqualTo("ns");
    assertThat(index.get().getTableName()).isEqualTo("tbl");
    assertThat(index.get().getColumnName()).isEqualTo("col3");

    assertThat(table.get().getIndex("col2")).isNotPresent();
  }
}
