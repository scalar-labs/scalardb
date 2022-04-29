package com.scalar.db.sql;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.sql.metadata.CachedMetadata;
import com.scalar.db.sql.metadata.CachedNamespaceMetadata;
import com.scalar.db.sql.statement.CreateCoordinatorTablesStatement;
import com.scalar.db.sql.statement.CreateIndexStatement;
import com.scalar.db.sql.statement.CreateNamespaceStatement;
import com.scalar.db.sql.statement.CreateTableStatement;
import com.scalar.db.sql.statement.DropCoordinatorTablesStatement;
import com.scalar.db.sql.statement.DropIndexStatement;
import com.scalar.db.sql.statement.DropNamespaceStatement;
import com.scalar.db.sql.statement.DropTableStatement;
import com.scalar.db.sql.statement.TruncateCoordinatorTablesStatement;
import com.scalar.db.sql.statement.TruncateTableStatement;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DdlStatementExecutorTest {

  @Mock private DistributedTransactionAdmin admin;
  @Mock private CachedMetadata metadata;
  @Mock private CachedNamespaceMetadata namespaceMetadata;

  private DdlStatementExecutor ddlStatementExecutor;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(metadata.getNamespace("ns")).thenReturn(Optional.of(namespaceMetadata));
    ddlStatementExecutor = new DdlStatementExecutor(admin, metadata);
  }

  @Test
  public void execute_CreateNamespaceStatementGiven_ShouldCallAdminProperly()
      throws ExecutionException {
    // Arrange
    CreateNamespaceStatement statement =
        CreateNamespaceStatement.of(
            "ns", false, ImmutableMap.of("name1", "value1", "name2", "value2"));

    // Act
    ddlStatementExecutor.execute(statement);

    // Assert
    verify(admin)
        .createNamespace("ns", false, ImmutableMap.of("name1", "value1", "name2", "value2"));
  }

  @Test
  public void execute_CreateTableStatementGiven_ShouldCallAdminProperly()
      throws ExecutionException {
    // Arrange
    CreateTableStatement statement =
        CreateTableStatement.of(
            "ns",
            "tbl",
            true,
            ImmutableMap.<String, DataType>builder()
                .put("p", DataType.TEXT)
                .put("c", DataType.TEXT)
                .put("col1", DataType.BOOLEAN)
                .put("col2", DataType.INT)
                .build(),
            ImmutableSet.of("p"),
            ImmutableSet.of("c"),
            ImmutableMap.of("c", ClusteringOrder.DESC),
            ImmutableSet.of("col2"),
            ImmutableMap.of("name1", "value1", "name2", "value2"));

    // Act
    ddlStatementExecutor.execute(statement);

    // Assert
    verify(admin)
        .createTable(
            "ns",
            "tbl",
            TableMetadata.newBuilder()
                .addColumn("p", com.scalar.db.io.DataType.TEXT)
                .addColumn("c", com.scalar.db.io.DataType.TEXT)
                .addColumn("col1", com.scalar.db.io.DataType.BOOLEAN)
                .addColumn("col2", com.scalar.db.io.DataType.INT)
                .addPartitionKey("p")
                .addClusteringKey("c", Scan.Ordering.Order.DESC)
                .addSecondaryIndex("col2")
                .build(),
            true,
            ImmutableMap.of("name1", "value1", "name2", "value2"));

    verify(namespaceMetadata).invalidateTableNamesCache();
  }

  @Test
  public void execute_DropNamespaceStatementGiven_ShouldCallAdminProperly()
      throws ExecutionException {
    // Arrange
    DropNamespaceStatement statement = DropNamespaceStatement.of("ns", true, true);
    when(admin.getNamespaceTableNames("ns")).thenReturn(ImmutableSet.of("tbl1", "tbl2", "tbl3"));

    // Act
    ddlStatementExecutor.execute(statement);

    // Assert
    verify(admin).dropTable("ns", "tbl1");
    verify(admin).dropTable("ns", "tbl2");
    verify(admin).dropTable("ns", "tbl3");
    verify(admin).dropNamespace("ns", true);

    verify(metadata).invalidateCache("ns");
  }

  @Test
  public void execute_DropTableStatementGiven_ShouldCallAdminProperly() throws ExecutionException {
    // Arrange
    DropTableStatement statement = DropTableStatement.of("ns", "tbl", true);

    // Act
    ddlStatementExecutor.execute(statement);

    // Assert
    verify(admin).dropTable("ns", "tbl", true);

    verify(namespaceMetadata).invalidateTableNamesCache();
    verify(namespaceMetadata).invalidateTableMetadataCache("tbl");
  }

  @Test
  public void execute_TruncateTableStatementGiven_ShouldCallAdminProperly()
      throws ExecutionException {
    // Arrange
    TruncateTableStatement statement = TruncateTableStatement.of("ns", "tbl");

    // Act
    ddlStatementExecutor.execute(statement);

    // Assert
    verify(admin).truncateTable("ns", "tbl");
  }

  @Test
  public void execute_CreateCoordinatorTableStatementGiven_ShouldCallAdminProperly()
      throws ExecutionException {
    // Arrange
    CreateCoordinatorTablesStatement statement =
        CreateCoordinatorTablesStatement.of(
            true, ImmutableMap.of("name1", "value1", "name2", "value2"));

    // Act
    ddlStatementExecutor.execute(statement);

    // Assert
    verify(admin)
        .createCoordinatorTables(true, ImmutableMap.of("name1", "value1", "name2", "value2"));
  }

  @Test
  public void execute_DropCoordinatorTableStatementGiven_ShouldCallAdminProperly()
      throws ExecutionException {
    // Arrange
    DropCoordinatorTablesStatement statement = DropCoordinatorTablesStatement.of(true);

    // Act
    ddlStatementExecutor.execute(statement);

    // Assert
    verify(admin).dropCoordinatorTables(true);
  }

  @Test
  public void execute_TruncateCoordinatorTableStatementGiven_ShouldCallAdminProperly()
      throws ExecutionException {
    // Arrange
    TruncateCoordinatorTablesStatement statement = TruncateCoordinatorTablesStatement.of();

    // Act
    ddlStatementExecutor.execute(statement);

    // Assert
    verify(admin).truncateCoordinatorTables();
  }

  @Test
  public void execute_CreateIndexStatementStatementGiven_ShouldCallAdminProperly()
      throws ExecutionException {
    // Arrange
    CreateIndexStatement statement =
        CreateIndexStatement.of(
            "ns", "tbl", "col", true, ImmutableMap.of("name1", "value1", "name2", "value2"));

    // Act
    ddlStatementExecutor.execute(statement);

    // Assert
    verify(admin)
        .createIndex(
            "ns", "tbl", "col", true, ImmutableMap.of("name1", "value1", "name2", "value2"));

    verify(namespaceMetadata).invalidateTableMetadataCache("tbl");
  }

  @Test
  public void execute_DropIndexStatementGiven_ShouldCallAdminProperly() throws ExecutionException {
    // Arrange
    DropIndexStatement statement = DropIndexStatement.of("ns", "tbl", "col", true);

    // Act
    ddlStatementExecutor.execute(statement);

    // Assert
    verify(admin).dropIndex("ns", "tbl", "col", true);

    verify(namespaceMetadata).invalidateTableMetadataCache("tbl");
  }
}
