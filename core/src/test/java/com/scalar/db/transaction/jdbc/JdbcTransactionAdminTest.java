package com.scalar.db.transaction.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import java.util.Collections;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class JdbcTransactionAdminTest {

  @Mock private JdbcAdmin jdbcAdmin;
  private JdbcTransactionAdmin admin;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    admin = new JdbcTransactionAdmin(jdbcAdmin);
  }

  @Test
  public void createNamespace_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.createNamespace("ns", Collections.emptyMap());

    // Assert
    verify(jdbcAdmin).createNamespace("ns", Collections.emptyMap());
  }

  @Test
  public void createTable_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder().addColumn("c1", DataType.INT).addPartitionKey("c1").build();

    // Act
    admin.createTable("ns", "tbl", metadata, Collections.emptyMap());

    // Assert
    verify(jdbcAdmin).createTable("ns", "tbl", metadata, Collections.emptyMap());
  }

  @Test
  public void dropTable_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.dropTable("ns", "tbl");

    // Assert
    verify(jdbcAdmin).dropTable("ns", "tbl");
  }

  @Test
  public void dropNamespace_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.dropNamespace("ns");

    // Assert
    verify(jdbcAdmin).dropNamespace("ns");
  }

  @Test
  public void truncateTable_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.truncateTable("ns", "tbl");

    // Assert
    verify(jdbcAdmin).truncateTable("ns", "tbl");
  }

  @Test
  public void createIndex_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.createIndex("ns", "tbl", "col", Collections.emptyMap());

    // Assert
    verify(jdbcAdmin).createIndex("ns", "tbl", "col", Collections.emptyMap());
  }

  @Test
  public void dropIndex_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange

    // Act
    admin.dropIndex("ns", "tbl", "col");

    // Assert
    verify(jdbcAdmin).dropIndex("ns", "tbl", "col");
  }

  @Test
  public void getTableMetadata_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder().addColumn("c1", DataType.INT).addPartitionKey("c1").build();
    when(jdbcAdmin.getTableMetadata(any(), any())).thenReturn(metadata);

    // Act
    TableMetadata actual = admin.getTableMetadata("ns", "tbl");

    // Assert
    verify(jdbcAdmin).getTableMetadata("ns", "tbl");
    assertThat(actual).isEqualTo(metadata);
  }

  @Test
  public void getNamespaceTableNames_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange
    Set<String> tableNames = ImmutableSet.of("tbl1", "tbl2", "tbl3");
    when(jdbcAdmin.getNamespaceTableNames(any())).thenReturn(tableNames);

    // Act
    Set<String> actual = admin.getNamespaceTableNames("ns");

    // Assert
    verify(jdbcAdmin).getNamespaceTableNames("ns");
    assertThat(actual).isEqualTo(tableNames);
  }

  @Test
  public void namespaceExists_ShouldCallJdbcAdminProperly() throws ExecutionException {
    // Arrange
    when(jdbcAdmin.namespaceExists(any())).thenReturn(true);

    // Act
    boolean actual = admin.namespaceExists("ns");

    // Assert
    verify(jdbcAdmin).namespaceExists("ns");
    assertThat(actual).isTrue();
  }

  @Test
  public void createCoordinatorNamespaceAndTable_ShouldThrowUnsupportedOperationException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.createCoordinatorNamespaceAndTable(Collections.emptyMap()))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void dropCoordinatorNamespaceAndTable_ShouldThrowUnsupportedOperationException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.dropCoordinatorNamespaceAndTable())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void truncateCoordinatorTable_ShouldThrowUnsupportedOperationException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.truncateCoordinatorTable())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void coordinatorTableExists_ShouldThrowUnsupportedOperationException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.coordinatorTableExists())
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
