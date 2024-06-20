package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CheckedDistributedStorageAdminTest {

  @Mock private DistributedStorageAdmin admin;

  private CheckedDistributedStorageAdmin checkedAdmin;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    checkedAdmin = new CheckedDistributedStorageAdmin(admin);
  }

  @Test
  public void createTable_ShouldCallAdminProperly() throws ExecutionException {
    // Arrange
    String namespaceName = "ns";
    String tableName = "tbl";
    TableMetadata tableMetadata = mock(TableMetadata.class);
    Map<String, String> options = ImmutableMap.of("name", "value");

    when(admin.namespaceExists(namespaceName)).thenReturn(true);
    when(admin.tableExists(namespaceName, tableName)).thenReturn(true);

    // Act
    checkedAdmin.createTable(namespaceName, tableName, tableMetadata, options);

    // Assert
    verify(admin).createTable(namespaceName, tableName, tableMetadata, options);
  }

  @Test
  public void
      createTable_TableMetadataWithEncryptedColumns_ShouldThrowUnsupportedOperationException()
          throws ExecutionException {
    // Arrange
    String namespaceName = "ns";
    String tableName = "tbl";

    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.getEncryptedColumnNames()).thenReturn(Collections.singleton("col"));

    Map<String, String> options = ImmutableMap.of("name", "value");

    when(admin.namespaceExists(namespaceName)).thenReturn(true);
    when(admin.tableExists(namespaceName, tableName)).thenReturn(true);

    // Act Assert
    assertThatThrownBy(
            () -> checkedAdmin.createTable(namespaceName, tableName, tableMetadata, options))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
