package com.scalar.db.dataloader.core.tablemetadata;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.UnitTestUtils;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TableMetadataServiceTest {

  DistributedStorageAdmin storageAdmin;
  DistributedTransactionAdmin transactionAdmin;
  TableMetadataService tableMetadataService;

  @BeforeEach
  void setup() throws ExecutionException {
    storageAdmin = Mockito.mock(DistributedStorageAdmin.class);
    Mockito.when(storageAdmin.getTableMetadata("namespace", "table"))
        .thenReturn(UnitTestUtils.createTestTableMetadata());
    transactionAdmin = Mockito.mock(DistributedTransactionAdmin.class);
    Mockito.when(transactionAdmin.getTableMetadata("namespace", "table"))
        .thenReturn(UnitTestUtils.createTestTableMetadata());
  }

  @Test
  void getTableMetadata_withStorage_withValidNamespaceAndTable_shouldReturnTableMetadataMap()
      throws TableMetadataException {
    tableMetadataService = new TableMetadataService(storageAdmin);
    Map<String, TableMetadata> expected = new HashMap<>();
    expected.put("namespace.table", UnitTestUtils.createTestTableMetadata());
    TableMetadataRequest tableMetadataRequest = new TableMetadataRequest("namespace", "table");
    Map<String, TableMetadata> output =
        tableMetadataService.getTableMetadata(Collections.singleton(tableMetadataRequest));
    Assertions.assertEquals(expected.get("namespace.table"), output.get("namespace.table"));
  }

  @Test
  void getTableMetadata_withStorage_withInvalidNamespaceAndTable_shouldThrowException() {
    tableMetadataService = new TableMetadataService(storageAdmin);
    TableMetadataRequest tableMetadataRequest = new TableMetadataRequest("namespace2", "table2");
    assertThatThrownBy(
            () ->
                tableMetadataService.getTableMetadata(Collections.singleton(tableMetadataRequest)))
        .isInstanceOf(TableMetadataException.class)
        .hasMessage(
            CoreError.DATA_LOADER_MISSING_NAMESPACE_OR_TABLE.buildMessage("namespace2", "table2"));
  }

  @Test
  void getTableMetadata_withTransaction_withValidNamespaceAndTable_shouldReturnTableMetadataMap()
      throws TableMetadataException {
    tableMetadataService = new TableMetadataService(transactionAdmin);
    Map<String, TableMetadata> expected = new HashMap<>();
    expected.put("namespace.table", UnitTestUtils.createTestTableMetadata());
    TableMetadataRequest tableMetadataRequest = new TableMetadataRequest("namespace", "table");
    Map<String, TableMetadata> output =
        tableMetadataService.getTableMetadata(Collections.singleton(tableMetadataRequest));
    Assertions.assertEquals(expected.get("namespace.table"), output.get("namespace.table"));
  }

  @Test
  void getTableMetadata_withTransaction_withInvalidNamespaceAndTable_shouldThrowException() {
    tableMetadataService = new TableMetadataService(transactionAdmin);
    TableMetadataRequest tableMetadataRequest = new TableMetadataRequest("namespace2", "table2");
    assertThatThrownBy(
            () ->
                tableMetadataService.getTableMetadata(Collections.singleton(tableMetadataRequest)))
        .isInstanceOf(TableMetadataException.class)
        .hasMessage(
            CoreError.DATA_LOADER_MISSING_NAMESPACE_OR_TABLE.buildMessage("namespace2", "table2"));
  }
}
