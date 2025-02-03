package com.scalar.db.dataloader.core.tablemetadata;

import static com.scalar.db.dataloader.core.ErrorMessage.MISSING_NAMESPACE_OR_TABLE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
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
  TableMetadataService tableMetadataService;

  @BeforeEach
  void setup() throws ExecutionException {
    storageAdmin = Mockito.mock(DistributedStorageAdmin.class);
    Mockito.when(storageAdmin.getTableMetadata("namespace", "table"))
        .thenReturn(UnitTestUtils.createTestTableMetadata());
    tableMetadataService = new TableMetadataService(storageAdmin);
  }

  @Test
  void getTableMetadata_withValidNamespaceAndTable_shouldReturnTableMetadataMap()
      throws TableMetadataException {

    Map<String, TableMetadata> expected = new HashMap<>();
    expected.put("namespace.table", UnitTestUtils.createTestTableMetadata());
    TableMetadataRequest tableMetadataRequest = new TableMetadataRequest("namespace", "table");
    Map<String, TableMetadata> output =
        tableMetadataService.getTableMetadata(Collections.singleton(tableMetadataRequest));
    Assertions.assertEquals(expected.get("namespace.table"), output.get("namespace.table"));
  }

  @Test
  void getTableMetadata_withInvalidNamespaceAndTable_shouldThrowException() {
    TableMetadataRequest tableMetadataRequest = new TableMetadataRequest("namespace2", "table2");
    assertThatThrownBy(
            () ->
                tableMetadataService.getTableMetadata(Collections.singleton(tableMetadataRequest)))
        .isInstanceOf(TableMetadataException.class)
        .hasMessage(String.format(MISSING_NAMESPACE_OR_TABLE, "namespace2", "table2"));
  }
}
