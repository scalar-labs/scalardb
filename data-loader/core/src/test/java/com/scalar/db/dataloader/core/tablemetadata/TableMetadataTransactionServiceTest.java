package com.scalar.db.dataloader.core.tablemetadata;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.DataLoaderError;
import com.scalar.db.dataloader.core.UnitTestUtils;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TableMetadataTransactionServiceTest {

  DistributedTransactionAdmin transactionAdmin;
  TableMetadataService tableMetadataService;

  @BeforeEach
  void setup() throws ExecutionException {
    transactionAdmin = Mockito.mock(DistributedTransactionAdmin.class);
    Mockito.when(transactionAdmin.getTableMetadata("namespace", "table"))
        .thenReturn(UnitTestUtils.createTestTableMetadata());

    tableMetadataService = new TableMetadataTransactionService(transactionAdmin);
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
  void getTableMetadata_withInvalidNamespaceAndTable_shouldThrowException()
      throws ExecutionException {
    Mockito.when(transactionAdmin.getTableMetadata("namespace2", "table2")).thenReturn(null);

    TableMetadataRequest tableMetadataRequest = new TableMetadataRequest("namespace2", "table2");
    assertThatThrownBy(
            () ->
                tableMetadataService.getTableMetadata(Collections.singleton(tableMetadataRequest)))
        .isInstanceOf(TableMetadataException.class)
        .hasMessage(
            DataLoaderError.MISSING_NAMESPACE_OR_TABLE.buildMessage("namespace2", "table2"));
  }
}
