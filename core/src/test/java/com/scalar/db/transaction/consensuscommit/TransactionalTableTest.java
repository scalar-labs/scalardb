package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import org.junit.Test;

public class TransactionalTableTest {

  @Test
  public void convertToTransactionalTable_tableMetadataGiven_shouldReturnProperTableMetadata() {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String BALANCE = "balance";

    TableMetadata expected =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addColumn(Attribute.ID, DataType.TEXT)
            .addColumn(Attribute.STATE, DataType.INT)
            .addColumn(Attribute.VERSION, DataType.INT)
            .addColumn(Attribute.PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.COMMITTED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_PREFIX + BALANCE, DataType.INT)
            .addColumn(Attribute.BEFORE_ID, DataType.TEXT)
            .addColumn(Attribute.BEFORE_STATE, DataType.INT)
            .addColumn(Attribute.BEFORE_VERSION, DataType.INT)
            .addColumn(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();

    // Act
    TableMetadata actual =
        TransactionalTable.convertToTransactionalTable(
            TableMetadata.newBuilder()
                .addColumn(ACCOUNT_ID, DataType.INT)
                .addColumn(ACCOUNT_TYPE, DataType.INT)
                .addColumn(BALANCE, DataType.INT)
                .addPartitionKey(ACCOUNT_ID)
                .addClusteringKey(ACCOUNT_TYPE)
                .build());

    // Assert
    assertThat(actual).isEqualTo(expected);
  }
}
