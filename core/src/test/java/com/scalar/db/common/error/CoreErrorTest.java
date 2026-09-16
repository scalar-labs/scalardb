package com.scalar.db.common.error;

import com.scalar.db.api.Put;
import com.scalar.db.common.CoreError;
import com.scalar.db.io.Key;
import java.util.Arrays;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class CoreErrorTest {

  @Test
  public void checkDuplicateErrorCode() {
    Assertions.assertThat(Arrays.stream(CoreError.values()).map(CoreError::buildCode))
        .doesNotHaveDuplicates();
  }

  @Test
  public void buildCode_ShouldBuildCorrectCode() {
    // Arrange
    CoreError error = CoreError.OPERATION_CHECK_ERROR_INDEX_ONLY_SINGLE_COLUMN_INDEX_SUPPORTED;

    // Act
    String code = error.buildCode();

    // Assert
    Assertions.assertThat(code).isEqualTo("DB-CORE-10000");
  }

  @Test
  public void buildCode_ForTransactionNotFound_ShouldBeConcurrencyErrorCode() {
    // TRANSACTION_NOT_FOUND is intentionally categorized as a CONCURRENCY_ERROR (an expired or
    // unknown transaction is retriable), not a USER_ERROR. Pin the category and the resulting code
    // so the intentional recategorization is not silently reverted.
    Assertions.assertThat(CoreError.TRANSACTION_NOT_FOUND.getCategory())
        .isEqualTo(Category.CONCURRENCY_ERROR);
    Assertions.assertThat(CoreError.TRANSACTION_NOT_FOUND.buildCode()).isEqualTo("DB-CORE-20031");
  }

  @Test
  public void buildMessage_ShouldBuildCorrectMessage() {
    // Arrange
    CoreError error = CoreError.OPERATION_CHECK_ERROR_INDEX_ONLY_SINGLE_COLUMN_INDEX_SUPPORTED;
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .build();

    // Act
    String message = error.buildMessage(put);

    // Assert
    Assertions.assertThat(message)
        .isEqualTo("DB-CORE-10000: Only a single-column index is supported. Operation: " + put);
  }

  @Test
  public void buildCode_ForCosmosConcatenatedPartitionKeyTooLong_ShouldBuildCorrectCode() {
    Assertions.assertThat(CoreError.COSMOS_CONCATENATED_PARTITION_KEY_TOO_LONG.buildCode())
        .isEqualTo("DB-CORE-10148");
  }

  @Test
  public void buildMessage_ForCosmosConcatenatedPartitionKeyTooLong_ShouldBuildCorrectMessage() {
    String message =
        CoreError.COSMOS_CONCATENATED_PARTITION_KEY_TOO_LONG.buildMessage(
            "ns", "tbl", "V1", 150, 101);

    Assertions.assertThat(message)
        .isEqualTo(
            "DB-CORE-10148: The concatenated partition key for table ns.tbl exceeds the maximum "
                + "length for this container's partition key version (V1). Length: 150 bytes, "
                + "maximum: 101 bytes. ScalarDB joins partition key columns with ':' and encodes "
                + "BLOB columns as Base64.");
  }

  @Test
  public void buildCode_ForCosmosDocumentIdTooLong_ShouldBuildCorrectCode() {
    Assertions.assertThat(CoreError.COSMOS_DOCUMENT_ID_TOO_LONG.buildCode())
        .isEqualTo("DB-CORE-10149");
  }

  @Test
  public void buildMessage_ForCosmosDocumentIdTooLong_ShouldBuildCorrectMessage() {
    String message = CoreError.COSMOS_DOCUMENT_ID_TOO_LONG.buildMessage("ns", "tbl", 255, 256);

    Assertions.assertThat(message)
        .isEqualTo(
            "DB-CORE-10149: The document id for table ns.tbl exceeds Cosmos DB's maximum of 255 "
                + "characters. ScalarDB builds the document id by joining partition key and "
                + "clustering key columns with ':'. Length: 256 characters.");
  }
}
