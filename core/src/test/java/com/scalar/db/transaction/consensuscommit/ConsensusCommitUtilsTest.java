package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import org.junit.Test;

public class ConsensusCommitUtilsTest {

  @Test
  public void
      buildTransactionalTableMetadata_tableMetadataGiven_shouldCreateTransactionalTableProperly() {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String BALANCE = "balance";

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();

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
    TableMetadata actual = ConsensusCommitUtils.buildTransactionalTableMetadata(tableMetadata);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void
      buildTransactionalTableMetadata_tableMetadataThatHasTransactionMetaColumnGiven_shouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                ConsensusCommitUtils.buildTransactionalTableMetadata(
                    TableMetadata.newBuilder()
                        .addColumn("col1", DataType.INT)
                        .addColumn("col2", DataType.INT)
                        .addColumn(Attribute.ID, DataType.TEXT) // transaction meta column
                        .addPartitionKey("col1")
                        .build()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      buildTransactionalTableMetadata_tableMetadataThatHasNonPrimaryKeyColumnWithBeforePrefixGiven_shouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                ConsensusCommitUtils.buildTransactionalTableMetadata(
                    TableMetadata.newBuilder()
                        .addColumn("col1", DataType.INT)
                        .addColumn("col2", DataType.INT)
                        .addColumn(
                            Attribute.BEFORE_PREFIX + "col2",
                            DataType.INT) // non-primary key column with the "before_" prefix
                        .addPartitionKey("col1")
                        .build()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      isTransactionalTableMetadata_properTransactionalTableMetadataGiven_shouldReturnTrue() {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String BALANCE = "balance";

    TableMetadata metadata =
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
    boolean actual = ConsensusCommitUtils.isTransactionalTableMetadata(metadata);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void
      isTransactionalTableMetadata_transactionalTableMetadataWithoutMetaColumnGiven_shouldReturnFalse() {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String BALANCE = "balance";

    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
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
    boolean actual = ConsensusCommitUtils.isTransactionalTableMetadata(metadata);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void
      isTransactionalTableMetadata_transactionalTableMetadataWithoutProperBeforeColumnGiven_shouldReturnFalse() {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String BALANCE = "balance";

    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addColumn(Attribute.ID, DataType.TEXT)
            .addColumn(Attribute.STATE, DataType.INT)
            .addColumn(Attribute.VERSION, DataType.INT)
            .addColumn(Attribute.PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.COMMITTED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_ID, DataType.TEXT)
            .addColumn(Attribute.BEFORE_STATE, DataType.INT)
            .addColumn(Attribute.BEFORE_VERSION, DataType.INT)
            .addColumn(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();

    // Act
    boolean actual = ConsensusCommitUtils.isTransactionalTableMetadata(metadata);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void
      removeTransactionalMetaColumns_transactionalTableMetadataWithoutProperBeforeColumnGiven_shouldReturnFalse() {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String BALANCE = "balance";

    TableMetadata metadata =
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
    TableMetadata actual = ConsensusCommitUtils.removeTransactionalMetaColumns(metadata);

    // Assert
    assertThat(actual)
        .isEqualTo(
            TableMetadata.newBuilder()
                .addColumn(ACCOUNT_ID, DataType.INT)
                .addColumn(ACCOUNT_TYPE, DataType.INT)
                .addColumn(BALANCE, DataType.INT)
                .addPartitionKey(ACCOUNT_ID)
                .addClusteringKey(ACCOUNT_TYPE)
                .build());
  }
}
