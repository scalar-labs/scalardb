package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.Set;
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

  @Test
  public void isTransactionalMetaColumn_TableMetadataGiven_shouldReturnProperResult() {
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

    // Act Assert
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(ACCOUNT_ID, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(ACCOUNT_TYPE, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(BALANCE, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.ID, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.STATE, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.VERSION, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.PREPARED_AT, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.COMMITTED_AT, metadata))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isTransactionalMetaColumn(
                Attribute.BEFORE_PREFIX + BALANCE, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.BEFORE_ID, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.BEFORE_STATE, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.BEFORE_VERSION, metadata))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.BEFORE_PREPARED_AT, metadata))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.BEFORE_COMMITTED_AT, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn("aaa", metadata)).isFalse();
  }

  @Test
  public void isTransactionalMetaColumn_ResultGiven_shouldReturnProperResult() {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String BALANCE = "balance";

    Set<String> allColumnNames =
        ImmutableSet.<String>builder()
            .add(ACCOUNT_ID)
            .add(ACCOUNT_TYPE)
            .add(BALANCE)
            .add(Attribute.ID)
            .add(Attribute.STATE)
            .add(Attribute.VERSION)
            .add(Attribute.PREPARED_AT)
            .add(Attribute.COMMITTED_AT)
            .add(Attribute.BEFORE_PREFIX + BALANCE)
            .add(Attribute.BEFORE_ID)
            .add(Attribute.BEFORE_STATE)
            .add(Attribute.BEFORE_VERSION)
            .add(Attribute.BEFORE_PREPARED_AT)
            .add(Attribute.BEFORE_COMMITTED_AT)
            .build();

    // Act Assert
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(ACCOUNT_ID, allColumnNames))
        .isFalse();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(ACCOUNT_TYPE, allColumnNames))
        .isFalse();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(BALANCE, allColumnNames)).isFalse();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.ID, allColumnNames))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.STATE, allColumnNames))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.VERSION, allColumnNames))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.PREPARED_AT, allColumnNames))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.COMMITTED_AT, allColumnNames))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isTransactionalMetaColumn(
                Attribute.BEFORE_PREFIX + BALANCE, allColumnNames))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.BEFORE_ID, allColumnNames))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isTransactionalMetaColumn(Attribute.BEFORE_STATE, allColumnNames))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isTransactionalMetaColumn(
                Attribute.BEFORE_VERSION, allColumnNames))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isTransactionalMetaColumn(
                Attribute.BEFORE_PREPARED_AT, allColumnNames))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isTransactionalMetaColumn(
                Attribute.BEFORE_COMMITTED_AT, allColumnNames))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionalMetaColumn("aaa", allColumnNames)).isFalse();
  }

  @Test
  public void isBeforeImageColumn_ResultGiven_shouldReturnProperResult() {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String BALANCE = "balance";

    Set<String> allColumnNames =
        ImmutableSet.<String>builder()
            .add(ACCOUNT_ID)
            .add(ACCOUNT_TYPE)
            .add(BALANCE)
            .add(Attribute.ID)
            .add(Attribute.STATE)
            .add(Attribute.VERSION)
            .add(Attribute.PREPARED_AT)
            .add(Attribute.COMMITTED_AT)
            .add(Attribute.BEFORE_PREFIX + BALANCE)
            .add(Attribute.BEFORE_ID)
            .add(Attribute.BEFORE_STATE)
            .add(Attribute.BEFORE_VERSION)
            .add(Attribute.BEFORE_PREPARED_AT)
            .add(Attribute.BEFORE_COMMITTED_AT)
            .build();

    // Act Assert
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(ACCOUNT_ID, allColumnNames)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(ACCOUNT_TYPE, allColumnNames)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(BALANCE, allColumnNames)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.ID, allColumnNames)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.STATE, allColumnNames)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.VERSION, allColumnNames))
        .isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.PREPARED_AT, allColumnNames))
        .isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.COMMITTED_AT, allColumnNames))
        .isFalse();
    assertThat(
            ConsensusCommitUtils.isBeforeImageColumn(
                Attribute.BEFORE_PREFIX + BALANCE, allColumnNames))
        .isTrue();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_ID, allColumnNames))
        .isTrue();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_STATE, allColumnNames))
        .isTrue();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_VERSION, allColumnNames))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_PREPARED_AT, allColumnNames))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_COMMITTED_AT, allColumnNames))
        .isTrue();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn("aaa", allColumnNames)).isFalse();
  }

  @Test
  public void isAfterImageColumn_ResultGiven_shouldReturnProperResult() {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String BALANCE = "balance";

    Set<String> allColumnNames =
        ImmutableSet.<String>builder()
            .add(ACCOUNT_ID)
            .add(ACCOUNT_TYPE)
            .add(BALANCE)
            .add(Attribute.ID)
            .add(Attribute.STATE)
            .add(Attribute.VERSION)
            .add(Attribute.PREPARED_AT)
            .add(Attribute.COMMITTED_AT)
            .add(Attribute.BEFORE_PREFIX + BALANCE)
            .add(Attribute.BEFORE_ID)
            .add(Attribute.BEFORE_STATE)
            .add(Attribute.BEFORE_VERSION)
            .add(Attribute.BEFORE_PREPARED_AT)
            .add(Attribute.BEFORE_COMMITTED_AT)
            .build();

    // Act Assert
    assertThat(ConsensusCommitUtils.isAfterImageColumn(ACCOUNT_ID, allColumnNames)).isTrue();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(ACCOUNT_TYPE, allColumnNames)).isTrue();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(BALANCE, allColumnNames)).isTrue();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.ID, allColumnNames)).isTrue();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.STATE, allColumnNames)).isTrue();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.VERSION, allColumnNames)).isTrue();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.PREPARED_AT, allColumnNames))
        .isTrue();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.COMMITTED_AT, allColumnNames))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isAfterImageColumn(
                Attribute.BEFORE_PREFIX + BALANCE, allColumnNames))
        .isFalse();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.BEFORE_ID, allColumnNames))
        .isFalse();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.BEFORE_STATE, allColumnNames))
        .isFalse();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.BEFORE_VERSION, allColumnNames))
        .isFalse();
    assertThat(
            ConsensusCommitUtils.isAfterImageColumn(Attribute.BEFORE_PREPARED_AT, allColumnNames))
        .isFalse();
    assertThat(
            ConsensusCommitUtils.isAfterImageColumn(Attribute.BEFORE_COMMITTED_AT, allColumnNames))
        .isFalse();
    assertThat(ConsensusCommitUtils.isAfterImageColumn("aaa", allColumnNames)).isFalse();
  }
}
