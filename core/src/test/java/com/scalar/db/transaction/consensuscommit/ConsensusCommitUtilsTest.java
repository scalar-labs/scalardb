package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class ConsensusCommitUtilsTest {

  @Test
  public void
      buildTransactionTableMetadata_tableMetadataGiven_shouldCreateTransactionTableProperly() {
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
    TableMetadata actual = ConsensusCommitUtils.buildTransactionTableMetadata(tableMetadata);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void
      buildTransactionTableMetadata_tableMetadataThatHasTransactionMetaColumnGiven_shouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                ConsensusCommitUtils.buildTransactionTableMetadata(
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
      buildTransactionTableMetadata_tableMetadataThatHasNonPrimaryKeyColumnWithBeforePrefixGiven_shouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                ConsensusCommitUtils.buildTransactionTableMetadata(
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
  public void getBeforeImageColumnName_tableMetadataGiven_shouldCreateTransactionalTableProperly() {
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

    String expectedBeforeColumnName = Attribute.BEFORE_PREFIX + BALANCE;

    // Act
    String actualBeforeColumnName =
        ConsensusCommitUtils.getBeforeImageColumnName(BALANCE, tableMetadata);

    // Assert
    assertThat(actualBeforeColumnName).isEqualTo(expectedBeforeColumnName);
  }

  @Test
  public void
      getBeforeImageColumnName_tableMetadataThatHasTransactionMetaColumnGiven_shouldThrowIllegalArgumentException() {
    // Arrange
    TableMetadata tableMetadata =
        TableMetadata.newBuilder().addPartitionKey("c1").addColumn("c1", DataType.TEXT).build();

    // Act Assert
    assertThatThrownBy(
            () -> ConsensusCommitUtils.getBeforeImageColumnName(Attribute.ID, tableMetadata))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      getBeforeImageColumnName_tableMetadataThatHasNonPrimaryKeyColumnWithBeforePrefixGiven_shouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                ConsensusCommitUtils.getBeforeImageColumnName(
                    "col2",
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
  public void isTransactionTableMetadata_properTransactionTableMetadataGiven_shouldReturnTrue() {
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
    boolean actual = ConsensusCommitUtils.isTransactionTableMetadata(metadata);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void
      isTransactionTableMetadata_TransactionTableMetadataWithoutMetaColumnGiven_shouldReturnFalse() {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String BALANCE = "balance";
    // the 'Attribute.ID' column is missing
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
    boolean actual = ConsensusCommitUtils.isTransactionTableMetadata(metadata);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void
      isTransactionTableMetadata_TransactionTableMetadataWithoutProperAfterColumnGiven_shouldReturnFalse() {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String FOO = "foo";

    // the 'before_foo' column exists but the 'foo' column is missing
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(Attribute.BEFORE_PREFIX + FOO, DataType.TEXT)
            .addColumn(Attribute.BEFORE_ID, DataType.TEXT)
            .addColumn(Attribute.BEFORE_STATE, DataType.INT)
            .addColumn(Attribute.BEFORE_VERSION, DataType.INT)
            .addColumn(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
            .addColumn(Attribute.ID, DataType.TEXT)
            .addColumn(Attribute.STATE, DataType.INT)
            .addColumn(Attribute.VERSION, DataType.INT)
            .addColumn(Attribute.PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.COMMITTED_AT, DataType.BIGINT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();

    // Act
    boolean actual = ConsensusCommitUtils.isTransactionTableMetadata(metadata);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void
      isTransactionTableMetadata_TransactionTableMetadataWithoutProperBeforeColumnGiven_shouldReturnFalse() {
    // Arrange
    final String ACCOUNT_ID = "account_id";
    final String ACCOUNT_TYPE = "account_type";
    final String BALANCE = "balance";

    // the 'balance' column exists but the 'before_balance' column is missing
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
    boolean actual = ConsensusCommitUtils.isTransactionTableMetadata(metadata);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void
      removeTransactionMetaColumns_TransactionTableMetadataWithoutProperBeforeColumnGiven_shouldReturnFalse() {
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
    TableMetadata actual = ConsensusCommitUtils.removeTransactionMetaColumns(metadata);

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
  public void isTransactionMetaColumn_TableMetadataGiven_shouldReturnProperResult() {
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
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(ACCOUNT_ID, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(ACCOUNT_TYPE, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(BALANCE, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.ID, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.STATE, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.VERSION, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.PREPARED_AT, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.COMMITTED_AT, metadata))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isTransactionMetaColumn(
                Attribute.BEFORE_PREFIX + BALANCE, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.BEFORE_ID, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.BEFORE_STATE, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.BEFORE_VERSION, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.BEFORE_PREPARED_AT, metadata))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isTransactionMetaColumn(Attribute.BEFORE_COMMITTED_AT, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn("aaa", metadata)).isFalse();
  }

  @Test
  public void isTransactionMetaColumn_ResultGiven_shouldReturnProperResult() {
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
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(ACCOUNT_ID, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(ACCOUNT_TYPE, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(BALANCE, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.ID, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.STATE, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.VERSION, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.PREPARED_AT, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.COMMITTED_AT, metadata))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isTransactionMetaColumn(
                Attribute.BEFORE_PREFIX + BALANCE, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.BEFORE_ID, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.BEFORE_STATE, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.BEFORE_VERSION, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn(Attribute.BEFORE_PREPARED_AT, metadata))
        .isTrue();
    assertThat(
            ConsensusCommitUtils.isTransactionMetaColumn(Attribute.BEFORE_COMMITTED_AT, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isTransactionMetaColumn("aaa", metadata)).isFalse();
  }

  @Test
  public void isBeforeImageColumn_ResultGiven_shouldReturnProperResult() {
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
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(ACCOUNT_ID, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(ACCOUNT_TYPE, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(BALANCE, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.ID, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.STATE, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.VERSION, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.PREPARED_AT, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.COMMITTED_AT, metadata))
        .isFalse();
    assertThat(
            ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_PREFIX + BALANCE, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_ID, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_STATE, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_VERSION, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_PREPARED_AT, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_COMMITTED_AT, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn("aaa", metadata)).isFalse();
  }

  @Test
  public void
      isBeforeImageColumn_PartitionKeyAndClusteringKeyWithBeforePrefixGiven_shouldReturnProperResult() {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn("partitionKey", DataType.INT)
            .addColumn("before_partitionKey", DataType.INT)
            .addColumn("clusteringKey", DataType.INT)
            .addColumn("before_clusteringKey", DataType.INT)
            .addColumn("col", DataType.INT)
            .addColumn(Attribute.ID, DataType.TEXT)
            .addColumn(Attribute.STATE, DataType.INT)
            .addColumn(Attribute.VERSION, DataType.INT)
            .addColumn(Attribute.PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.COMMITTED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_PREFIX + "col", DataType.INT)
            .addColumn(Attribute.BEFORE_ID, DataType.TEXT)
            .addColumn(Attribute.BEFORE_STATE, DataType.INT)
            .addColumn(Attribute.BEFORE_VERSION, DataType.INT)
            .addColumn(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
            .addPartitionKey("partitionKey")
            .addPartitionKey("before_partitionKey")
            .addClusteringKey("clusteringKey")
            .addClusteringKey("before_clusteringKey")
            .build();

    // Act Assert
    assertThat(ConsensusCommitUtils.isBeforeImageColumn("partitionKey", metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn("before_partitionKey", metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn("clusteringKey", metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn("before_clusteringKey", metadata))
        .isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn("col", metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.ID, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.STATE, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.VERSION, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.PREPARED_AT, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.COMMITTED_AT, metadata))
        .isFalse();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_PREFIX + "col", metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_ID, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_STATE, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_VERSION, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_PREPARED_AT, metadata))
        .isTrue();
    assertThat(ConsensusCommitUtils.isBeforeImageColumn(Attribute.BEFORE_COMMITTED_AT, metadata))
        .isTrue();
  }

  @Test
  public void isAfterImageColumn_ResultGiven_shouldReturnProperResult() {
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
    assertThat(ConsensusCommitUtils.isAfterImageColumn(ACCOUNT_ID, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(ACCOUNT_TYPE, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(BALANCE, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.ID, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.STATE, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.VERSION, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.PREPARED_AT, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.COMMITTED_AT, metadata)).isTrue();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.BEFORE_PREFIX + BALANCE, metadata))
        .isFalse();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.BEFORE_ID, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.BEFORE_STATE, metadata)).isFalse();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.BEFORE_VERSION, metadata))
        .isFalse();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.BEFORE_PREPARED_AT, metadata))
        .isFalse();
    assertThat(ConsensusCommitUtils.isAfterImageColumn(Attribute.BEFORE_COMMITTED_AT, metadata))
        .isFalse();
    assertThat(ConsensusCommitUtils.isAfterImageColumn("aaa", metadata)).isFalse();
  }

  @Test
  void getNextTxVersion_NullGiven_shouldReturnInitialVersion() {
    // Act Assert
    assertThat(ConsensusCommitUtils.getNextTxVersion(null)).isEqualTo(1);
  }

  @Test
  void getNextTxVersion_OneGiven_shouldReturnNextVersion() {
    // Act Assert
    assertThat(ConsensusCommitUtils.getNextTxVersion(1)).isEqualTo(2);
  }

  @Test
  void getNextTxVersion_LargeValueGiven_shouldReturnNextVersion() {
    // Act Assert
    assertThat(ConsensusCommitUtils.getNextTxVersion(100000)).isEqualTo(100001);
  }

  @Test
  public void createAfterImageColumnsFromBeforeImage_shouldExtractCorrectly() {
    // Arrange
    Map<String, Column<?>> columns = new HashMap<>();
    Set<String> beforeImageColumnNames = new HashSet<>();
    beforeImageColumnNames.add("before_balance");
    beforeImageColumnNames.add("before_name");

    Map<String, Column<?>> resultColumns = new HashMap<>();
    resultColumns.put("before_balance", IntColumn.of("before_balance", 1000));
    resultColumns.put("before_name", TextColumn.of("before_name", "Alice"));
    resultColumns.put("account_id", IntColumn.of("account_id", 123)); // Not a before column

    TransactionResult result = mock(TransactionResult.class);
    when(result.getColumns()).thenReturn(resultColumns);

    // Act
    ConsensusCommitUtils.createAfterImageColumnsFromBeforeImage(
        columns, result, beforeImageColumnNames);

    // Assert
    assertThat(columns).hasSize(2);
    assertThat(columns).containsKey("balance");
    assertThat(columns).containsKey("name");
    assertThat(columns.get("balance").getIntValue()).isEqualTo(1000);
    assertThat(columns.get("name").getTextValue()).isEqualTo("Alice");
    assertThat(columns).doesNotContainKey("account_id");
  }

  @Test
  public void
      createAfterImageColumnsFromBeforeImage_versionColumnWithZero_shouldCreateNullVersion() {
    // Arrange
    Map<String, Column<?>> columns = new HashMap<>();
    Set<String> beforeImageColumnNames = new HashSet<>();
    beforeImageColumnNames.add("before_tx_version");

    Map<String, Column<?>> resultColumns = new HashMap<>();
    resultColumns.put("before_tx_version", IntColumn.of("before_tx_version", 0));

    TransactionResult result = mock(TransactionResult.class);
    when(result.getColumns()).thenReturn(resultColumns);

    // Act
    ConsensusCommitUtils.createAfterImageColumnsFromBeforeImage(
        columns, result, beforeImageColumnNames);

    // Assert
    assertThat(columns).hasSize(1);
    assertThat(columns).containsKey("tx_version");
    assertThat(columns.get("tx_version").hasNullValue()).isTrue();
  }

  @Test
  public void createAfterImageColumnsFromBeforeImage_versionColumnWithNonZero_shouldCopyValue() {
    // Arrange
    Map<String, Column<?>> columns = new HashMap<>();
    Set<String> beforeImageColumnNames = new HashSet<>();
    beforeImageColumnNames.add("before_tx_version");

    Map<String, Column<?>> resultColumns = new HashMap<>();
    resultColumns.put("before_tx_version", IntColumn.of("before_tx_version", 5));

    TransactionResult result = mock(TransactionResult.class);
    when(result.getColumns()).thenReturn(resultColumns);

    // Act
    ConsensusCommitUtils.createAfterImageColumnsFromBeforeImage(
        columns, result, beforeImageColumnNames);

    // Assert
    assertThat(columns).hasSize(1);
    assertThat(columns).containsKey("tx_version");
    assertThat(columns.get("tx_version").hasNullValue()).isFalse();
    assertThat(columns.get("tx_version").getIntValue()).isEqualTo(5);
  }
}
