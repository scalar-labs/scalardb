package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TransactionTableMetadataManagerTest {

  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";
  private static final String BRANCH = "branch";

  @Mock private DistributedStorageAdmin admin;
  private TableMetadata tableMetadata;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addColumn(BRANCH, DataType.TEXT)
            .addColumn(Attribute.ID, DataType.TEXT)
            .addColumn(Attribute.STATE, DataType.INT)
            .addColumn(Attribute.VERSION, DataType.INT)
            .addColumn(Attribute.PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.COMMITTED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_PREFIX + BALANCE, DataType.INT)
            .addColumn(Attribute.BEFORE_PREFIX + BRANCH, DataType.TEXT)
            .addColumn(Attribute.BEFORE_ID, DataType.TEXT)
            .addColumn(Attribute.BEFORE_STATE, DataType.INT)
            .addColumn(Attribute.BEFORE_VERSION, DataType.INT)
            .addColumn(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
            .addColumn(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .addSecondaryIndex(BRANCH)
            .build();
    when(admin.getTableMetadata(anyString(), anyString())).thenReturn(tableMetadata);
  }

  @Test
  public void getTransactionTableMetadata_CalledOnce_ShouldCallDistributedStorageAdminOnce()
      throws ExecutionException {
    // Arrange
    TransactionTableMetadataManager tableMetadataManager =
        new TransactionTableMetadataManager(admin, -1);

    // Act
    TransactionTableMetadata actual =
        tableMetadataManager.getTransactionTableMetadata(
            new Get(new Key("c1", "aaa")).forNamespace("ns").forTable("tbl"));

    // Assert
    verify(admin).getTableMetadata(anyString(), anyString());
    assertTransactionMetadata(actual);
  }

  @Test
  public void getTransactionTableMetadata_CalledTwice_ShouldCallDistributedStorageAdminOnlyOnce()
      throws ExecutionException {
    // Arrange
    TransactionTableMetadataManager tableMetadataManager =
        new TransactionTableMetadataManager(admin, -1);

    Get get = new Get(new Key("c1", "aaa")).forNamespace("ns").forTable("tbl");

    // Act
    tableMetadataManager.getTransactionTableMetadata(get);
    TransactionTableMetadata actual = tableMetadataManager.getTransactionTableMetadata(get);

    // Assert
    verify(admin).getTableMetadata(anyString(), anyString());
    assertTransactionMetadata(actual);
  }

  @Test
  public void
      getTransactionTableMetadata_CalledAfterCacheExpiration_ShouldCallDistributedStorageAdminAgain()
          throws ExecutionException {
    // Arrange
    TransactionTableMetadataManager tableMetadataManager =
        new TransactionTableMetadataManager(admin, 1); // one second

    Get get = new Get(new Key("c1", "aaa")).forNamespace("ns").forTable("tbl");

    // Act
    tableMetadataManager.getTransactionTableMetadata(get);
    // Wait for cache to be expired
    Uninterruptibles.sleepUninterruptibly(1200, TimeUnit.MILLISECONDS);
    TransactionTableMetadata actual = tableMetadataManager.getTransactionTableMetadata(get);

    // Assert
    verify(admin, times(2)).getTableMetadata(anyString(), anyString());
    assertTransactionMetadata(actual);
  }

  private void assertTransactionMetadata(TransactionTableMetadata actual) {
    assertThat(actual.getTableMetadata()).isEqualTo(tableMetadata);
    assertThat(actual.getPartitionKeyNames())
        .isEqualTo(new LinkedHashSet<>(Collections.singletonList(ACCOUNT_ID)));
    assertThat(actual.getClusteringKeyNames())
        .isEqualTo(new LinkedHashSet<>(Collections.singletonList(ACCOUNT_TYPE)));
    assertThat(actual.getClusteringOrder(ACCOUNT_TYPE)).isEqualTo(Order.ASC);
    assertThat(actual.getClusteringOrders().get(ACCOUNT_TYPE)).isEqualTo(Order.ASC);
    assertThat(actual.getColumnNames())
        .isEqualTo(
            new LinkedHashSet<>(
                Arrays.asList(
                    ACCOUNT_ID,
                    ACCOUNT_TYPE,
                    BALANCE,
                    BRANCH,
                    Attribute.ID,
                    Attribute.STATE,
                    Attribute.VERSION,
                    Attribute.PREPARED_AT,
                    Attribute.COMMITTED_AT,
                    Attribute.BEFORE_PREFIX + BALANCE,
                    Attribute.BEFORE_PREFIX + BRANCH,
                    Attribute.BEFORE_ID,
                    Attribute.BEFORE_STATE,
                    Attribute.BEFORE_VERSION,
                    Attribute.BEFORE_PREPARED_AT,
                    Attribute.BEFORE_COMMITTED_AT)));
    assertThat(actual.getColumnDataType(ACCOUNT_ID)).isEqualTo(DataType.INT);
    assertThat(actual.getColumnDataType(ACCOUNT_TYPE)).isEqualTo(DataType.INT);
    assertThat(actual.getColumnDataType(BALANCE)).isEqualTo(DataType.INT);
    assertThat(actual.getColumnDataType(BRANCH)).isEqualTo(DataType.TEXT);
    assertThat(actual.getColumnDataType(Attribute.ID)).isEqualTo(DataType.TEXT);
    assertThat(actual.getColumnDataType(Attribute.STATE)).isEqualTo(DataType.INT);
    assertThat(actual.getColumnDataType(Attribute.VERSION)).isEqualTo(DataType.INT);
    assertThat(actual.getColumnDataType(Attribute.PREPARED_AT)).isEqualTo(DataType.BIGINT);
    assertThat(actual.getColumnDataType(Attribute.COMMITTED_AT)).isEqualTo(DataType.BIGINT);
    assertThat(actual.getColumnDataType(Attribute.BEFORE_PREFIX + BALANCE)).isEqualTo(DataType.INT);
    assertThat(actual.getColumnDataType(Attribute.BEFORE_PREFIX + BRANCH)).isEqualTo(DataType.TEXT);
    assertThat(actual.getColumnDataType(Attribute.BEFORE_ID)).isEqualTo(DataType.TEXT);
    assertThat(actual.getColumnDataType(Attribute.BEFORE_STATE)).isEqualTo(DataType.INT);
    assertThat(actual.getColumnDataType(Attribute.BEFORE_VERSION)).isEqualTo(DataType.INT);
    assertThat(actual.getColumnDataType(Attribute.BEFORE_PREPARED_AT)).isEqualTo(DataType.BIGINT);
    assertThat(actual.getColumnDataType(Attribute.BEFORE_COMMITTED_AT)).isEqualTo(DataType.BIGINT);
    assertThat(actual.getSecondaryIndexNames())
        .isEqualTo(new HashSet<>(Collections.singletonList(BRANCH)));
    assertThat(actual.getPrimaryKeyColumnNames()).containsExactly(ACCOUNT_ID, ACCOUNT_TYPE);
    assertThat(actual.getTransactionMetaColumnNames())
        .isEqualTo(
            new LinkedHashSet<>(
                Arrays.asList(
                    Attribute.ID,
                    Attribute.STATE,
                    Attribute.VERSION,
                    Attribute.PREPARED_AT,
                    Attribute.COMMITTED_AT,
                    Attribute.BEFORE_PREFIX + BALANCE,
                    Attribute.BEFORE_PREFIX + BRANCH,
                    Attribute.BEFORE_ID,
                    Attribute.BEFORE_STATE,
                    Attribute.BEFORE_VERSION,
                    Attribute.BEFORE_PREPARED_AT,
                    Attribute.BEFORE_COMMITTED_AT)));
    assertThat(actual.getBeforeImageColumnNames())
        .isEqualTo(
            new LinkedHashSet<>(
                Arrays.asList(
                    Attribute.BEFORE_PREFIX + BALANCE,
                    Attribute.BEFORE_PREFIX + BRANCH,
                    Attribute.BEFORE_ID,
                    Attribute.BEFORE_STATE,
                    Attribute.BEFORE_VERSION,
                    Attribute.BEFORE_PREPARED_AT,
                    Attribute.BEFORE_COMMITTED_AT)));
    assertThat(actual.getAfterImageColumnNames())
        .isEqualTo(
            new LinkedHashSet<>(
                Arrays.asList(
                    ACCOUNT_ID,
                    ACCOUNT_TYPE,
                    BALANCE,
                    BRANCH,
                    Attribute.ID,
                    Attribute.STATE,
                    Attribute.VERSION,
                    Attribute.PREPARED_AT,
                    Attribute.COMMITTED_AT)));
  }
}
