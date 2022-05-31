package com.scalar.db.storage.common.checker;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;

import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class DynamoOperationCheckerTest {
  private static final String PKEY1 = "p1";
  private static final String COL1 = "v1";
  private static final String COL2 = "v2";
  @Mock TableMetadataManager metadataManager;
  private OperationChecker operationChecker;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(PKEY1, DataType.INT)
            .addColumn(COL1, DataType.INT)
            .addColumn(COL2, DataType.DOUBLE)
            .addPartitionKey(PKEY1)
            .addSecondaryIndex(COL1)
            .build();
    Mockito.when(metadataManager.getTableMetadata(any())).thenReturn(tableMetadata);
    operationChecker = new DynamoOperationChecker(metadataManager);
  }

  @Test
  public void check_ForPutWithNullIndex_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    // Arrange
    Put put = new Put(Key.ofInt(PKEY1, 0)).withIntValue(COL1, null);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void check_ForPutWithNonNullIndex_ShouldDoNothing() {
    // Arrange
    Put put = new Put(Key.ofInt(PKEY1, 0)).withIntValue(COL1, 1);

    // Act Assert
    assertThatCode(() -> operationChecker.check(put)).doesNotThrowAnyException();
  }

  @Test
  public void check_ForPutWithoutSettingIndex_ShouldDoNothing() {
    // Arrange
    Put put = new Put(Key.ofInt(PKEY1, 0));

    // Act Assert
    assertThatCode(() -> operationChecker.check(put)).doesNotThrowAnyException();
  }
}
