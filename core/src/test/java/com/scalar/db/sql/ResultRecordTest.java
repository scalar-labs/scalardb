package com.scalar.db.sql;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.Before;
import org.junit.Test;

public class ResultRecordTest {

  private static final String COLUMN_NAME_1 = "col1";
  private static final String COLUMN_NAME_2 = "col2";
  private static final String COLUMN_NAME_3 = "col3";
  private static final String COLUMN_NAME_4 = "col4";
  private static final String COLUMN_NAME_5 = "col5";
  private static final String COLUMN_NAME_6 = "col6";
  private static final String COLUMN_NAME_7 = "col7";

  private static final com.scalar.db.api.TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionalTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(COLUMN_NAME_1, com.scalar.db.io.DataType.INT)
              .addColumn(COLUMN_NAME_2, com.scalar.db.io.DataType.BOOLEAN)
              .addColumn(COLUMN_NAME_3, com.scalar.db.io.DataType.BIGINT)
              .addColumn(COLUMN_NAME_4, com.scalar.db.io.DataType.FLOAT)
              .addColumn(COLUMN_NAME_5, com.scalar.db.io.DataType.DOUBLE)
              .addColumn(COLUMN_NAME_6, com.scalar.db.io.DataType.TEXT)
              .addColumn(COLUMN_NAME_7, com.scalar.db.io.DataType.BLOB)
              .addPartitionKey(COLUMN_NAME_1)
              .build());

  private ResultRecord resultRecord;

  @Before
  public void setUp() {
    // Arrange
    resultRecord =
        new ResultRecord(
            new ResultImpl(
                ImmutableMap.<String, Column<?>>builder()
                    .put(COLUMN_NAME_1, IntColumn.of(COLUMN_NAME_1, 10))
                    .put(COLUMN_NAME_2, BooleanColumn.of(COLUMN_NAME_2, true))
                    .put(COLUMN_NAME_3, BigIntColumn.of(COLUMN_NAME_3, 100L))
                    .put(COLUMN_NAME_4, FloatColumn.of(COLUMN_NAME_4, 1.23F))
                    .put(COLUMN_NAME_5, DoubleColumn.of(COLUMN_NAME_5, 4.56))
                    .put(COLUMN_NAME_6, TextColumn.of(COLUMN_NAME_6, "text"))
                    .put(
                        COLUMN_NAME_7,
                        BlobColumn.of(COLUMN_NAME_7, "blob".getBytes(StandardCharsets.UTF_8)))
                    .build(),
                TABLE_METADATA),
            ImmutableList.of(
                COLUMN_NAME_4,
                COLUMN_NAME_7,
                COLUMN_NAME_6,
                COLUMN_NAME_2,
                COLUMN_NAME_3,
                COLUMN_NAME_1,
                COLUMN_NAME_5));
  }

  @Test
  public void getXXX_NameGiven_ShouldReturnCorrectResult() {
    // Arrange

    // Act Assert
    assertThat(resultRecord.getContainedColumnNames())
        .isEqualTo(
            ImmutableSet.of(
                COLUMN_NAME_1,
                COLUMN_NAME_2,
                COLUMN_NAME_3,
                COLUMN_NAME_4,
                COLUMN_NAME_5,
                COLUMN_NAME_6,
                COLUMN_NAME_7));
    assertThat(resultRecord.getInt(COLUMN_NAME_1)).isEqualTo(10);
    assertThat(resultRecord.getBoolean(COLUMN_NAME_2)).isTrue();
    assertThat(resultRecord.getBigInt(COLUMN_NAME_3)).isEqualTo(100L);
    assertThat(resultRecord.getFloat(COLUMN_NAME_4)).isEqualTo(1.23F);
    assertThat(resultRecord.getDouble(COLUMN_NAME_5)).isEqualTo(4.56);
    assertThat(resultRecord.getText(COLUMN_NAME_6)).isEqualTo("text");
    assertThat(resultRecord.getBlob(COLUMN_NAME_7))
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void getXXX_IndexGiven_ShouldReturnCorrectResult() {
    // Arrange

    // Act Assert
    assertThat(resultRecord.size()).isEqualTo(7);
    assertThat(resultRecord.getFloat(0)).isEqualTo(1.23F);
    assertThat(resultRecord.getBlob(1))
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThat(resultRecord.getText(2)).isEqualTo("text");
    assertThat(resultRecord.getBoolean(3)).isTrue();
    assertThat(resultRecord.getBigInt(4)).isEqualTo(100L);
    assertThat(resultRecord.getInt(5)).isEqualTo(10);
    assertThat(resultRecord.getDouble(6)).isEqualTo(4.56);
  }

  @Test
  public void isNull_NameGiven_ShouldReturnCorrectResult() {
    // Arrange
    resultRecord =
        new ResultRecord(
            new ResultImpl(
                ImmutableMap.<String, Column<?>>builder()
                    .put(COLUMN_NAME_1, IntColumn.ofNull(COLUMN_NAME_1))
                    .put(COLUMN_NAME_2, BooleanColumn.ofNull(COLUMN_NAME_2))
                    .put(COLUMN_NAME_3, BigIntColumn.ofNull(COLUMN_NAME_3))
                    .put(COLUMN_NAME_4, FloatColumn.ofNull(COLUMN_NAME_4))
                    .put(COLUMN_NAME_5, DoubleColumn.ofNull(COLUMN_NAME_5))
                    .put(COLUMN_NAME_6, TextColumn.ofNull(COLUMN_NAME_6))
                    .put(COLUMN_NAME_7, BlobColumn.ofNull(COLUMN_NAME_7))
                    .build(),
                TABLE_METADATA),
            ImmutableList.of(
                COLUMN_NAME_4,
                COLUMN_NAME_7,
                COLUMN_NAME_6,
                COLUMN_NAME_2,
                COLUMN_NAME_3,
                COLUMN_NAME_1,
                COLUMN_NAME_5));

    // Act Assert
    assertThat(resultRecord.getContainedColumnNames())
        .isEqualTo(
            ImmutableSet.of(
                COLUMN_NAME_1,
                COLUMN_NAME_2,
                COLUMN_NAME_3,
                COLUMN_NAME_4,
                COLUMN_NAME_5,
                COLUMN_NAME_6,
                COLUMN_NAME_7));
    assertThat(resultRecord.isNull(COLUMN_NAME_1)).isTrue();
    assertThat(resultRecord.isNull(COLUMN_NAME_2)).isTrue();
    assertThat(resultRecord.isNull(COLUMN_NAME_3)).isTrue();
    assertThat(resultRecord.isNull(COLUMN_NAME_4)).isTrue();
    assertThat(resultRecord.isNull(COLUMN_NAME_5)).isTrue();
    assertThat(resultRecord.isNull(COLUMN_NAME_6)).isTrue();
    assertThat(resultRecord.isNull(COLUMN_NAME_7)).isTrue();
  }

  @Test
  public void isNull_IndexGiven_ShouldReturnCorrectResult() {
    // Arrange
    resultRecord =
        new ResultRecord(
            new ResultImpl(
                ImmutableMap.<String, Column<?>>builder()
                    .put(COLUMN_NAME_1, IntColumn.ofNull(COLUMN_NAME_1))
                    .put(COLUMN_NAME_2, BooleanColumn.ofNull(COLUMN_NAME_2))
                    .put(COLUMN_NAME_3, BigIntColumn.ofNull(COLUMN_NAME_3))
                    .put(COLUMN_NAME_4, FloatColumn.ofNull(COLUMN_NAME_4))
                    .put(COLUMN_NAME_5, DoubleColumn.ofNull(COLUMN_NAME_5))
                    .put(COLUMN_NAME_6, TextColumn.ofNull(COLUMN_NAME_6))
                    .put(COLUMN_NAME_7, BlobColumn.ofNull(COLUMN_NAME_7))
                    .build(),
                TABLE_METADATA),
            ImmutableList.of(
                COLUMN_NAME_4,
                COLUMN_NAME_7,
                COLUMN_NAME_6,
                COLUMN_NAME_2,
                COLUMN_NAME_3,
                COLUMN_NAME_1,
                COLUMN_NAME_5));

    // Act Assert
    assertThat(resultRecord.size()).isEqualTo(7);
    assertThat(resultRecord.isNull(0)).isTrue();
    assertThat(resultRecord.isNull(1)).isTrue();
    assertThat(resultRecord.isNull(2)).isTrue();
    assertThat(resultRecord.isNull(3)).isTrue();
    assertThat(resultRecord.isNull(4)).isTrue();
    assertThat(resultRecord.isNull(5)).isTrue();
    assertThat(resultRecord.isNull(6)).isTrue();
  }
}
