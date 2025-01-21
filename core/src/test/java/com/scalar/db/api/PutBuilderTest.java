package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitOperationAttributes;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PutBuilderTest {
  private static final String NAMESPACE_1 = "namespace1";
  private static final String NAMESPACE_2 = "namespace2";

  private static final String TABLE_1 = "table1";
  private static final String TABLE_2 = "table2";

  private static final LocalDate ANY_DATE = DateColumn.MAX_VALUE;
  private static final LocalTime ANY_TIME = TimeColumn.MAX_VALUE;
  private static final LocalDateTime ANY_TIMESTAMP = TimestampColumn.MAX_VALUE;
  private static final Instant ANY_TIMESTAMPTZ = TimestampTZColumn.MAX_VALUE;

  @Mock private Key partitionKey1;
  @Mock private Key partitionKey2;
  @Mock private Key clusteringKey1;
  @Mock private Key clusteringKey2;
  @Mock private MutationCondition condition1;
  @Mock private MutationCondition condition2;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void build_WithMandatoryParameters_ShouldBuildPutWithMandatoryParameters() {
    // Arrange Act
    Put actual = Put.newBuilder().table(TABLE_1).partitionKey(partitionKey1).build();

    // Assert
    assertThat(actual).isEqualTo(new Put(partitionKey1).forTable(TABLE_1));
  }

  @Test
  public void build_WithClusteringKey_ShouldBuildPutWithClusteringKey() {
    // Arrange Act
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .build();

    // Assert
    assertThat(put)
        .isEqualTo(
            new Put(partitionKey1, clusteringKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void build_WithAllParameters_ShouldBuildPutCorrectly() {
    // Arrange Act
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .consistency(Consistency.EVENTUAL)
            .bigIntValue("bigint1", BigIntColumn.MAX_VALUE)
            .bigIntValue("bigint2", Long.valueOf(BigIntColumn.MAX_VALUE))
            .blobValue("blob1", "blob".getBytes(StandardCharsets.UTF_8))
            .blobValue("blob2", ByteBuffer.allocate(1))
            .booleanValue("bool1", true)
            .booleanValue("bool2", Boolean.TRUE)
            .doubleValue("double1", Double.MAX_VALUE)
            .doubleValue("double2", Double.valueOf(Double.MAX_VALUE))
            .floatValue("float1", Float.MAX_VALUE)
            .floatValue("float2", Float.valueOf(Float.MAX_VALUE))
            .intValue("int1", Integer.MAX_VALUE)
            .intValue("int2", Integer.valueOf(Integer.MAX_VALUE))
            .textValue("text1", "a_value")
            .dateValue("date", ANY_DATE)
            .timeValue("time", ANY_TIME)
            .timestampValue("timestamp", ANY_TIMESTAMP)
            .timestampTZValue("timestamptz", ANY_TIMESTAMPTZ)
            .value(TextColumn.of("text2", "another_value"))
            .condition(condition1)
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .enableImplicitPreRead()
            .enableInsertMode()
            .build();

    // Assert
    assertThat(put)
        .isEqualTo(
            new Put(
                NAMESPACE_1,
                TABLE_1,
                partitionKey1,
                clusteringKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(
                    "a1",
                    "v1",
                    "a2",
                    "v2",
                    "a3",
                    "v3",
                    ConsensusCommitOperationAttributes.IMPLICIT_PRE_READ_ENABLED,
                    "true",
                    ConsensusCommitOperationAttributes.INSERT_MODE_ENABLED,
                    "true"),
                condition1,
                ImmutableMap.<String, Column<?>>builder()
                    .put("bigint1", BigIntColumn.of("bigint1", BigIntColumn.MAX_VALUE))
                    .put("bigint2", BigIntColumn.of("bigint2", BigIntColumn.MAX_VALUE))
                    .put("blob1", BlobColumn.of("blob1", "blob".getBytes(StandardCharsets.UTF_8)))
                    .put("blob2", BlobColumn.of("blob2", ByteBuffer.allocate(1)))
                    .put("bool1", BooleanColumn.of("bool1", true))
                    .put("bool2", BooleanColumn.of("bool2", true))
                    .put("double1", DoubleColumn.of("double1", Double.MAX_VALUE))
                    .put("double2", DoubleColumn.of("double2", Double.MAX_VALUE))
                    .put("float1", FloatColumn.of("float1", Float.MAX_VALUE))
                    .put("float2", FloatColumn.of("float2", Float.MAX_VALUE))
                    .put("int1", IntColumn.of("int1", Integer.MAX_VALUE))
                    .put("int2", IntColumn.of("int2", Integer.MAX_VALUE))
                    .put("text1", TextColumn.of("text1", "a_value"))
                    .put("date", DateColumn.of("date", ANY_DATE))
                    .put("time", TimeColumn.of("time", ANY_TIME))
                    .put("timestamp", TimestampColumn.of("timestamp", ANY_TIMESTAMP))
                    .put("timestamptz", TimestampTZColumn.of("timestamptz", ANY_TIMESTAMPTZ))
                    .put("text2", TextColumn.of("text2", "another_value"))
                    .build()));
  }

  @Test
  public void build_WithAllValuesToNull_ShouldBuildPutCorrectly() {
    // Arrange Act
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .bigIntValue("bigint", null)
            .blobValue("blob1", (byte[]) null)
            .blobValue("blob2", (ByteBuffer) null)
            .booleanValue("bool", null)
            .doubleValue("double", null)
            .floatValue("float", null)
            .intValue("int", null)
            .textValue("text", null)
            .dateValue("date", null)
            .timeValue("time", null)
            .timestampValue("timestamp", null)
            .timestampTZValue("timestamptz", null)
            .build();

    // Assert
    assertThat(put)
        .isEqualTo(
            new Put(
                NAMESPACE_1,
                TABLE_1,
                partitionKey1,
                null,
                null,
                ImmutableMap.of(),
                null,
                ImmutableMap.<String, Column<?>>builder()
                    .put("bigint", BigIntColumn.ofNull("bigint"))
                    .put("blob1", BlobColumn.ofNull("blob1"))
                    .put("blob2", BlobColumn.ofNull("blob2"))
                    .put("bool", BooleanColumn.ofNull("bool"))
                    .put("double", DoubleColumn.ofNull("double"))
                    .put("float", FloatColumn.ofNull("float"))
                    .put("int", IntColumn.ofNull("int"))
                    .put("text", TextColumn.ofNull("text"))
                    .put("date", DateColumn.ofNull("date"))
                    .put("time", TimeColumn.ofNull("time"))
                    .put("timestamp", TimestampColumn.ofNull("timestamp"))
                    .put("timestamptz", TimestampTZColumn.ofNull("timestamptz"))
                    .build()));
  }

  @Test
  public void build_WithTwoValueUsingSameColumnName_ShouldBuildPutCorrectly() {
    // Arrange Act
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .bigIntValue("bigint", null)
            .textValue("bigint", "a_value")
            .build();

    // Assert
    assertThat(put)
        .isEqualTo(
            new Put(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withTextValue("bigint", "a_value"));
  }

  @Test
  public void build_FromExistingWithoutChange_ShouldCopy() {
    // Arrange
    Put existingPut =
        new Put(
            NAMESPACE_1,
            TABLE_1,
            partitionKey1,
            clusteringKey1,
            Consistency.EVENTUAL,
            ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"),
            condition1,
            ImmutableMap.<String, Column<?>>builder()
                .put("bigint1", BigIntColumn.of("bigint1", BigIntColumn.MAX_VALUE))
                .put("bigint2", BigIntColumn.of("bigint2", BigIntColumn.MAX_VALUE))
                .put("blob1", BlobColumn.of("blob1", "blob".getBytes(StandardCharsets.UTF_8)))
                .put("blob2", BlobColumn.of("blob2", ByteBuffer.allocate(1)))
                .put("bool1", BooleanColumn.of("bool1", true))
                .put("bool2", BooleanColumn.of("bool2", true))
                .put("double1", DoubleColumn.of("double1", Double.MAX_VALUE))
                .put("double2", DoubleColumn.of("double2", Double.MAX_VALUE))
                .put("float1", FloatColumn.of("float1", Float.MAX_VALUE))
                .put("float2", FloatColumn.of("float2", Float.MAX_VALUE))
                .put("int1", IntColumn.of("int1", Integer.MAX_VALUE))
                .put("int2", IntColumn.of("int2", Integer.MAX_VALUE))
                .put("text1", TextColumn.of("text1", "a_value"))
                .put("text2", TextColumn.of("text2", "another_value"))
                .put("date", DateColumn.of("date", ANY_DATE))
                .put("time", TimeColumn.of("time", ANY_TIME))
                .put("timestamp", TimestampColumn.of("timestamp", ANY_TIMESTAMP))
                .put("timestamptz", TimestampTZColumn.of("timestamptz", ANY_TIMESTAMPTZ))
                .build());

    // Act
    Put newPut = Put.newBuilder(existingPut).build();

    // Assert
    assertThat(newPut).isEqualTo(existingPut);
  }

  @Test
  public void build_FromExistingAndUpdateAllParameters_ShouldBuildPutWithUpdatedParameters() {
    // Arrange
    Put existingPut =
        new Put(
            NAMESPACE_1,
            TABLE_1,
            partitionKey1,
            clusteringKey1,
            Consistency.EVENTUAL,
            ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"),
            condition1,
            ImmutableMap.<String, Column<?>>builder()
                .put("bigint1", BigIntColumn.of("bigint1", BigIntColumn.MAX_VALUE))
                .put("bigint2", BigIntColumn.of("bigint2", BigIntColumn.MAX_VALUE))
                .put("blob1", BlobColumn.of("blob1", "blob".getBytes(StandardCharsets.UTF_8)))
                .put("blob2", BlobColumn.of("blob2", ByteBuffer.allocate(1)))
                .put("bool1", BooleanColumn.of("bool1", true))
                .put("bool2", BooleanColumn.of("bool2", true))
                .put("double1", DoubleColumn.of("double1", Double.MAX_VALUE))
                .put("double2", DoubleColumn.of("double2", Double.MAX_VALUE))
                .put("float1", FloatColumn.of("float1", Float.MAX_VALUE))
                .put("float2", FloatColumn.of("float2", Float.MAX_VALUE))
                .put("int1", IntColumn.of("int1", Integer.MAX_VALUE))
                .put("int2", IntColumn.of("int2", Integer.MAX_VALUE))
                .put("text1", TextColumn.of("text1", "a_value"))
                .put("text2", TextColumn.of("text2", "another_value"))
                .put("date", DateColumn.of("date", DateColumn.MAX_VALUE))
                .put("time", TimeColumn.of("time", TimeColumn.MAX_VALUE))
                .put("timestamp", TimestampColumn.of("timestamp", TimestampColumn.MAX_VALUE))
                .put(
                    "timestamptz", TimestampTZColumn.of("timestamptz", TimestampTZColumn.MAX_VALUE))
                .build());

    // Act
    Put newPut =
        Put.newBuilder(existingPut)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .partitionKey(partitionKey2)
            .clusteringKey(clusteringKey2)
            .consistency(Consistency.LINEARIZABLE)
            .clearValues()
            .bigIntValue("bigint1", BigIntColumn.MIN_VALUE)
            .bigIntValue("bigint2", Long.valueOf(BigIntColumn.MIN_VALUE))
            .blobValue("blob1", "foo".getBytes(StandardCharsets.UTF_8))
            .blobValue("blob2", ByteBuffer.allocate(2))
            .booleanValue("bool1", false)
            .booleanValue("bool2", Boolean.FALSE)
            .doubleValue("double1", Double.MIN_VALUE)
            .doubleValue("double2", Double.valueOf(Double.MIN_VALUE))
            .floatValue("float1", Float.MIN_VALUE)
            .floatValue("float2", Float.valueOf(Float.MIN_VALUE))
            .intValue("int1", Integer.MIN_VALUE)
            .intValue("int2", Integer.valueOf(Integer.MIN_VALUE))
            .textValue("text1", "another_value")
            .dateValue("date", LocalDate.ofEpochDay(0))
            .timeValue("time", LocalTime.NOON)
            .timestampValue("timestamp", LocalDateTime.of(LocalDate.ofEpochDay(0), LocalTime.NOON))
            .timestampTZValue("timestamptz", Instant.EPOCH)
            .value(TextColumn.of("text2", "foo"))
            .condition(condition2)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .enableImplicitPreRead()
            .enableInsertMode()
            .build();

    // Assert
    assertThat(newPut)
        .isEqualTo(
            new Put(
                NAMESPACE_2,
                TABLE_2,
                partitionKey2,
                clusteringKey2,
                Consistency.LINEARIZABLE,
                ImmutableMap.of(
                    "a4",
                    "v4",
                    "a5",
                    "v5",
                    "a6",
                    "v6",
                    ConsensusCommitOperationAttributes.IMPLICIT_PRE_READ_ENABLED,
                    "true",
                    ConsensusCommitOperationAttributes.INSERT_MODE_ENABLED,
                    "true"),
                condition2,
                ImmutableMap.<String, Column<?>>builder()
                    .put("bigint1", BigIntColumn.of("bigint1", BigIntColumn.MIN_VALUE))
                    .put("bigint2", BigIntColumn.of("bigint2", BigIntColumn.MIN_VALUE))
                    .put("blob1", BlobColumn.of("blob1", "foo".getBytes(StandardCharsets.UTF_8)))
                    .put("blob2", BlobColumn.of("blob2", ByteBuffer.allocate(2)))
                    .put("bool1", BooleanColumn.of("bool1", false))
                    .put("bool2", BooleanColumn.of("bool2", false))
                    .put("double1", DoubleColumn.of("double1", Double.MIN_VALUE))
                    .put("double2", DoubleColumn.of("double2", Double.MIN_VALUE))
                    .put("float1", FloatColumn.of("float1", Float.MIN_VALUE))
                    .put("float2", FloatColumn.of("float2", Float.MIN_VALUE))
                    .put("int1", IntColumn.of("int1", Integer.MIN_VALUE))
                    .put("int2", IntColumn.of("int2", Integer.MIN_VALUE))
                    .put("text1", TextColumn.of("text1", "another_value"))
                    .put("text2", TextColumn.of("text2", "foo"))
                    .put("date", DateColumn.of("date", LocalDate.ofEpochDay(0)))
                    .put("time", TimeColumn.of("time", LocalTime.NOON))
                    .put(
                        "timestamp",
                        TimestampColumn.of(
                            "timestamp", LocalDateTime.of(LocalDate.ofEpochDay(0), LocalTime.NOON)))
                    .put("timestamptz", TimestampTZColumn.of("timestamptz", Instant.EPOCH))
                    .build()));
  }

  @Test
  public void build_FromExistingAndClearValue_ShouldBuildPutWithoutClearedValues() {
    // Arrange
    Put existingPut =
        new Put(partitionKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withBooleanValue("bool", Boolean.TRUE)
            .withDoubleValue("double", Double.MIN_VALUE);

    // Act
    Put newPut =
        Put.newBuilder(existingPut).clearValue("double").clearValue("unknown_column").build();

    // Assert
    assertThat(newPut)
        .isEqualTo(
            new Put(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withBooleanValue("bool", Boolean.TRUE));
  }

  @Test
  public void build_FromExistingAndClearCondition_ShouldBuildPutWithoutCondition() {
    // Arrange
    Put existingPut =
        new Put(partitionKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withCondition(condition1);

    // Act
    Put newPut = Put.newBuilder(existingPut).clearCondition().build();

    // Assert
    assertThat(newPut)
        .isEqualTo(new Put(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void build_FromExistingAndClearClusteringKey_ShouldBuildPutWithoutClusteringKey() {
    // Arrange
    Put existingPut =
        new Put(partitionKey1, clusteringKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act
    Put newPut = Put.newBuilder(existingPut).clearClusteringKey().build();

    // Assert
    assertThat(newPut)
        .isEqualTo(new Put(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void build_FromExistingAndClearNamespace_ShouldBuildPutWithoutNamespace() {
    // Arrange
    Put existingPut =
        new Put(partitionKey1, clusteringKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act
    Put newPut = Put.newBuilder(existingPut).clearNamespace().build();

    // Assert
    assertThat(newPut).isEqualTo(new Put(partitionKey1, clusteringKey1).forTable(TABLE_1));
  }
}
