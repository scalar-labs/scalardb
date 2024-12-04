package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class UpsertBuilderTest {
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

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void build_WithMandatoryParameters_ShouldBuildUpsertWithMandatoryParameters() {
    // Arrange Act
    Upsert actual = Upsert.newBuilder().table(TABLE_1).partitionKey(partitionKey1).build();

    // Assert
    assertThat(actual.forNamespace()).isEmpty();
    assertThat(actual.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(actual.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(actual.getClusteringKey()).isEmpty();
    assertThat(actual.getColumns()).isEmpty();
  }

  @Test
  public void build_WithClusteringKey_ShouldBuildUpsertWithClusteringKey() {
    // Arrange Act
    Upsert actual =
        Upsert.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .build();

    // Assert
    assertThat(actual.forNamespace()).hasValue(NAMESPACE_1);
    assertThat(actual.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(actual.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(actual.getClusteringKey()).hasValue(clusteringKey1);
    assertThat(actual.getColumns()).isEmpty();
  }

  @Test
  public void build_WithAllParameters_ShouldBuildUpsertCorrectly() {
    // Arrange Act
    Upsert actual =
        Upsert.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
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
            .textValue("text", "a_value")
            .dateValue("date", ANY_DATE)
            .timeValue("time", ANY_TIME)
            .timestampValue("timestamp", ANY_TIMESTAMP)
            .timestampTZValue("timestamptz", ANY_TIMESTAMPTZ)
            .value(TextColumn.of("text2", "another_value"))
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .build();

    // Assert
    assertThat(actual.forNamespace()).hasValue(NAMESPACE_1);
    assertThat(actual.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(actual.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(actual.getClusteringKey()).hasValue(clusteringKey1);
    assertThat(actual.getColumns().size()).isEqualTo(18);
    assertThat(actual.getColumns().get("bigint1").getBigIntValue())
        .isEqualTo(BigIntColumn.MAX_VALUE);
    assertThat(actual.getColumns().get("bigint2").getBigIntValue())
        .isEqualTo(Long.valueOf(BigIntColumn.MAX_VALUE));
    assertThat(actual.getColumns().get("blob1").getBlobValueAsBytes())
        .isEqualTo("blob".getBytes(StandardCharsets.UTF_8));
    assertThat(actual.getColumns().get("blob2").getBlobValueAsByteBuffer())
        .isEqualTo(ByteBuffer.allocate(1));
    assertThat(actual.getColumns().get("bool1").getBooleanValue()).isTrue();
    assertThat(actual.getColumns().get("bool2").getBooleanValue()).isTrue();
    assertThat(actual.getColumns().get("double1").getDoubleValue()).isEqualTo(Double.MAX_VALUE);
    assertThat(actual.getColumns().get("double2").getDoubleValue())
        .isEqualTo(Double.valueOf(Double.MAX_VALUE));
    assertThat(actual.getColumns().get("float1").getFloatValue()).isEqualTo(Float.MAX_VALUE);
    assertThat(actual.getColumns().get("float2").getFloatValue())
        .isEqualTo(Float.valueOf(Float.MAX_VALUE));
    assertThat(actual.getColumns().get("int1").getIntValue()).isEqualTo(Integer.MAX_VALUE);
    assertThat(actual.getColumns().get("int2").getIntValue())
        .isEqualTo(Integer.valueOf(Integer.MAX_VALUE));
    assertThat(actual.getColumns().get("text").getTextValue()).isEqualTo("a_value");
    assertThat(actual.getColumns().get("date").getDateValue()).isEqualTo(ANY_DATE);
    assertThat(actual.getColumns().get("time").getTimeValue()).isEqualTo(ANY_TIME);
    assertThat(actual.getColumns().get("timestamp").getTimestampValue()).isEqualTo(ANY_TIMESTAMP);
    assertThat(actual.getColumns().get("timestamptz").getTimestampTZValue())
        .isEqualTo(ANY_TIMESTAMPTZ);
    assertThat(actual.getColumns().get("text2").getTextValue()).isEqualTo("another_value");
    assertThat(actual.getAttributes())
        .isEqualTo(ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"));
  }

  @Test
  public void build_WithAllValuesToNull_ShouldBuildUpsertCorrectly() {
    // Arrange Act
    Upsert actual =
        Upsert.newBuilder()
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
    assertThat(actual.forNamespace()).hasValue(NAMESPACE_1);
    assertThat(actual.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(actual.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(actual.getClusteringKey()).isEmpty();
    assertThat(actual.getColumns().size()).isEqualTo(12);
    assertThat(actual.getColumns().get("bigint").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("blob1").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("blob2").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("bool").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("double").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("float").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("int").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("text").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("date").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("time").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("timestamp").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("timestamptz").hasNullValue()).isTrue();
  }

  @Test
  public void build_WithTwoValueUsingSameColumnName_ShouldBuildUpsertCorrectly() {
    // Arrange Act
    Upsert actual =
        Upsert.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .bigIntValue("bigint", null)
            .textValue("bigint", "a_value")
            .build();

    // Assert
    assertThat(actual.forNamespace()).hasValue(NAMESPACE_1);
    assertThat(actual.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(actual.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(actual.getClusteringKey()).isEmpty();
    assertThat(actual.getColumns().size()).isEqualTo(1);
    assertThat(actual.getColumns().get("bigint").getTextValue()).isEqualTo("a_value");
  }

  @Test
  public void build_FromExistingWithoutChange_ShouldCopy() {
    // Arrange
    Upsert existingUpsert =
        Upsert.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
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
            .textValue("text", "a_value")
            .dateValue("date", ANY_DATE)
            .timeValue("time", ANY_TIME)
            .timestampValue("timestamp", ANY_TIMESTAMP)
            .timestampTZValue("timestamptz", ANY_TIMESTAMPTZ)
            .value(TextColumn.of("text2", "another_value"))
            .build();

    // Act
    Upsert newUpsert = Upsert.newBuilder(existingUpsert).build();

    // Assert
    assertThat(newUpsert).isEqualTo(existingUpsert);
  }

  @Test
  public void build_FromExistingAndUpdateAllParameters_ShouldBuildUpsertWithUpdatedParameters() {
    // Arrange
    Upsert existingUpsert =
        Upsert.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
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
            .textValue("text", "a_value")
            .dateValue("date", DateColumn.MAX_VALUE)
            .timeValue("time", TimeColumn.MAX_VALUE)
            .timestampValue("timestamp", TimestampColumn.MAX_VALUE)
            .timestampTZValue("timestamptz", TimestampTZColumn.MAX_VALUE)
            .value(TextColumn.of("text2", "another_value"))
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .build();

    // Act
    Upsert newUpsert =
        Upsert.newBuilder(existingUpsert)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .partitionKey(partitionKey2)
            .clusteringKey(clusteringKey2)
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
            .textValue("text", "another_value")
            .dateValue("date1", LocalDate.ofEpochDay(123))
            .timeValue("time1", LocalTime.ofSecondOfDay(456))
            .timestampValue(
                "timestamp1", LocalDateTime.of(LocalDate.ofEpochDay(12354), LocalTime.NOON))
            .timestampTZValue("timestampTZ1", Instant.ofEpochSecond(-12))
            .value(TextColumn.of("text2", "foo"))
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .build();

    // Assert
    assertThat(newUpsert.forNamespace()).hasValue(NAMESPACE_2);
    assertThat(newUpsert.forTable()).hasValue(TABLE_2);
    Assertions.<Key>assertThat(newUpsert.getPartitionKey()).isEqualTo(partitionKey2);
    assertThat(newUpsert.getClusteringKey()).hasValue(clusteringKey2);
    assertThat(newUpsert.getColumns().size()).isEqualTo(18);
    assertThat(newUpsert.getColumns().get("bigint1").getBigIntValue())
        .isEqualTo(BigIntColumn.MIN_VALUE);
    assertThat(newUpsert.getColumns().get("bigint2").getBigIntValue())
        .isEqualTo(Long.valueOf(BigIntColumn.MIN_VALUE));
    assertThat(newUpsert.getColumns().get("blob1").getBlobValueAsBytes())
        .isEqualTo("foo".getBytes(StandardCharsets.UTF_8));
    assertThat(newUpsert.getColumns().get("blob2").getBlobValueAsByteBuffer())
        .isEqualTo(ByteBuffer.allocate(2));
    assertThat(newUpsert.getColumns().get("bool1").getBooleanValue()).isFalse();
    assertThat(newUpsert.getColumns().get("bool2").getBooleanValue()).isFalse();
    assertThat(newUpsert.getColumns().get("double1").getDoubleValue()).isEqualTo(Double.MIN_VALUE);
    assertThat(newUpsert.getColumns().get("double2").getDoubleValue())
        .isEqualTo(Double.valueOf(Double.MIN_VALUE));
    assertThat(newUpsert.getColumns().get("float1").getFloatValue()).isEqualTo(Float.MIN_VALUE);
    assertThat(newUpsert.getColumns().get("float2").getFloatValue())
        .isEqualTo(Float.valueOf(Float.MIN_VALUE));
    assertThat(newUpsert.getColumns().get("int1").getIntValue()).isEqualTo(Integer.MIN_VALUE);
    assertThat(newUpsert.getColumns().get("int2").getIntValue())
        .isEqualTo(Integer.valueOf(Integer.MIN_VALUE));
    assertThat(newUpsert.getColumns().get("text").getTextValue()).isEqualTo("another_value");
    assertThat(newUpsert.getColumns().get("text2").getTextValue()).isEqualTo("foo");
    assertThat(newUpsert.getColumns().get("date1").getDateValue())
        .isEqualTo(LocalDate.ofEpochDay(123));
    assertThat(newUpsert.getColumns().get("time1").getTimeValue())
        .isEqualTo(LocalTime.ofSecondOfDay(456));
    assertThat(newUpsert.getColumns().get("timestamp1").getTimestampValue())
        .isEqualTo(LocalDateTime.of(LocalDate.ofEpochDay(12354), LocalTime.NOON));
    assertThat(newUpsert.getColumns().get("timestampTZ1").getTimestampTZValue())
        .isEqualTo(Instant.ofEpochSecond(-12));
    assertThat(newUpsert.getAttributes())
        .isEqualTo(ImmutableMap.of("a4", "v4", "a5", "v5", "a6", "v6"));
  }

  @Test
  public void build_FromExistingAndClearValue_ShouldBuildUpsertWithoutClearedValues() {
    // Arrange
    Upsert existingUpsert =
        Upsert.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .booleanValue("bool", Boolean.TRUE)
            .doubleValue("double", Double.MIN_VALUE)
            .build();

    // Act
    Upsert newUpsert =
        Upsert.newBuilder(existingUpsert).clearValue("double").clearValue("unknown_column").build();

    // Assert
    assertThat(newUpsert.forNamespace()).hasValue(NAMESPACE_1);
    assertThat(newUpsert.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(newUpsert.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(newUpsert.getClusteringKey()).isEmpty();
    assertThat(newUpsert.getColumns().size()).isEqualTo(1);
    assertThat(newUpsert.getColumns().get("bool").getBooleanValue()).isTrue();
  }

  @Test
  public void build_FromExistingAndClearClusteringKey_ShouldBuildUpsertWithoutClusteringKey() {
    // Arrange
    Upsert existingUpsert =
        Upsert.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .build();

    // Act
    Upsert newUpsert = Upsert.newBuilder(existingUpsert).clearClusteringKey().build();

    // Assert
    assertThat(newUpsert.forNamespace()).hasValue(NAMESPACE_1);
    assertThat(newUpsert.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(newUpsert.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(newUpsert.getClusteringKey()).isEmpty();
    assertThat(newUpsert.getColumns()).isEmpty();
  }

  @Test
  public void build_FromExistingAndClearNamespace_ShouldBuildUpsertWithoutNamespace() {
    // Arrange
    Upsert existingUpsert =
        Upsert.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .build();

    // Act
    Upsert newUpsert = Upsert.newBuilder(existingUpsert).clearNamespace().build();

    // Assert
    assertThat(newUpsert.forNamespace()).isEmpty();
    assertThat(newUpsert.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(newUpsert.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(newUpsert.getClusteringKey()).hasValue(clusteringKey1);
    assertThat(newUpsert.getColumns()).isEmpty();
  }
}
