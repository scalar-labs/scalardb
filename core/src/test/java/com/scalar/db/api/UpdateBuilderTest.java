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

public class UpdateBuilderTest {
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
  public void build_WithMandatoryParameters_ShouldBuildUpdateWithMandatoryParameters() {
    // Arrange Act
    Update actual = Update.newBuilder().table(TABLE_1).partitionKey(partitionKey1).build();

    // Assert
    assertThat(actual.forNamespace()).isEmpty();
    assertThat(actual.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(actual.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(actual.getClusteringKey()).isEmpty();
    assertThat(actual.getColumns()).isEmpty();
    assertThat(actual.getCondition()).isEmpty();
  }

  @Test
  public void build_WithClusteringKey_ShouldBuildUpdateWithClusteringKey() {
    // Arrange Act
    Update actual =
        Update.newBuilder()
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
    assertThat(actual.getCondition()).isEmpty();
  }

  @Test
  public void build_WithAllParameters_ShouldBuildUpdateCorrectly() {
    // Arrange Act
    Update actual =
        Update.newBuilder()
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
            .dateValue("date1", ANY_DATE)
            .timeValue("time1", ANY_TIME)
            .timestampValue("timestamp1", ANY_TIMESTAMP)
            .timestampTZValue("timestampTZ1", ANY_TIMESTAMPTZ)
            .value(TextColumn.of("text2", "another_value"))
            .condition(condition1)
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
    assertThat(actual.getColumns().get("text2").getTextValue()).isEqualTo("another_value");
    assertThat(actual.getColumns().get("date1").getDateValue()).isEqualTo(ANY_DATE);
    assertThat(actual.getColumns().get("time1").getTimeValue()).isEqualTo(ANY_TIME);
    assertThat(actual.getColumns().get("timestamp1").getTimestampValue()).isEqualTo(ANY_TIMESTAMP);
    assertThat(actual.getColumns().get("timestampTZ1").getTimestampTZValue())
        .isEqualTo(ANY_TIMESTAMPTZ);
    assertThat(actual.getCondition()).hasValue(condition1);
    assertThat(actual.getAttributes())
        .isEqualTo(ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"));
  }

  @Test
  public void build_WithAllValuesToNull_ShouldBuildUpdateCorrectly() {
    // Arrange Act
    Update actual =
        Update.newBuilder()
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
            .timestampTZValue("timestampTZ", null)
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
    assertThat(actual.getColumns().get("timestampTZ").hasNullValue()).isTrue();
    assertThat(actual.getCondition()).isEmpty();
  }

  @Test
  public void build_WithTwoValueUsingSameColumnName_ShouldBuildUpdateCorrectly() {
    // Arrange Act
    Update actual =
        Update.newBuilder()
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
    assertThat(actual.getCondition()).isEmpty();
  }

  @Test
  public void build_FromExistingWithoutChange_ShouldCopy() {
    // Arrange
    Update existingUpdate =
        Update.newBuilder()
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
            .dateValue("date1", ANY_DATE)
            .timeValue("time1", ANY_TIME)
            .timestampValue("timestamp1", ANY_TIMESTAMP)
            .timestampTZValue("timestampTZ1", ANY_TIMESTAMPTZ)
            .value(TextColumn.of("text2", "another_value"))
            .dateValue("date1", ANY_DATE)
            .timeValue("time1", ANY_TIME)
            .timestampValue("timestamp1", ANY_TIMESTAMP)
            .timestampTZValue("timestampTZ1", ANY_TIMESTAMPTZ)
            .condition(condition1)
            .build();

    // Act
    Update newUpdate = Update.newBuilder(existingUpdate).build();

    // Assert
    assertThat(newUpdate).isEqualTo(existingUpdate);
  }

  @Test
  public void build_FromExistingAndUpdateAllParameters_ShouldBuildUpdateWithUpdatedParameters() {
    // Arrange
    Update existingUpdate =
        Update.newBuilder()
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
            .dateValue("date1", DateColumn.MAX_VALUE)
            .timeValue("time1", TimeColumn.MAX_VALUE)
            .timestampValue("timestamp1", TimestampColumn.MAX_VALUE)
            .timestampTZValue("timestampTZ1", TimestampTZColumn.MAX_VALUE)
            .value(TextColumn.of("text2", "another_value"))
            .condition(condition1)
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .build();

    // Act
    Update newUpdate =
        Update.newBuilder(existingUpdate)
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
            .condition(condition2)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .build();

    // Assert
    assertThat(newUpdate.forNamespace()).hasValue(NAMESPACE_2);
    assertThat(newUpdate.forTable()).hasValue(TABLE_2);
    Assertions.<Key>assertThat(newUpdate.getPartitionKey()).isEqualTo(partitionKey2);
    assertThat(newUpdate.getClusteringKey()).hasValue(clusteringKey2);
    assertThat(newUpdate.getColumns().size()).isEqualTo(18);
    assertThat(newUpdate.getColumns().get("bigint1").getBigIntValue())
        .isEqualTo(BigIntColumn.MIN_VALUE);
    assertThat(newUpdate.getColumns().get("bigint2").getBigIntValue())
        .isEqualTo(Long.valueOf(BigIntColumn.MIN_VALUE));
    assertThat(newUpdate.getColumns().get("blob1").getBlobValueAsBytes())
        .isEqualTo("foo".getBytes(StandardCharsets.UTF_8));
    assertThat(newUpdate.getColumns().get("blob2").getBlobValueAsByteBuffer())
        .isEqualTo(ByteBuffer.allocate(2));
    assertThat(newUpdate.getColumns().get("bool1").getBooleanValue()).isFalse();
    assertThat(newUpdate.getColumns().get("bool2").getBooleanValue()).isFalse();
    assertThat(newUpdate.getColumns().get("double1").getDoubleValue()).isEqualTo(Double.MIN_VALUE);
    assertThat(newUpdate.getColumns().get("double2").getDoubleValue())
        .isEqualTo(Double.valueOf(Double.MIN_VALUE));
    assertThat(newUpdate.getColumns().get("float1").getFloatValue()).isEqualTo(Float.MIN_VALUE);
    assertThat(newUpdate.getColumns().get("float2").getFloatValue())
        .isEqualTo(Float.valueOf(Float.MIN_VALUE));
    assertThat(newUpdate.getColumns().get("int1").getIntValue()).isEqualTo(Integer.MIN_VALUE);
    assertThat(newUpdate.getColumns().get("int2").getIntValue())
        .isEqualTo(Integer.valueOf(Integer.MIN_VALUE));
    assertThat(newUpdate.getColumns().get("text").getTextValue()).isEqualTo("another_value");
    assertThat(newUpdate.getColumns().get("date1").getDateValue())
        .isEqualTo(LocalDate.ofEpochDay(123));
    assertThat(newUpdate.getColumns().get("time1").getTimeValue())
        .isEqualTo(LocalTime.ofSecondOfDay(456));
    assertThat(newUpdate.getColumns().get("timestamp1").getTimestampValue())
        .isEqualTo(LocalDateTime.of(LocalDate.ofEpochDay(12354), LocalTime.NOON));
    assertThat(newUpdate.getColumns().get("timestampTZ1").getTimestampTZValue())
        .isEqualTo(Instant.ofEpochSecond(-12));
    assertThat(newUpdate.getColumns().get("text2").getTextValue()).isEqualTo("foo");
    assertThat(newUpdate.getCondition()).hasValue(condition2);
    assertThat(newUpdate.getAttributes())
        .isEqualTo(ImmutableMap.of("a4", "v4", "a5", "v5", "a6", "v6"));
  }

  @Test
  public void build_FromExistingAndClearValue_ShouldBuildUpdateWithoutClearedValues() {
    // Arrange
    Update existingUpdate =
        Update.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .booleanValue("bool", Boolean.TRUE)
            .doubleValue("double", Double.MIN_VALUE)
            .build();

    // Act
    Update newUpdate =
        Update.newBuilder(existingUpdate).clearValue("double").clearValue("unknown_column").build();

    // Assert
    assertThat(newUpdate.forNamespace()).hasValue(NAMESPACE_1);
    assertThat(newUpdate.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(newUpdate.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(newUpdate.getClusteringKey()).isEmpty();
    assertThat(newUpdate.getColumns().size()).isEqualTo(1);
    assertThat(newUpdate.getColumns().get("bool").getBooleanValue()).isTrue();
    assertThat(newUpdate.getCondition()).isEmpty();
  }

  @Test
  public void build_FromExistingAndClearCondition_ShouldBuildUpdateWithoutCondition() {
    // Arrange
    Update existingUpdate =
        Update.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .condition(condition1)
            .build();

    // Act
    Update newUpdate = Update.newBuilder(existingUpdate).clearCondition().build();

    // Assert
    assertThat(newUpdate.forNamespace()).hasValue(NAMESPACE_1);
    assertThat(newUpdate.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(newUpdate.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(newUpdate.getClusteringKey()).isEmpty();
    assertThat(newUpdate.getColumns()).isEmpty();
    assertThat(newUpdate.getCondition()).isEmpty();
  }

  @Test
  public void build_FromExistingAndClearClusteringKey_ShouldBuildUpdateWithoutClusteringKey() {
    // Arrange
    Update existingUpdate =
        Update.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .build();

    // Act
    Update newUpdate = Update.newBuilder(existingUpdate).clearClusteringKey().build();

    // Assert
    assertThat(newUpdate.forNamespace()).hasValue(NAMESPACE_1);
    assertThat(newUpdate.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(newUpdate.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(newUpdate.getClusteringKey()).isEmpty();
    assertThat(newUpdate.getColumns()).isEmpty();
    assertThat(newUpdate.getCondition()).isEmpty();
  }

  @Test
  public void build_FromExistingAndClearNamespace_ShouldBuildUpdateWithoutNamespace() {
    // Arrange
    Update existingUpdate =
        Update.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .build();

    // Act
    Update newUpdate = Update.newBuilder(existingUpdate).clearNamespace().build();

    // Assert
    assertThat(newUpdate.forNamespace()).isEmpty();
    assertThat(newUpdate.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(newUpdate.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(newUpdate.getClusteringKey()).hasValue(clusteringKey1);
    assertThat(newUpdate.getColumns()).isEmpty();
    assertThat(newUpdate.getCondition()).isEmpty();
  }
}
