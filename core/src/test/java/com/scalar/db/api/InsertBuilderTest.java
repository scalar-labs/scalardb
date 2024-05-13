package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class InsertBuilderTest {
  private static final String NAMESPACE_1 = "namespace1";
  private static final String NAMESPACE_2 = "namespace2";

  private static final String TABLE_1 = "table1";
  private static final String TABLE_2 = "table2";
  @Mock private Key partitionKey1;
  @Mock private Key partitionKey2;
  @Mock private Key clusteringKey1;
  @Mock private Key clusteringKey2;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void build_WithMandatoryParameters_ShouldBuildInsertWithMandatoryParameters() {
    // Arrange Act
    Insert actual = Insert.newBuilder().table(TABLE_1).partitionKey(partitionKey1).build();

    // Assert
    assertThat(actual.forNamespace()).isEmpty();
    assertThat(actual.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(actual.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(actual.getClusteringKey()).isEmpty();
    assertThat(actual.getColumns()).isEmpty();
  }

  @Test
  public void build_WithClusteringKey_ShouldBuildInsertWithClusteringKey() {
    // Arrange Act
    Insert actual =
        Insert.newBuilder()
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
  public void build_WithAllParameters_ShouldBuildInsertCorrectly() {
    // Arrange Act
    Insert actual =
        Insert.newBuilder()
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
            .value(TextColumn.of("text2", "another_value"))
            .build();

    // Assert
    assertThat(actual.forNamespace()).hasValue(NAMESPACE_1);
    assertThat(actual.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(actual.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(actual.getClusteringKey()).hasValue(clusteringKey1);
    assertThat(actual.getColumns().size()).isEqualTo(14);
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
  }

  @Test
  public void build_WithAllValuesToNull_ShouldBuildInsertCorrectly() {
    // Arrange Act
    Insert actual =
        Insert.newBuilder()
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
            .build();

    // Assert
    assertThat(actual.forNamespace()).hasValue(NAMESPACE_1);
    assertThat(actual.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(actual.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(actual.getClusteringKey()).isEmpty();
    assertThat(actual.getColumns().size()).isEqualTo(8);
    assertThat(actual.getColumns().get("bigint").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("blob1").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("blob2").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("bool").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("double").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("float").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("int").hasNullValue()).isTrue();
    assertThat(actual.getColumns().get("text").hasNullValue()).isTrue();
  }

  @Test
  public void build_WithTwoValueUsingSameColumnName_ShouldBuildInsertCorrectly() {
    // Arrange Act
    Insert actual =
        Insert.newBuilder()
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
    Insert existingInsert =
        Insert.newBuilder()
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
            .value(TextColumn.of("text2", "another_value"))
            .build();

    // Act
    Insert newInsert = Insert.newBuilder(existingInsert).build();

    // Assert
    assertThat(newInsert).isEqualTo(existingInsert);
  }

  @Test
  public void build_FromExistingAndUpdateAllParameters_ShouldBuildInsertWithUpdatedParameters() {
    // Arrange
    Insert existingInsert =
        Insert.newBuilder()
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
            .value(TextColumn.of("text2", "another_value"))
            .build();

    // Act
    Insert newInsert =
        Insert.newBuilder(existingInsert)
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
            .value(TextColumn.of("text2", "foo"))
            .build();

    // Assert
    assertThat(newInsert.forNamespace()).hasValue(NAMESPACE_2);
    assertThat(newInsert.forTable()).hasValue(TABLE_2);
    Assertions.<Key>assertThat(newInsert.getPartitionKey()).isEqualTo(partitionKey2);
    assertThat(newInsert.getClusteringKey()).hasValue(clusteringKey2);
    assertThat(newInsert.getColumns().size()).isEqualTo(14);
    assertThat(newInsert.getColumns().get("bigint1").getBigIntValue())
        .isEqualTo(BigIntColumn.MIN_VALUE);
    assertThat(newInsert.getColumns().get("bigint2").getBigIntValue())
        .isEqualTo(Long.valueOf(BigIntColumn.MIN_VALUE));
    assertThat(newInsert.getColumns().get("blob1").getBlobValueAsBytes())
        .isEqualTo("foo".getBytes(StandardCharsets.UTF_8));
    assertThat(newInsert.getColumns().get("blob2").getBlobValueAsByteBuffer())
        .isEqualTo(ByteBuffer.allocate(2));
    assertThat(newInsert.getColumns().get("bool1").getBooleanValue()).isFalse();
    assertThat(newInsert.getColumns().get("bool2").getBooleanValue()).isFalse();
    assertThat(newInsert.getColumns().get("double1").getDoubleValue()).isEqualTo(Double.MIN_VALUE);
    assertThat(newInsert.getColumns().get("double2").getDoubleValue())
        .isEqualTo(Double.valueOf(Double.MIN_VALUE));
    assertThat(newInsert.getColumns().get("float1").getFloatValue()).isEqualTo(Float.MIN_VALUE);
    assertThat(newInsert.getColumns().get("float2").getFloatValue())
        .isEqualTo(Float.valueOf(Float.MIN_VALUE));
    assertThat(newInsert.getColumns().get("int1").getIntValue()).isEqualTo(Integer.MIN_VALUE);
    assertThat(newInsert.getColumns().get("int2").getIntValue())
        .isEqualTo(Integer.valueOf(Integer.MIN_VALUE));
    assertThat(newInsert.getColumns().get("text").getTextValue()).isEqualTo("another_value");
    assertThat(newInsert.getColumns().get("text2").getTextValue()).isEqualTo("foo");
  }

  @Test
  public void build_FromExistingAndClearValue_ShouldBuildInsertWithoutClearedValues() {
    // Arrange
    Insert existingInsert =
        Insert.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .booleanValue("bool", Boolean.TRUE)
            .doubleValue("double", Double.MIN_VALUE)
            .build();

    // Act
    Insert newInsert =
        Insert.newBuilder(existingInsert).clearValue("double").clearValue("unknown_column").build();

    // Assert
    assertThat(newInsert.forNamespace()).hasValue(NAMESPACE_1);
    assertThat(newInsert.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(newInsert.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(newInsert.getClusteringKey()).isEmpty();
    assertThat(newInsert.getColumns().size()).isEqualTo(1);
    assertThat(newInsert.getColumns().get("bool").getBooleanValue()).isTrue();
  }

  @Test
  public void build_FromExistingAndClearClusteringKey_ShouldBuildInsertWithoutClusteringKey() {
    // Arrange
    Insert existingInsert =
        Insert.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .build();

    // Act
    Insert newInsert = Insert.newBuilder(existingInsert).clearClusteringKey().build();

    // Assert
    assertThat(newInsert.forNamespace()).hasValue(NAMESPACE_1);
    assertThat(newInsert.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(newInsert.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(newInsert.getClusteringKey()).isEmpty();
    assertThat(newInsert.getColumns()).isEmpty();
  }

  @Test
  public void build_FromExistingAndClearNamespace_ShouldBuildInsertWithoutNamespace() {
    // Arrange
    Insert existingInsert =
        Insert.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .build();

    // Act
    Insert newInsert = Insert.newBuilder(existingInsert).clearNamespace().build();

    // Assert
    assertThat(newInsert.forNamespace()).isEmpty();
    assertThat(newInsert.forTable()).hasValue(TABLE_1);
    Assertions.<Key>assertThat(newInsert.getPartitionKey()).isEqualTo(partitionKey1);
    assertThat(newInsert.getClusteringKey()).hasValue(clusteringKey1);
    assertThat(newInsert.getColumns()).isEmpty();
  }
}
