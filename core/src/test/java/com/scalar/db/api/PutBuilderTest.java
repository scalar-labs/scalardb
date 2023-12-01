package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PutBuilderTest {
  private static final String NAMESPACE_1 = "namespace1";
  private static final String NAMESPACE_2 = "namespace2";

  private static final String TABLE_1 = "table1";
  private static final String TABLE_2 = "table2";
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
            .textValue("text", "a_value")
            .value(TextColumn.of("text2", "another_value"))
            .condition(condition1)
            .disableImplicitPreRead()
            .build();

    // Assert
    assertThat(put)
        .isEqualTo(
            new Put(partitionKey1, clusteringKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConsistency(Consistency.EVENTUAL)
                .withBigIntValue("bigint1", BigIntColumn.MAX_VALUE)
                .withBigIntValue("bigint2", Long.valueOf(BigIntColumn.MAX_VALUE))
                .withBlobValue("blob1", "blob".getBytes(StandardCharsets.UTF_8))
                .withBlobValue("blob2", ByteBuffer.allocate(1))
                .withBooleanValue("bool1", true)
                .withBooleanValue("bool2", Boolean.TRUE)
                .withDoubleValue("double1", Double.MAX_VALUE)
                .withDoubleValue("double2", Double.valueOf(Double.MAX_VALUE))
                .withFloatValue("float1", Float.MAX_VALUE)
                .withFloatValue("float2", Float.valueOf(Float.MAX_VALUE))
                .withIntValue("int1", Integer.MAX_VALUE)
                .withIntValue("int2", Integer.valueOf(Integer.MAX_VALUE))
                .withTextValue("text", "a_value")
                .withValue(TextColumn.of("text2", "another_value"))
                .withCondition(condition1)
                .setImplicitPreReadEnabled(false));
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
            .build();

    // Assert
    assertThat(put)
        .isEqualTo(
            new Put(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withBigIntValue("bigint", null)
                .withBlobValue("blob1", (byte[]) null)
                .withBlobValue("blob2", (ByteBuffer) null)
                .withBooleanValue("bool", null)
                .withDoubleValue("double", null)
                .withFloatValue("float", null)
                .withIntValue("int", null)
                .withTextValue("text", null));
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
        new Put(partitionKey1, clusteringKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withConsistency(Consistency.EVENTUAL)
            .withBigIntValue("bigint1", BigIntColumn.MAX_VALUE)
            .withBigIntValue("bigint2", Long.valueOf(BigIntColumn.MAX_VALUE))
            .withBlobValue("blob1", "blob".getBytes(StandardCharsets.UTF_8))
            .withBlobValue("blob2", ByteBuffer.allocate(1))
            .withBooleanValue("bool1", true)
            .withBooleanValue("bool2", Boolean.TRUE)
            .withDoubleValue("double1", Double.MAX_VALUE)
            .withDoubleValue("double2", Double.valueOf(Double.MAX_VALUE))
            .withFloatValue("float1", Float.MAX_VALUE)
            .withFloatValue("float2", Float.valueOf(Float.MAX_VALUE))
            .withIntValue("int1", Integer.MAX_VALUE)
            .withIntValue("int2", Integer.valueOf(Integer.MAX_VALUE))
            .withTextValue("text", "a_value")
            .withCondition(condition1)
            .setImplicitPreReadEnabled(true);

    // Act
    Put newPut = Put.newBuilder(existingPut).build();

    // Assert
    assertThat(newPut).isEqualTo(existingPut);
  }

  @Test
  public void build_FromExistingAndUpdateAllParameters_ShouldBuildPutWithUpdatedParameters() {
    // Arrange
    Put existingPut =
        new Put(partitionKey1, clusteringKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withConsistency(Consistency.EVENTUAL)
            .withBigIntValue("bigint1", BigIntColumn.MAX_VALUE)
            .withBigIntValue("bigint2", Long.valueOf(BigIntColumn.MAX_VALUE))
            .withBlobValue("blob1", "blob".getBytes(StandardCharsets.UTF_8))
            .withBlobValue("blob2", ByteBuffer.allocate(1))
            .withBooleanValue("bool1", true)
            .withBooleanValue("bool2", Boolean.TRUE)
            .withDoubleValue("double1", Double.MAX_VALUE)
            .withDoubleValue("double2", Double.valueOf(Double.MAX_VALUE))
            .withFloatValue("float1", Float.MAX_VALUE)
            .withFloatValue("float2", Float.valueOf(Float.MAX_VALUE))
            .withIntValue("int1", Integer.MAX_VALUE)
            .withIntValue("int2", Integer.valueOf(Integer.MAX_VALUE))
            .withTextValue("text", "a_value")
            .withValue(TextColumn.of("text2", "another_value"))
            .withCondition(condition1);

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
            .textValue("text", "another_value")
            .value(TextColumn.of("text2", "foo"))
            .condition(condition2)
            .enableImplicitPreRead()
            .build();

    // Assert
    assertThat(newPut)
        .isEqualTo(
            new Put(partitionKey2, clusteringKey2)
                .forNamespace(NAMESPACE_2)
                .forTable(TABLE_2)
                .withConsistency(Consistency.LINEARIZABLE)
                .withBigIntValue("bigint1", BigIntColumn.MIN_VALUE)
                .withBigIntValue("bigint2", Long.valueOf(BigIntColumn.MIN_VALUE))
                .withBlobValue("blob1", "foo".getBytes(StandardCharsets.UTF_8))
                .withBlobValue("blob2", ByteBuffer.allocate(2))
                .withBooleanValue("bool1", false)
                .withBooleanValue("bool2", Boolean.FALSE)
                .withDoubleValue("double1", Double.MIN_VALUE)
                .withDoubleValue("double2", Double.valueOf(Double.MIN_VALUE))
                .withFloatValue("float1", Float.MIN_VALUE)
                .withFloatValue("float2", Float.valueOf(Float.MIN_VALUE))
                .withIntValue("int1", Integer.MIN_VALUE)
                .withIntValue("int2", Integer.valueOf(Integer.MIN_VALUE))
                .withTextValue("text", "another_value")
                .withTextValue("text2", "foo")
                .withCondition(condition2)
                .setImplicitPreReadEnabled(true));
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
