package com.scalar.db.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.rpc.ConditionalExpression;
import com.scalar.db.rpc.ConditionalExpression.Operator;
import com.scalar.db.rpc.MutateCondition;
import com.scalar.db.rpc.Order;
import com.scalar.db.rpc.Value;
import com.scalar.db.rpc.Value.BlobValue;
import com.scalar.db.rpc.Value.TextValue;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class ProtoUtilsTest {

  private final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn("p1", DataType.INT)
          .addColumn("p2", DataType.TEXT)
          .addColumn("c1", DataType.TEXT)
          .addColumn("c2", DataType.INT)
          .addColumn("col1", DataType.BOOLEAN)
          .addColumn("col2", DataType.INT)
          .addColumn("col3", DataType.BIGINT)
          .addColumn("col4", DataType.FLOAT)
          .addColumn("col5", DataType.DOUBLE)
          .addColumn("col6", DataType.TEXT)
          .addColumn("col7", DataType.BLOB)
          .addPartitionKey("p1")
          .addPartitionKey("p2")
          .addClusteringKey("c1", Scan.Ordering.Order.DESC)
          .addClusteringKey("c2", Scan.Ordering.Order.ASC)
          .addSecondaryIndex("col2")
          .addSecondaryIndex("col4")
          .build();

  @Test
  public void toGet_GetGiven_ShouldConvertProperly() {
    // Arrange
    Get get =
        Get.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.of("p1", 10, "p2", "text1"))
            .clusteringKey(Key.of("c1", "text2", "c2", 20))
            .projections("col1", "col2", "col3")
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Act
    com.scalar.db.rpc.Get actual = ProtoUtils.toGet(get);

    // Assert
    assertThat(actual)
        .isEqualTo(
            com.scalar.db.rpc.Get.newBuilder()
                .setPartitionKey(
                    com.scalar.db.rpc.Key.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p1")
                                .setIntValue(10)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p2")
                                .setTextValue("text1")
                                .build())
                        .build())
                .setClusteringKey(
                    com.scalar.db.rpc.Key.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c1")
                                .setTextValue("text2")
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c2")
                                .setIntValue(20)
                                .build())
                        .build())
                .addAllProjections(Arrays.asList("col1", "col2", "col3"))
                .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_LINEARIZABLE)
                .setNamespace("ns")
                .setTable("tbl")
                .build());
  }

  @Test
  public void toGet_ProtoGetGiven_ShouldConvertProperly() {
    // Arrange
    com.scalar.db.rpc.Get get =
        com.scalar.db.rpc.Get.newBuilder()
            .setPartitionKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder().setName("p1").setIntValue(10).build())
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder()
                            .setName("p2")
                            .setTextValue("text1")
                            .build())
                    .build())
            .setClusteringKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder()
                            .setName("c1")
                            .setTextValue("text2")
                            .build())
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder().setName("c2").setIntValue(20).build())
                    .build())
            .addAllProjections(Arrays.asList("col1", "col2", "col3"))
            .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_LINEARIZABLE)
            .setNamespace("ns")
            .setTable("tbl")
            .build();

    // Act
    Get actual = ProtoUtils.toGet(get, TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            Get.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.of("p1", 10, "p2", "text1"))
                .clusteringKey(Key.of("c1", "text2", "c2", 20))
                .projections("col1", "col2", "col3")
                .consistency(Consistency.LINEARIZABLE)
                .build());
  }

  @Test
  public void toGet_GetWithIndexGiven_ShouldConvertProperly() {
    // Arrange
    Get getWithIndex =
        Get.newBuilder()
            .namespace("ns")
            .table("tbl")
            .indexKey(Key.ofInt("col2", 1))
            .projections("col1", "col2", "col3")
            .consistency(Consistency.SEQUENTIAL)
            .build();

    // Act
    com.scalar.db.rpc.Get actual = ProtoUtils.toGet(getWithIndex);

    // Assert
    assertThat(actual)
        .isEqualTo(
            com.scalar.db.rpc.Get.newBuilder()
                .setPartitionKey(
                    com.scalar.db.rpc.Key.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col2")
                                .setIntValue(1)
                                .build()))
                .addAllProjections(Arrays.asList("col1", "col2", "col3"))
                .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_SEQUENTIAL)
                .setNamespace("ns")
                .setTable("tbl")
                .build());
  }

  @Test
  public void tGet_ProtoScanForGetWithIndexGiven_ShouldConvertProperly() {
    // Arrange
    com.scalar.db.rpc.Get get =
        com.scalar.db.rpc.Get.newBuilder()
            .setPartitionKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder()
                            .setName("col2")
                            .setIntValue(1)
                            .build()))
            .addAllProjections(Arrays.asList("col1", "col2", "col3"))
            .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_SEQUENTIAL)
            .setNamespace("ns")
            .setTable("tbl")
            .build();

    // Act
    Get actual = ProtoUtils.toGet(get, TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            Get.newBuilder()
                .namespace("ns")
                .table("tbl")
                .indexKey(Key.ofInt("col2", 1))
                .projections("col1", "col2", "col3")
                .consistency(Consistency.SEQUENTIAL)
                .build());
  }

  @Test
  public void toGet_ProtoGetFromOldClientGiven_ShouldConvertProperly() {
    // Arrange
    com.scalar.db.rpc.Get get =
        com.scalar.db.rpc.Get.newBuilder()
            .setPartitionKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addValue(Value.newBuilder().setName("p1").setIntValue(10).build())
                    .addValue(
                        Value.newBuilder()
                            .setName("p2")
                            .setTextValue(TextValue.newBuilder().setValue("text1").build())
                            .build())
                    .build())
            .setClusteringKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addValue(
                        Value.newBuilder()
                            .setName("c1")
                            .setTextValue(TextValue.newBuilder().setValue("text2").build())
                            .build())
                    .addValue(Value.newBuilder().setName("c2").setIntValue(20).build())
                    .build())
            .addAllProjections(Arrays.asList("col1", "col2", "col3"))
            .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_LINEARIZABLE)
            .setNamespace("ns")
            .setTable("tbl")
            .build();

    // Act
    Get actual = ProtoUtils.toGet(get, TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            Get.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.of("p1", 10, "p2", "text1"))
                .clusteringKey(Key.of("c1", "text2", "c2", 20))
                .projections("col1", "col2", "col3")
                .consistency(Consistency.LINEARIZABLE)
                .build());
  }

  @Test
  public void toScan_ScanGiven_ShouldConvertProperly() {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.of("p1", 10, "p2", "text1"))
            .start(Key.of("c1", "text2", "c2", 0))
            .end(Key.of("c1", "text2", "c2", 100), false)
            .orderings(Ordering.desc("c1"), Ordering.asc("c2"))
            .limit(10)
            .projections("col1", "col2", "col3")
            .consistency(Consistency.SEQUENTIAL)
            .build();

    // Act
    com.scalar.db.rpc.Scan actual = ProtoUtils.toScan(scan);

    // Assert
    assertThat(actual)
        .isEqualTo(
            com.scalar.db.rpc.Scan.newBuilder()
                .setPartitionKey(
                    com.scalar.db.rpc.Key.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p1")
                                .setIntValue(10)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p2")
                                .setTextValue("text1")
                                .build())
                        .build())
                .setStartClusteringKey(
                    com.scalar.db.rpc.Key.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c1")
                                .setTextValue("text2")
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c2")
                                .setIntValue(0)
                                .build())
                        .build())
                .setStartInclusive(true)
                .setEndClusteringKey(
                    com.scalar.db.rpc.Key.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c1")
                                .setTextValue("text2")
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c2")
                                .setIntValue(100)
                                .build())
                        .build())
                .setEndInclusive(false)
                .addAllOrderings(
                    Arrays.asList(
                        com.scalar.db.rpc.Ordering.newBuilder()
                            .setName("c1")
                            .setOrder(Order.ORDER_DESC)
                            .build(),
                        com.scalar.db.rpc.Ordering.newBuilder()
                            .setName("c2")
                            .setOrder(Order.ORDER_ASC)
                            .build()))
                .setLimit(10)
                .addAllProjections(Arrays.asList("col1", "col2", "col3"))
                .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_SEQUENTIAL)
                .setNamespace("ns")
                .setTable("tbl")
                .build());
  }

  @Test
  public void toScan_ProtoScanGiven_ShouldConvertProperly() {
    // Arrange
    com.scalar.db.rpc.Scan scan =
        com.scalar.db.rpc.Scan.newBuilder()
            .setPartitionKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder().setName("p1").setIntValue(10).build())
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder()
                            .setName("p2")
                            .setTextValue("text1")
                            .build())
                    .build())
            .setStartClusteringKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder()
                            .setName("c1")
                            .setTextValue("text2")
                            .build())
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder().setName("c2").setIntValue(0).build())
                    .build())
            .setStartInclusive(true)
            .setEndClusteringKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder()
                            .setName("c1")
                            .setTextValue("text2")
                            .build())
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder()
                            .setName("c2")
                            .setIntValue(100)
                            .build())
                    .build())
            .setEndInclusive(false)
            .addAllOrderings(
                Arrays.asList(
                    com.scalar.db.rpc.Ordering.newBuilder()
                        .setName("c1")
                        .setOrder(Order.ORDER_DESC)
                        .build(),
                    com.scalar.db.rpc.Ordering.newBuilder()
                        .setName("c2")
                        .setOrder(Order.ORDER_ASC)
                        .build()))
            .setLimit(10)
            .addAllProjections(Arrays.asList("col1", "col2", "col3"))
            .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_SEQUENTIAL)
            .setNamespace("ns")
            .setTable("tbl")
            .build();

    // Act
    Scan actual = ProtoUtils.toScan(scan, TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            Scan.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.of("p1", 10, "p2", "text1"))
                .start(Key.of("c1", "text2", "c2", 0))
                .end(Key.of("c1", "text2", "c2", 100), false)
                .orderings(Ordering.desc("c1"), Ordering.asc("c2"))
                .limit(10)
                .projections("col1", "col2", "col3")
                .consistency(Consistency.SEQUENTIAL)
                .build());
  }

  @Test
  public void toScan_ProtoScanFromOldClientGiven_ShouldConvertProperly() {
    // Arrange
    com.scalar.db.rpc.Scan scan =
        com.scalar.db.rpc.Scan.newBuilder()
            .setPartitionKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addValue(Value.newBuilder().setName("p1").setIntValue(10).build())
                    .addValue(
                        Value.newBuilder()
                            .setName("p2")
                            .setTextValue(TextValue.newBuilder().setValue("text1").build())
                            .build())
                    .build())
            .setStartClusteringKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addValue(
                        Value.newBuilder()
                            .setName("c1")
                            .setTextValue(TextValue.newBuilder().setValue("text2").build())
                            .build())
                    .addValue(Value.newBuilder().setName("c2").setIntValue(0).build())
                    .build())
            .setStartInclusive(true)
            .setEndClusteringKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addValue(
                        Value.newBuilder()
                            .setName("c1")
                            .setTextValue(TextValue.newBuilder().setValue("text2").build())
                            .build())
                    .addValue(Value.newBuilder().setName("c2").setIntValue(100).build())
                    .build())
            .setEndInclusive(false)
            .addAllOrderings(
                Arrays.asList(
                    com.scalar.db.rpc.Ordering.newBuilder()
                        .setName("c1")
                        .setOrder(Order.ORDER_DESC)
                        .build(),
                    com.scalar.db.rpc.Ordering.newBuilder()
                        .setName("c2")
                        .setOrder(Order.ORDER_ASC)
                        .build()))
            .setLimit(10)
            .addAllProjections(Arrays.asList("col1", "col2", "col3"))
            .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_SEQUENTIAL)
            .setNamespace("ns")
            .setTable("tbl")
            .build();

    // Act
    Scan actual = ProtoUtils.toScan(scan, TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            Scan.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.of("p1", 10, "p2", "text1"))
                .start(Key.of("c1", "text2", "c2", 0))
                .end(Key.of("c1", "text2", "c2", 100), false)
                .orderings(Ordering.desc("c1"), Ordering.asc("c2"))
                .limit(10)
                .projections("col1", "col2", "col3")
                .consistency(Consistency.SEQUENTIAL)
                .build());
  }

  @Test
  public void toScan_ScanWithIndexGiven_ShouldConvertProperly() {
    // Arrange
    Scan scanWithIndex =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .indexKey(Key.ofInt("col2", 1))
            .limit(10)
            .projections("col1", "col2", "col3")
            .consistency(Consistency.SEQUENTIAL)
            .build();

    // Act
    com.scalar.db.rpc.Scan actual = ProtoUtils.toScan(scanWithIndex);

    // Assert
    assertThat(actual)
        .isEqualTo(
            com.scalar.db.rpc.Scan.newBuilder()
                .setPartitionKey(
                    com.scalar.db.rpc.Key.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("col2")
                                .setIntValue(1)
                                .build()))
                .setLimit(10)
                .addAllProjections(Arrays.asList("col1", "col2", "col3"))
                .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_SEQUENTIAL)
                .setNamespace("ns")
                .setTable("tbl")
                .build());
  }

  @Test
  public void toScan_ProtoScanForScanWithIndexGiven_ShouldConvertProperly() {
    // Arrange
    com.scalar.db.rpc.Scan scan =
        com.scalar.db.rpc.Scan.newBuilder()
            .setPartitionKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder()
                            .setName("col2")
                            .setIntValue(1)
                            .build()))
            .setLimit(10)
            .addAllProjections(Arrays.asList("col1", "col2", "col3"))
            .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_SEQUENTIAL)
            .setNamespace("ns")
            .setTable("tbl")
            .build();

    // Act
    Scan actual = ProtoUtils.toScan(scan, TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            Scan.newBuilder()
                .namespace("ns")
                .table("tbl")
                .indexKey(Key.ofInt("col2", 1))
                .limit(10)
                .projections("col1", "col2", "col3")
                .consistency(Consistency.SEQUENTIAL)
                .build());
  }

  @Test
  public void toScan_ScanAllGiven_ShouldConvertProperly() {
    // Arrange
    Scan scanAll =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .all()
            .limit(10)
            .projections("col1", "col2", "col3")
            .consistency(Consistency.SEQUENTIAL)
            .build();

    // Act
    com.scalar.db.rpc.Scan actual = ProtoUtils.toScan(scanAll);

    // Assert
    assertThat(actual)
        .isEqualTo(
            com.scalar.db.rpc.Scan.newBuilder()
                .setLimit(10)
                .addAllProjections(Arrays.asList("col1", "col2", "col3"))
                .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_SEQUENTIAL)
                .setNamespace("ns")
                .setTable("tbl")
                .build());
  }

  @Test
  public void toScan_ProtoScanForScanAllGiven_ShouldConvertProperly() {
    // Arrange
    com.scalar.db.rpc.Scan scan =
        com.scalar.db.rpc.Scan.newBuilder()
            .setLimit(10)
            .addAllProjections(Arrays.asList("col1", "col2", "col3"))
            .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_SEQUENTIAL)
            .setNamespace("ns")
            .setTable("tbl")
            .build();

    // Act
    Scan actual = ProtoUtils.toScan(scan, TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            Scan.newBuilder()
                .namespace("ns")
                .table("tbl")
                .all()
                .limit(10)
                .projections("col1", "col2", "col3")
                .consistency(Consistency.SEQUENTIAL)
                .build());
  }

  @Test
  public void toMutation_PutGiven_ShouldConvertProperly() {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.of("p1", 10, "p2", "text1"))
            .clusteringKey(Key.of("c1", "text2", "c2", 20))
            .booleanValue("col1", true)
            .intValue("col2", 10)
            .bigIntValue("col3", 100L)
            .floatValue("col4", 1.23F)
            .doubleValue("col5", 4.56)
            .textValue("col6", "text")
            .blobValue("col7", "blob".getBytes(StandardCharsets.UTF_8))
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column("col1").isEqualToBoolean(true))
                    .and(ConditionBuilder.column("col1").isNotEqualToBoolean(false))
                    .and(ConditionBuilder.column("col2").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("col3").isGreaterThanOrEqualToBigInt(100L))
                    .and(ConditionBuilder.column("col4").isLessThanFloat(1.23F))
                    .and(ConditionBuilder.column("col5").isLessThanOrEqualToDouble(4.56))
                    .and(ConditionBuilder.column("col6").isNullText())
                    .and(ConditionBuilder.column("col7").isNotNullBlob())
                    .build())
            .consistency(Consistency.EVENTUAL)
            .disableImplicitPreRead()
            .build();

    // Act
    com.scalar.db.rpc.Mutation actual = ProtoUtils.toMutation(put);

    // Assert
    assertThat(actual)
        .isEqualTo(
            com.scalar.db.rpc.Mutation.newBuilder()
                .setType(com.scalar.db.rpc.Mutation.Type.PUT)
                .setPartitionKey(
                    com.scalar.db.rpc.Key.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p1")
                                .setIntValue(10)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p2")
                                .setTextValue("text1")
                                .build())
                        .build())
                .setClusteringKey(
                    com.scalar.db.rpc.Key.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c1")
                                .setTextValue("text2")
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c2")
                                .setIntValue(20)
                                .build())
                        .build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("col1")
                        .setBooleanValue(true)
                        .build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder().setName("col2").setIntValue(10).build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("col3")
                        .setBigintValue(100L)
                        .build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("col4")
                        .setFloatValue(1.23F)
                        .build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("col5")
                        .setDoubleValue(4.56)
                        .build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("col6")
                        .setTextValue("text")
                        .build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("col7")
                        .setBlobValue(ByteString.copyFrom("blob".getBytes(StandardCharsets.UTF_8)))
                        .build())
                .setCondition(
                    MutateCondition.newBuilder()
                        .setType(com.scalar.db.rpc.MutateCondition.Type.PUT_IF)
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col1")
                                        .setBooleanValue(true)
                                        .build())
                                .setOperator(Operator.EQ)
                                .build())
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col1")
                                        .setBooleanValue(false)
                                        .build())
                                .setOperator(Operator.NE)
                                .build())
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col2")
                                        .setIntValue(10)
                                        .build())
                                .setOperator(Operator.GT)
                                .build())
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col3")
                                        .setBigintValue(100L)
                                        .build())
                                .setOperator(Operator.GTE)
                                .build())
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col4")
                                        .setFloatValue(1.23F)
                                        .build())
                                .setOperator(Operator.LT)
                                .build())
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col5")
                                        .setDoubleValue(4.56)
                                        .build())
                                .setOperator(Operator.LTE)
                                .build())
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder().setName("col6").build())
                                .setOperator(Operator.IS_NULL)
                                .build())
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder().setName("col7").build())
                                .setOperator(Operator.IS_NOT_NULL)
                                .build())
                        .build())
                .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_EVENTUAL)
                .setImplicitPreReadEnabled(false)
                .setNamespace("ns")
                .setTable("tbl")
                .build());
  }

  @Test
  public void toMutation_ProtoPutGiven_ShouldConvertProperly() {
    // Arrange
    com.scalar.db.rpc.Mutation put =
        com.scalar.db.rpc.Mutation.newBuilder()
            .setType(com.scalar.db.rpc.Mutation.Type.PUT)
            .setPartitionKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder().setName("p1").setIntValue(10).build())
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder()
                            .setName("p2")
                            .setTextValue("text1")
                            .build())
                    .build())
            .setClusteringKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder()
                            .setName("c1")
                            .setTextValue("text2")
                            .build())
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder().setName("c2").setIntValue(20).build())
                    .build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("col1").setBooleanValue(true).build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("col2").setIntValue(10).build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("col3").setBigintValue(100L).build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("col4").setFloatValue(1.23F).build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("col5").setDoubleValue(4.56).build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("col6").setTextValue("text").build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder()
                    .setName("col7")
                    .setBlobValue(ByteString.copyFrom("blob".getBytes(StandardCharsets.UTF_8)))
                    .build())
            .setCondition(
                MutateCondition.newBuilder()
                    .setType(com.scalar.db.rpc.MutateCondition.Type.PUT_IF)
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder()
                                    .setName("col1")
                                    .setBooleanValue(true)
                                    .build())
                            .setOperator(Operator.EQ)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder()
                                    .setName("col1")
                                    .setBooleanValue(false)
                                    .build())
                            .setOperator(Operator.NE)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder()
                                    .setName("col2")
                                    .setIntValue(10)
                                    .build())
                            .setOperator(Operator.GT)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder()
                                    .setName("col3")
                                    .setBigintValue(100L)
                                    .build())
                            .setOperator(Operator.GTE)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder()
                                    .setName("col4")
                                    .setFloatValue(1.23F)
                                    .build())
                            .setOperator(Operator.LT)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder()
                                    .setName("col5")
                                    .setDoubleValue(4.56)
                                    .build())
                            .setOperator(Operator.LTE)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder().setName("col6").build())
                            .setOperator(Operator.IS_NULL)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder().setName("col7").build())
                            .setOperator(Operator.IS_NOT_NULL)
                            .build())
                    .build())
            .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_EVENTUAL)
            .setImplicitPreReadEnabled(false)
            .setNamespace("ns")
            .setTable("tbl")
            .build();

    // Act
    Mutation actual = ProtoUtils.toMutation(put, TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.of("p1", 10, "p2", "text1"))
                .clusteringKey(Key.of("c1", "text2", "c2", 20))
                .booleanValue("col1", true)
                .intValue("col2", 10)
                .bigIntValue("col3", 100L)
                .floatValue("col4", 1.23F)
                .doubleValue("col5", 4.56)
                .textValue("col6", "text")
                .blobValue("col7", "blob".getBytes(StandardCharsets.UTF_8))
                .condition(
                    ConditionBuilder.putIf(ConditionBuilder.column("col1").isEqualToBoolean(true))
                        .and(ConditionBuilder.column("col1").isNotEqualToBoolean(false))
                        .and(ConditionBuilder.column("col2").isGreaterThanInt(10))
                        .and(ConditionBuilder.column("col3").isGreaterThanOrEqualToBigInt(100L))
                        .and(ConditionBuilder.column("col4").isLessThanFloat(1.23F))
                        .and(ConditionBuilder.column("col5").isLessThanOrEqualToDouble(4.56))
                        .and(ConditionBuilder.column("col6").isNullText())
                        .and(ConditionBuilder.column("col7").isNotNullBlob())
                        .build())
                .consistency(Consistency.EVENTUAL)
                .disableImplicitPreRead()
                .build());
  }

  @Test
  public void toMutation_PutWithNullValuesGiven_ShouldConvertProperly() {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.of("p1", 10, "p2", "text1"))
            .clusteringKey(Key.of("c1", "text2", "c2", 20))
            .booleanValue("col1", null)
            .intValue("col2", null)
            .bigIntValue("col3", null)
            .floatValue("col4", null)
            .doubleValue("col5", null)
            .textValue("col6", null)
            .blobValue("col7", (byte[]) null)
            .condition(ConditionBuilder.putIfNotExists())
            .consistency(Consistency.EVENTUAL)
            .build();

    // Act
    com.scalar.db.rpc.Mutation actual = ProtoUtils.toMutation(put);

    // Assert
    assertThat(actual)
        .isEqualTo(
            com.scalar.db.rpc.Mutation.newBuilder()
                .setType(com.scalar.db.rpc.Mutation.Type.PUT)
                .setPartitionKey(
                    com.scalar.db.rpc.Key.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p1")
                                .setIntValue(10)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p2")
                                .setTextValue("text1")
                                .build())
                        .build())
                .setClusteringKey(
                    com.scalar.db.rpc.Key.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c1")
                                .setTextValue("text2")
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c2")
                                .setIntValue(20)
                                .build())
                        .build())
                .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col1").build())
                .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col2").build())
                .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col3").build())
                .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col4").build())
                .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col5").build())
                .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col6").build())
                .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col7").build())
                .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_EVENTUAL)
                .setCondition(
                    MutateCondition.newBuilder()
                        .setType(com.scalar.db.rpc.MutateCondition.Type.PUT_IF_NOT_EXISTS))
                .setNamespace("ns")
                .setTable("tbl")
                .setImplicitPreReadEnabled(true)
                .build());
  }

  @Test
  public void toMutation_ProtoPutWithNullValuesGiven_ShouldConvertProperly() {
    // Arrange
    com.scalar.db.rpc.Mutation put =
        com.scalar.db.rpc.Mutation.newBuilder()
            .setType(com.scalar.db.rpc.Mutation.Type.PUT)
            .setPartitionKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder().setName("p1").setIntValue(10).build())
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder()
                            .setName("p2")
                            .setTextValue("text1")
                            .build())
                    .build())
            .setClusteringKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder()
                            .setName("c1")
                            .setTextValue("text2")
                            .build())
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder().setName("c2").setIntValue(20).build())
                    .build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col1").build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col2").build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col3").build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col4").build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col5").build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col6").build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col7").build())
            .setCondition(
                MutateCondition.newBuilder()
                    .setType(com.scalar.db.rpc.MutateCondition.Type.PUT_IF_NOT_EXISTS))
            .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_EVENTUAL)
            .setImplicitPreReadEnabled(true)
            .setNamespace("ns")
            .setTable("tbl")
            .build();

    // Act
    Mutation actual = ProtoUtils.toMutation(put, TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.of("p1", 10, "p2", "text1"))
                .clusteringKey(Key.of("c1", "text2", "c2", 20))
                .booleanValue("col1", null)
                .intValue("col2", null)
                .bigIntValue("col3", null)
                .floatValue("col4", null)
                .doubleValue("col5", null)
                .textValue("col6", null)
                .blobValue("col7", (byte[]) null)
                .condition(ConditionBuilder.putIfNotExists())
                .consistency(Consistency.EVENTUAL)
                .implicitRreReadEnabled(true)
                .build());
  }

  @Test
  public void toMutation_ProtoPutFromOldClientGiven_ShouldConvertProperly() {
    // Arrange
    com.scalar.db.rpc.Mutation put =
        com.scalar.db.rpc.Mutation.newBuilder()
            .setType(com.scalar.db.rpc.Mutation.Type.PUT)
            .setPartitionKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addValue(Value.newBuilder().setName("p1").setIntValue(10).build())
                    .addValue(
                        Value.newBuilder()
                            .setName("p2")
                            .setTextValue(TextValue.newBuilder().setValue("text1").build())
                            .build())
                    .build())
            .setClusteringKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addValue(
                        Value.newBuilder()
                            .setName("c1")
                            .setTextValue(TextValue.newBuilder().setValue("text2").build())
                            .build())
                    .addValue(Value.newBuilder().setName("c2").setIntValue(20).build())
                    .build())
            .addValue(Value.newBuilder().setName("col1").setBooleanValue(true).build())
            .addValue(Value.newBuilder().setName("col2").setIntValue(10).build())
            .addValue(Value.newBuilder().setName("col3").setBigintValue(100L).build())
            .addValue(Value.newBuilder().setName("col4").setFloatValue(1.23F).build())
            .addValue(Value.newBuilder().setName("col5").setDoubleValue(4.56).build())
            .addValue(
                Value.newBuilder()
                    .setName("col6")
                    .setTextValue(TextValue.newBuilder().setValue("text").build())
                    .build())
            .addValue(
                Value.newBuilder()
                    .setName("col7")
                    .setBlobValue(
                        BlobValue.newBuilder()
                            .setValue(ByteString.copyFrom("blob".getBytes(StandardCharsets.UTF_8)))
                            .build())
                    .build())
            .setCondition(
                MutateCondition.newBuilder()
                    .setType(com.scalar.db.rpc.MutateCondition.Type.PUT_IF)
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col1")
                            .setValue(Value.newBuilder().setBooleanValue(true).build())
                            .setOperator(Operator.EQ)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col1")
                            .setValue(Value.newBuilder().setBooleanValue(false).build())
                            .setOperator(Operator.NE)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col2")
                            .setValue(Value.newBuilder().setIntValue(10).build())
                            .setOperator(Operator.GT)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col3")
                            .setValue(Value.newBuilder().setBigintValue(100L).build())
                            .setOperator(Operator.GTE)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col4")
                            .setValue(Value.newBuilder().setFloatValue(1.23F).build())
                            .setOperator(Operator.LT)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col5")
                            .setValue(Value.newBuilder().setDoubleValue(4.56).build())
                            .setOperator(Operator.LTE)
                            .build())
                    .build())
            .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_EVENTUAL)
            .setImplicitPreReadEnabled(true)
            .setNamespace("ns")
            .setTable("tbl")
            .build();

    // Act
    Mutation actual = ProtoUtils.toMutation(put, TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.of("p1", 10, "p2", "text1"))
                .clusteringKey(Key.of("c1", "text2", "c2", 20))
                .booleanValue("col1", true)
                .intValue("col2", 10)
                .bigIntValue("col3", 100L)
                .floatValue("col4", 1.23F)
                .doubleValue("col5", 4.56)
                .textValue("col6", "text")
                .blobValue("col7", "blob".getBytes(StandardCharsets.UTF_8))
                .condition(
                    ConditionBuilder.putIf(ConditionBuilder.column("col1").isEqualToBoolean(true))
                        .and(ConditionBuilder.column("col1").isNotEqualToBoolean(false))
                        .and(ConditionBuilder.column("col2").isGreaterThanInt(10))
                        .and(ConditionBuilder.column("col3").isGreaterThanOrEqualToBigInt(100L))
                        .and(ConditionBuilder.column("col4").isLessThanFloat(1.23F))
                        .and(ConditionBuilder.column("col5").isLessThanOrEqualToDouble(4.56))
                        .build())
                .consistency(Consistency.EVENTUAL)
                .implicitRreReadEnabled(true)
                .build());
  }

  @Test
  public void toMutation_ProtoPutWithNullValuesFromOldClientGiven_ShouldConvertProperly() {
    // Arrange
    com.scalar.db.rpc.Mutation put =
        com.scalar.db.rpc.Mutation.newBuilder()
            .setType(com.scalar.db.rpc.Mutation.Type.PUT)
            .setPartitionKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addValue(Value.newBuilder().setName("p1").setIntValue(10).build())
                    .addValue(
                        Value.newBuilder()
                            .setName("p2")
                            .setTextValue(TextValue.newBuilder().setValue("text1").build())
                            .build())
                    .build())
            .setClusteringKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addValue(
                        Value.newBuilder()
                            .setName("c1")
                            .setTextValue(TextValue.newBuilder().setValue("text2").build())
                            .build())
                    .addValue(Value.newBuilder().setName("c2").setIntValue(20).build())
                    .build())
            .addValue(Value.newBuilder().setName("col1").setBooleanValue(true).build())
            .addValue(Value.newBuilder().setName("col2").setIntValue(10).build())
            .addValue(Value.newBuilder().setName("col3").setBigintValue(100L).build())
            .addValue(Value.newBuilder().setName("col4").setFloatValue(1.23F).build())
            .addValue(Value.newBuilder().setName("col5").setDoubleValue(4.56).build())
            .addValue(
                Value.newBuilder()
                    .setName("col6")
                    .setTextValue(TextValue.getDefaultInstance())
                    .build())
            .addValue(
                Value.newBuilder()
                    .setName("col7")
                    .setBlobValue(BlobValue.getDefaultInstance())
                    .build())
            .setCondition(
                MutateCondition.newBuilder()
                    .setType(MutateCondition.Type.PUT_IF)
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col1")
                            .setValue(Value.newBuilder().setBooleanValue(true).build())
                            .setOperator(Operator.EQ)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col1")
                            .setValue(Value.newBuilder().setBooleanValue(false).build())
                            .setOperator(Operator.NE)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col2")
                            .setValue(Value.newBuilder().setIntValue(10).build())
                            .setOperator(Operator.GT)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col3")
                            .setValue(Value.newBuilder().setBigintValue(100L).build())
                            .setOperator(Operator.GTE)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col4")
                            .setValue(Value.newBuilder().setFloatValue(1.23F).build())
                            .setOperator(Operator.LT)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col5")
                            .setValue(Value.newBuilder().setDoubleValue(4.56).build())
                            .setOperator(Operator.LTE)
                            .build())
                    .build())
            .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_EVENTUAL)
            .setImplicitPreReadEnabled(true)
            .setNamespace("ns")
            .setTable("tbl")
            .build();

    // Act
    Mutation actual = ProtoUtils.toMutation(put, TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            Put.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.of("p1", 10, "p2", "text1"))
                .clusteringKey(Key.of("c1", "text2", "c2", 20))
                .booleanValue("col1", true)
                .intValue("col2", 10)
                .bigIntValue("col3", 100L)
                .floatValue("col4", 1.23F)
                .doubleValue("col5", 4.56)
                .textValue("col6", null)
                .blobValue("col7", (byte[]) null)
                .condition(
                    ConditionBuilder.putIf(ConditionBuilder.column("col1").isEqualToBoolean(true))
                        .and(ConditionBuilder.column("col1").isNotEqualToBoolean(false))
                        .and(ConditionBuilder.column("col2").isGreaterThanInt(10))
                        .and(ConditionBuilder.column("col3").isGreaterThanOrEqualToBigInt(100L))
                        .and(ConditionBuilder.column("col4").isLessThanFloat(1.23F))
                        .and(ConditionBuilder.column("col5").isLessThanOrEqualToDouble(4.56))
                        .build())
                .consistency(Consistency.EVENTUAL)
                .implicitRreReadEnabled(true)
                .build());
  }

  @Test
  public void toMutation_DeleteGiven_ShouldConvertProperly() {
    // Arrange
    Delete delete =
        Delete.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.of("p1", 10, "p2", "text1"))
            .clusteringKey(Key.of("c1", "text2", "c2", 20))
            .condition(
                ConditionBuilder.deleteIf(ConditionBuilder.column("col1").isEqualToBoolean(true))
                    .and(ConditionBuilder.column("col1").isNotEqualToBoolean(false))
                    .and(ConditionBuilder.column("col2").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("col3").isGreaterThanOrEqualToBigInt(100L))
                    .and(ConditionBuilder.column("col4").isLessThanFloat(1.23F))
                    .and(ConditionBuilder.column("col5").isLessThanOrEqualToDouble(4.56))
                    .and(ConditionBuilder.column("col6").isNullText())
                    .and(ConditionBuilder.column("col7").isNotNullBlob())
                    .build())
            .consistency(Consistency.EVENTUAL)
            .build();

    // Act
    com.scalar.db.rpc.Mutation actual = ProtoUtils.toMutation(delete);

    // Assert
    assertThat(actual)
        .isEqualTo(
            com.scalar.db.rpc.Mutation.newBuilder()
                .setType(com.scalar.db.rpc.Mutation.Type.DELETE)
                .setPartitionKey(
                    com.scalar.db.rpc.Key.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p1")
                                .setIntValue(10)
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("p2")
                                .setTextValue("text1")
                                .build())
                        .build())
                .setClusteringKey(
                    com.scalar.db.rpc.Key.newBuilder()
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c1")
                                .setTextValue("text2")
                                .build())
                        .addColumns(
                            com.scalar.db.rpc.Column.newBuilder()
                                .setName("c2")
                                .setIntValue(20)
                                .build())
                        .build())
                .setCondition(
                    MutateCondition.newBuilder()
                        .setType(com.scalar.db.rpc.MutateCondition.Type.DELETE_IF)
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col1")
                                        .setBooleanValue(true)
                                        .build())
                                .setOperator(Operator.EQ)
                                .build())
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col1")
                                        .setBooleanValue(false)
                                        .build())
                                .setOperator(Operator.NE)
                                .build())
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col2")
                                        .setIntValue(10)
                                        .build())
                                .setOperator(Operator.GT)
                                .build())
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col3")
                                        .setBigintValue(100L)
                                        .build())
                                .setOperator(Operator.GTE)
                                .build())
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col4")
                                        .setFloatValue(1.23F)
                                        .build())
                                .setOperator(Operator.LT)
                                .build())
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder()
                                        .setName("col5")
                                        .setDoubleValue(4.56)
                                        .build())
                                .setOperator(Operator.LTE)
                                .build())
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder().setName("col6").build())
                                .setOperator(Operator.IS_NULL)
                                .build())
                        .addExpressions(
                            ConditionalExpression.newBuilder()
                                .setColumn(
                                    com.scalar.db.rpc.Column.newBuilder().setName("col7").build())
                                .setOperator(Operator.IS_NOT_NULL)
                                .build())
                        .build())
                .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_EVENTUAL)
                .setNamespace("ns")
                .setTable("tbl")
                .build());
  }

  @Test
  public void toMutation_ProtoDeleteGiven_ShouldConvertProperly() {
    // Arrange
    com.scalar.db.rpc.Mutation delete =
        com.scalar.db.rpc.Mutation.newBuilder()
            .setType(com.scalar.db.rpc.Mutation.Type.DELETE)
            .setPartitionKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder().setName("p1").setIntValue(10).build())
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder()
                            .setName("p2")
                            .setTextValue("text1")
                            .build())
                    .build())
            .setClusteringKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder()
                            .setName("c1")
                            .setTextValue("text2")
                            .build())
                    .addColumns(
                        com.scalar.db.rpc.Column.newBuilder().setName("c2").setIntValue(20).build())
                    .build())
            .setCondition(
                MutateCondition.newBuilder()
                    .setType(com.scalar.db.rpc.MutateCondition.Type.DELETE_IF)
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder()
                                    .setName("col1")
                                    .setBooleanValue(true)
                                    .build())
                            .setOperator(Operator.EQ)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder()
                                    .setName("col1")
                                    .setBooleanValue(false)
                                    .build())
                            .setOperator(Operator.NE)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder()
                                    .setName("col2")
                                    .setIntValue(10)
                                    .build())
                            .setOperator(Operator.GT)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder()
                                    .setName("col3")
                                    .setBigintValue(100L)
                                    .build())
                            .setOperator(Operator.GTE)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder()
                                    .setName("col4")
                                    .setFloatValue(1.23F)
                                    .build())
                            .setOperator(Operator.LT)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder()
                                    .setName("col5")
                                    .setDoubleValue(4.56)
                                    .build())
                            .setOperator(Operator.LTE)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder().setName("col6").build())
                            .setOperator(Operator.IS_NULL)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setColumn(
                                com.scalar.db.rpc.Column.newBuilder().setName("col7").build())
                            .setOperator(Operator.IS_NOT_NULL)
                            .build())
                    .build())
            .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_EVENTUAL)
            .setNamespace("ns")
            .setTable("tbl")
            .build();

    // Act
    Mutation actual = ProtoUtils.toMutation(delete, TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            Delete.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.of("p1", 10, "p2", "text1"))
                .clusteringKey(Key.of("c1", "text2", "c2", 20))
                .condition(
                    ConditionBuilder.deleteIf(
                            ConditionBuilder.column("col1").isEqualToBoolean(true))
                        .and(ConditionBuilder.column("col1").isNotEqualToBoolean(false))
                        .and(ConditionBuilder.column("col2").isGreaterThanInt(10))
                        .and(ConditionBuilder.column("col3").isGreaterThanOrEqualToBigInt(100L))
                        .and(ConditionBuilder.column("col4").isLessThanFloat(1.23F))
                        .and(ConditionBuilder.column("col5").isLessThanOrEqualToDouble(4.56))
                        .and(ConditionBuilder.column("col6").isNullText())
                        .and(ConditionBuilder.column("col7").isNotNullBlob())
                        .build())
                .consistency(Consistency.EVENTUAL)
                .build());
  }

  @Test
  public void toMutation_ProtoDeleteFromOldClientGiven_ShouldConvertProperly() {
    // Arrange
    com.scalar.db.rpc.Mutation delete =
        com.scalar.db.rpc.Mutation.newBuilder()
            .setType(com.scalar.db.rpc.Mutation.Type.DELETE)
            .setPartitionKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addValue(Value.newBuilder().setName("p1").setIntValue(10).build())
                    .addValue(
                        Value.newBuilder()
                            .setName("p2")
                            .setTextValue(TextValue.newBuilder().setValue("text1").build())
                            .build())
                    .build())
            .setClusteringKey(
                com.scalar.db.rpc.Key.newBuilder()
                    .addValue(
                        Value.newBuilder()
                            .setName("c1")
                            .setTextValue(TextValue.newBuilder().setValue("text2").build())
                            .build())
                    .addValue(Value.newBuilder().setName("c2").setIntValue(20).build())
                    .build())
            .setCondition(
                MutateCondition.newBuilder()
                    .setType(com.scalar.db.rpc.MutateCondition.Type.DELETE_IF)
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col1")
                            .setValue(Value.newBuilder().setBooleanValue(true).build())
                            .setOperator(Operator.EQ)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col1")
                            .setValue(Value.newBuilder().setBooleanValue(false).build())
                            .setOperator(Operator.NE)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col2")
                            .setValue(Value.newBuilder().setIntValue(10).build())
                            .setOperator(Operator.GT)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col3")
                            .setValue(Value.newBuilder().setBigintValue(100L).build())
                            .setOperator(Operator.GTE)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col4")
                            .setValue(Value.newBuilder().setFloatValue(1.23F).build())
                            .setOperator(Operator.LT)
                            .build())
                    .addExpressions(
                        ConditionalExpression.newBuilder()
                            .setName("col5")
                            .setValue(Value.newBuilder().setDoubleValue(4.56).build())
                            .setOperator(Operator.LTE)
                            .build())
                    .build())
            .setConsistency(com.scalar.db.rpc.Consistency.CONSISTENCY_EVENTUAL)
            .setNamespace("ns")
            .setTable("tbl")
            .build();

    // Act
    Mutation actual = ProtoUtils.toMutation(delete, TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            Delete.newBuilder()
                .namespace("ns")
                .table("tbl")
                .partitionKey(Key.of("p1", 10, "p2", "text1"))
                .clusteringKey(Key.of("c1", "text2", "c2", 20))
                .condition(
                    ConditionBuilder.deleteIf(
                            ConditionBuilder.column("col1").isEqualToBoolean(true))
                        .and(ConditionBuilder.column("col1").isNotEqualToBoolean(false))
                        .and(ConditionBuilder.column("col2").isGreaterThanInt(10))
                        .and(ConditionBuilder.column("col3").isGreaterThanOrEqualToBigInt(100L))
                        .and(ConditionBuilder.column("col4").isLessThanFloat(1.23F))
                        .and(ConditionBuilder.column("col5").isLessThanOrEqualToDouble(4.56))
                        .build())
                .consistency(Consistency.EVENTUAL)
                .build());
  }

  @Test
  public void toResult_ResultGiven_ShouldConvertProperly() {
    // Arrange
    Result result =
        new ResultImpl(
            ImmutableMap.<String, Column<?>>builder()
                .put("p1", IntColumn.of("p1", 10))
                .put("p2", TextColumn.of("p2", "text1"))
                .put("c1", TextColumn.of("c1", "text2"))
                .put("c2", IntColumn.of("c2", 20))
                .put("col1", BooleanColumn.of("col1", true))
                .put("col2", IntColumn.of("col2", 10))
                .put("col3", BigIntColumn.of("col3", 100L))
                .put("col4", FloatColumn.of("col4", 1.23F))
                .put("col5", DoubleColumn.of("col5", 4.56))
                .put("col6", TextColumn.of("col6", "text"))
                .put("col7", BlobColumn.of("col7", "blob".getBytes(StandardCharsets.UTF_8)))
                .build(),
            TABLE_METADATA);

    // Act
    com.scalar.db.rpc.Result actual = ProtoUtils.toResult(result);

    // Assert
    assertThat(actual)
        .isEqualTo(
            com.scalar.db.rpc.Result.newBuilder()
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder().setName("p1").setIntValue(10).build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("p2")
                        .setTextValue("text1")
                        .build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("c1")
                        .setTextValue("text2")
                        .build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder().setName("c2").setIntValue(20).build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("col1")
                        .setBooleanValue(true)
                        .build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder().setName("col2").setIntValue(10).build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("col3")
                        .setBigintValue(100L)
                        .build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("col4")
                        .setFloatValue(1.23F)
                        .build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("col5")
                        .setDoubleValue(4.56)
                        .build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("col6")
                        .setTextValue("text")
                        .build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("col7")
                        .setBlobValue(ByteString.copyFrom("blob".getBytes(StandardCharsets.UTF_8)))
                        .build())
                .build());
  }

  @Test
  public void toResult_ProtoResultGiven_ShouldConvertProperly() {
    // Arrange
    com.scalar.db.rpc.Result result =
        com.scalar.db.rpc.Result.newBuilder()
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("p1").setIntValue(10).build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("p2").setTextValue("text1").build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("c1").setTextValue("text2").build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("c2").setIntValue(20).build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("col1").setBooleanValue(true).build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("col2").setIntValue(10).build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("col3").setBigintValue(100L).build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("col4").setFloatValue(1.23F).build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("col5").setDoubleValue(4.56).build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("col6").setTextValue("text").build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder()
                    .setName("col7")
                    .setBlobValue(ByteString.copyFrom("blob".getBytes(StandardCharsets.UTF_8)))
                    .build())
            .build();

    // Act
    Result actual = ProtoUtils.toResult(result, TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            new ResultImpl(
                ImmutableMap.<String, Column<?>>builder()
                    .put("p1", IntColumn.of("p1", 10))
                    .put("p2", TextColumn.of("p2", "text1"))
                    .put("c1", TextColumn.of("c1", "text2"))
                    .put("c2", IntColumn.of("c2", 20))
                    .put("col1", BooleanColumn.of("col1", true))
                    .put("col2", IntColumn.of("col2", 10))
                    .put("col3", BigIntColumn.of("col3", 100L))
                    .put("col4", FloatColumn.of("col4", 1.23F))
                    .put("col5", DoubleColumn.of("col5", 4.56))
                    .put("col6", TextColumn.of("col6", "text"))
                    .put("col7", BlobColumn.of("col7", "blob".getBytes(StandardCharsets.UTF_8)))
                    .build(),
                TABLE_METADATA));
  }

  @Test
  public void toResult_ResultWithNullValuesGiven_ShouldConvertProperly() {
    // Arrange
    Result result =
        new ResultImpl(
            ImmutableMap.<String, Column<?>>builder()
                .put("p1", IntColumn.of("p1", 10))
                .put("p2", TextColumn.of("p2", "text1"))
                .put("c1", TextColumn.of("c1", "text2"))
                .put("c2", IntColumn.of("c2", 20))
                .put("col1", BooleanColumn.ofNull("col1"))
                .put("col2", IntColumn.ofNull("col2"))
                .put("col3", BigIntColumn.ofNull("col3"))
                .put("col4", FloatColumn.ofNull("col4"))
                .put("col5", DoubleColumn.ofNull("col5"))
                .put("col6", TextColumn.ofNull("col6"))
                .put("col7", BlobColumn.ofNull("col7"))
                .build(),
            TABLE_METADATA);

    // Act
    com.scalar.db.rpc.Result actual = ProtoUtils.toResult(result);

    // Assert
    assertThat(actual)
        .isEqualTo(
            com.scalar.db.rpc.Result.newBuilder()
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder().setName("p1").setIntValue(10).build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("p2")
                        .setTextValue("text1")
                        .build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder()
                        .setName("c1")
                        .setTextValue("text2")
                        .build())
                .addColumns(
                    com.scalar.db.rpc.Column.newBuilder().setName("c2").setIntValue(20).build())
                .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col1").build())
                .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col2").build())
                .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col3").build())
                .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col4").build())
                .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col5").build())
                .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col6").build())
                .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col7").build())
                .build());
  }

  @Test
  public void toResult_ProtoResultWithNullValuesGiven_ShouldConvertProperly() {
    // Arrange
    com.scalar.db.rpc.Result result =
        com.scalar.db.rpc.Result.newBuilder()
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("p1").setIntValue(10).build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("p2").setTextValue("text1").build())
            .addColumns(
                com.scalar.db.rpc.Column.newBuilder().setName("c1").setTextValue("text2").build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("c2").setIntValue(20).build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col1").build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col2").build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col3").build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col4").build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col5").build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col6").build())
            .addColumns(com.scalar.db.rpc.Column.newBuilder().setName("col7").build())
            .build();

    // Act
    Result actual = ProtoUtils.toResult(result, TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            new ResultImpl(
                ImmutableMap.<String, Column<?>>builder()
                    .put("p1", IntColumn.of("p1", 10))
                    .put("p2", TextColumn.of("p2", "text1"))
                    .put("c1", TextColumn.of("c1", "text2"))
                    .put("c2", IntColumn.of("c2", 20))
                    .put("col1", BooleanColumn.ofNull("col1"))
                    .put("col2", IntColumn.ofNull("col2"))
                    .put("col3", BigIntColumn.ofNull("col3"))
                    .put("col4", FloatColumn.ofNull("col4"))
                    .put("col5", DoubleColumn.ofNull("col5"))
                    .put("col6", TextColumn.ofNull("col6"))
                    .put("col7", BlobColumn.ofNull("col7"))
                    .build(),
                TABLE_METADATA));
  }

  @Test
  public void toResultWithValue_ResultGiven_ShouldConvertProperly() {
    // Arrange
    Result result =
        new ResultImpl(
            ImmutableMap.<String, Column<?>>builder()
                .put("p1", IntColumn.of("p1", 10))
                .put("p2", TextColumn.of("p2", "text1"))
                .put("c1", TextColumn.of("c1", "text2"))
                .put("c2", IntColumn.of("c2", 20))
                .put("col1", BooleanColumn.of("col1", true))
                .put("col2", IntColumn.of("col2", 10))
                .put("col3", BigIntColumn.of("col3", 100L))
                .put("col4", FloatColumn.of("col4", 1.23F))
                .put("col5", DoubleColumn.of("col5", 4.56))
                .put("col6", TextColumn.of("col6", "text"))
                .put("col7", BlobColumn.of("col7", "blob".getBytes(StandardCharsets.UTF_8)))
                .build(),
            TABLE_METADATA);

    // Act
    com.scalar.db.rpc.Result actual = ProtoUtils.toResultWithValue(result);

    // Assert
    assertThat(actual)
        .isEqualTo(
            com.scalar.db.rpc.Result.newBuilder()
                .addValue(Value.newBuilder().setName("p1").setIntValue(10).build())
                .addValue(
                    Value.newBuilder()
                        .setName("p2")
                        .setTextValue(TextValue.newBuilder().setValue("text1").build())
                        .build())
                .addValue(
                    Value.newBuilder()
                        .setName("c1")
                        .setTextValue(TextValue.newBuilder().setValue("text2").build())
                        .build())
                .addValue(Value.newBuilder().setName("c2").setIntValue(20).build())
                .addValue(Value.newBuilder().setName("col1").setBooleanValue(true).build())
                .addValue(Value.newBuilder().setName("col2").setIntValue(10).build())
                .addValue(Value.newBuilder().setName("col3").setBigintValue(100L).build())
                .addValue(Value.newBuilder().setName("col4").setFloatValue(1.23F).build())
                .addValue(Value.newBuilder().setName("col5").setDoubleValue(4.56).build())
                .addValue(
                    Value.newBuilder()
                        .setName("col6")
                        .setTextValue(TextValue.newBuilder().setValue("text").build())
                        .build())
                .addValue(
                    Value.newBuilder()
                        .setName("col7")
                        .setBlobValue(
                            BlobValue.newBuilder()
                                .setValue(
                                    ByteString.copyFrom("blob".getBytes(StandardCharsets.UTF_8)))
                                .build())
                        .build())
                .build());
  }

  @Test
  public void toResultWithValue_ResultWithNullValuesGiven_ShouldConvertProperly() {
    // Arrange
    Result result =
        new ResultImpl(
            ImmutableMap.<String, Column<?>>builder()
                .put("p1", IntColumn.of("p1", 10))
                .put("p2", TextColumn.of("p2", "text1"))
                .put("c1", TextColumn.of("c1", "text2"))
                .put("c2", IntColumn.of("c2", 20))
                .put("col1", BooleanColumn.ofNull("col1"))
                .put("col2", IntColumn.ofNull("col2"))
                .put("col3", BigIntColumn.ofNull("col3"))
                .put("col4", FloatColumn.ofNull("col4"))
                .put("col5", DoubleColumn.ofNull("col5"))
                .put("col6", TextColumn.ofNull("col6"))
                .put("col7", BlobColumn.ofNull("col7"))
                .build(),
            TABLE_METADATA);

    // Act
    com.scalar.db.rpc.Result actual = ProtoUtils.toResultWithValue(result);

    // Assert
    assertThat(actual)
        .isEqualTo(
            com.scalar.db.rpc.Result.newBuilder()
                .addValue(Value.newBuilder().setName("p1").setIntValue(10).build())
                .addValue(
                    Value.newBuilder()
                        .setName("p2")
                        .setTextValue(TextValue.newBuilder().setValue("text1").build())
                        .build())
                .addValue(
                    Value.newBuilder()
                        .setName("c1")
                        .setTextValue(TextValue.newBuilder().setValue("text2").build())
                        .build())
                .addValue(Value.newBuilder().setName("c2").setIntValue(20).build())
                .addValue(Value.newBuilder().setName("col1").setBooleanValue(false).build())
                .addValue(Value.newBuilder().setName("col2").setIntValue(0).build())
                .addValue(Value.newBuilder().setName("col3").setBigintValue(0L).build())
                .addValue(Value.newBuilder().setName("col4").setFloatValue(0.0F).build())
                .addValue(Value.newBuilder().setName("col5").setDoubleValue(0.0).build())
                .addValue(
                    Value.newBuilder()
                        .setName("col6")
                        .setTextValue(TextValue.getDefaultInstance())
                        .build())
                .addValue(
                    Value.newBuilder()
                        .setName("col7")
                        .setBlobValue(BlobValue.getDefaultInstance())
                        .build())
                .build());
  }

  @Test
  public void toTableMetadata_TableMetadataGiven_ShouldConvertProperly() {
    // Arrange

    // Act
    com.scalar.db.rpc.TableMetadata actual = ProtoUtils.toTableMetadata(TABLE_METADATA);

    // Assert
    assertThat(actual)
        .isEqualTo(
            com.scalar.db.rpc.TableMetadata.newBuilder()
                .putColumns("p1", com.scalar.db.rpc.DataType.DATA_TYPE_INT)
                .putColumns("p2", com.scalar.db.rpc.DataType.DATA_TYPE_TEXT)
                .putColumns("c1", com.scalar.db.rpc.DataType.DATA_TYPE_TEXT)
                .putColumns("c2", com.scalar.db.rpc.DataType.DATA_TYPE_INT)
                .putColumns("col1", com.scalar.db.rpc.DataType.DATA_TYPE_BOOLEAN)
                .putColumns("col2", com.scalar.db.rpc.DataType.DATA_TYPE_INT)
                .putColumns("col3", com.scalar.db.rpc.DataType.DATA_TYPE_BIGINT)
                .putColumns("col4", com.scalar.db.rpc.DataType.DATA_TYPE_FLOAT)
                .putColumns("col5", com.scalar.db.rpc.DataType.DATA_TYPE_DOUBLE)
                .putColumns("col6", com.scalar.db.rpc.DataType.DATA_TYPE_TEXT)
                .putColumns("col7", com.scalar.db.rpc.DataType.DATA_TYPE_BLOB)
                .addPartitionKeyNames("p1")
                .addPartitionKeyNames("p2")
                .addClusteringKeyNames("c1")
                .addClusteringKeyNames("c2")
                .putClusteringOrders("c1", Order.ORDER_DESC)
                .putClusteringOrders("c2", Order.ORDER_ASC)
                .addSecondaryIndexNames("col4")
                .addSecondaryIndexNames("col2")
                .build());
  }

  @Test
  public void toTableMetadata_ProtoTableMetadataGiven_ShouldConvertProperly() {
    // Arrange

    // Act
    TableMetadata actual =
        ProtoUtils.toTableMetadata(
            com.scalar.db.rpc.TableMetadata.newBuilder()
                .putColumns("p1", com.scalar.db.rpc.DataType.DATA_TYPE_INT)
                .putColumns("p2", com.scalar.db.rpc.DataType.DATA_TYPE_TEXT)
                .putColumns("c1", com.scalar.db.rpc.DataType.DATA_TYPE_TEXT)
                .putColumns("c2", com.scalar.db.rpc.DataType.DATA_TYPE_INT)
                .putColumns("col1", com.scalar.db.rpc.DataType.DATA_TYPE_BOOLEAN)
                .putColumns("col2", com.scalar.db.rpc.DataType.DATA_TYPE_INT)
                .putColumns("col3", com.scalar.db.rpc.DataType.DATA_TYPE_BIGINT)
                .putColumns("col4", com.scalar.db.rpc.DataType.DATA_TYPE_FLOAT)
                .putColumns("col5", com.scalar.db.rpc.DataType.DATA_TYPE_DOUBLE)
                .putColumns("col6", com.scalar.db.rpc.DataType.DATA_TYPE_TEXT)
                .putColumns("col7", com.scalar.db.rpc.DataType.DATA_TYPE_BLOB)
                .addPartitionKeyNames("p1")
                .addPartitionKeyNames("p2")
                .addClusteringKeyNames("c1")
                .addClusteringKeyNames("c2")
                .putClusteringOrders("c1", Order.ORDER_DESC)
                .putClusteringOrders("c2", Order.ORDER_ASC)
                .addSecondaryIndexNames("col4")
                .addSecondaryIndexNames("col2")
                .build());

    // Assert
    assertThat(actual).isEqualTo(TABLE_METADATA);
  }
}
