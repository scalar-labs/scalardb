package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.ConditionalExpression.Operator;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class ConditionBuilderTest {

  @Test
  public void putIf_WithIsEqualToConditions_ShouldBuildProperly() {
    // Arrange

    // Act
    PutIf actual =
        ConditionBuilder.putIf(ConditionBuilder.column("col1").isEqualToBoolean(true))
            .and(ConditionBuilder.column("col2").isEqualToInt(123))
            .and(ConditionBuilder.column("col3").isEqualToBigInt(456L))
            .and(ConditionBuilder.column("col4").isEqualToFloat(1.23F))
            .and(ConditionBuilder.column("col5").isEqualToDouble(4.56))
            .and(ConditionBuilder.column("col6").isEqualToText("text"))
            .and(
                ConditionBuilder.column("col7")
                    .isEqualToBlob("blob1".getBytes(StandardCharsets.UTF_8)))
            .and(
                ConditionBuilder.column("col8")
                    .isEqualToBlob(ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8))))
            .build();

    // Assert
    assertThat(actual.getExpressions().size()).isEqualTo(8);
    assertThat(actual.getExpressions().get(0))
        .isEqualTo(new ConditionalExpression("col1", true, Operator.EQ));
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", 123, Operator.EQ));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 456L, Operator.EQ));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 1.23F, Operator.EQ));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 4.56D, Operator.EQ));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", "text", Operator.EQ));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(
            new ConditionalExpression(
                "col7", "blob1".getBytes(StandardCharsets.UTF_8), Operator.EQ));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.EQ));
  }

  @Test
  public void putIf_WithIsNotEqualToConditions_ShouldBuildProperly() {
    // Arrange

    // Act
    PutIf actual =
        ConditionBuilder.putIf(ConditionBuilder.column("col1").isNotEqualToBoolean(true))
            .and(ConditionBuilder.column("col2").isNotEqualToInt(123))
            .and(ConditionBuilder.column("col3").isNotEqualToBigInt(456L))
            .and(ConditionBuilder.column("col4").isNotEqualToFloat(1.23F))
            .and(ConditionBuilder.column("col5").isNotEqualToDouble(4.56))
            .and(ConditionBuilder.column("col6").isNotEqualToText("text"))
            .and(
                ConditionBuilder.column("col7")
                    .isNotEqualToBlob("blob1".getBytes(StandardCharsets.UTF_8)))
            .and(
                ConditionBuilder.column("col8")
                    .isNotEqualToBlob(ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8))))
            .build();

    // Assert
    assertThat(actual.getExpressions().size()).isEqualTo(8);
    assertThat(actual.getExpressions().get(0))
        .isEqualTo(new ConditionalExpression("col1", true, Operator.NE));
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", 123, Operator.NE));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 456L, Operator.NE));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 1.23F, Operator.NE));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 4.56D, Operator.NE));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", "text", Operator.NE));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(
            new ConditionalExpression(
                "col7", "blob1".getBytes(StandardCharsets.UTF_8), Operator.NE));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.NE));
  }

  @Test
  public void putIf_WithIsGreaterThanConditions_ShouldBuildProperly() {
    // Arrange

    // Act
    PutIf actual =
        ConditionBuilder.putIf(ConditionBuilder.column("col1").isGreaterThanBoolean(true))
            .and(ConditionBuilder.column("col2").isGreaterThanInt(123))
            .and(ConditionBuilder.column("col3").isGreaterThanBigInt(456L))
            .and(ConditionBuilder.column("col4").isGreaterThanFloat(1.23F))
            .and(ConditionBuilder.column("col5").isGreaterThanDouble(4.56))
            .and(ConditionBuilder.column("col6").isGreaterThanText("text"))
            .and(
                ConditionBuilder.column("col7")
                    .isGreaterThanBlob("blob1".getBytes(StandardCharsets.UTF_8)))
            .and(
                ConditionBuilder.column("col8")
                    .isGreaterThanBlob(ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8))))
            .build();

    // Assert
    assertThat(actual.getExpressions().size()).isEqualTo(8);
    assertThat(actual.getExpressions().get(0))
        .isEqualTo(new ConditionalExpression("col1", true, Operator.GT));
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", 123, Operator.GT));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 456L, Operator.GT));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 1.23F, Operator.GT));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 4.56D, Operator.GT));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", "text", Operator.GT));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(
            new ConditionalExpression(
                "col7", "blob1".getBytes(StandardCharsets.UTF_8), Operator.GT));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.GT));
  }

  @Test
  public void putIf_WithIsGreaterThanOrEqualToConditions_ShouldBuildProperly() {
    // Arrange

    // Act
    PutIf actual =
        ConditionBuilder.putIf(ConditionBuilder.column("col1").isGreaterThanOrEqualToBoolean(true))
            .and(ConditionBuilder.column("col2").isGreaterThanOrEqualToInt(123))
            .and(ConditionBuilder.column("col3").isGreaterThanOrEqualToBigInt(456L))
            .and(ConditionBuilder.column("col4").isGreaterThanOrEqualToFloat(1.23F))
            .and(ConditionBuilder.column("col5").isGreaterThanOrEqualToDouble(4.56))
            .and(ConditionBuilder.column("col6").isGreaterThanOrEqualToText("text"))
            .and(
                ConditionBuilder.column("col7")
                    .isGreaterThanOrEqualToBlob("blob1".getBytes(StandardCharsets.UTF_8)))
            .and(
                ConditionBuilder.column("col8")
                    .isGreaterThanOrEqualToBlob(
                        ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8))))
            .build();

    // Assert
    assertThat(actual.getExpressions().size()).isEqualTo(8);
    assertThat(actual.getExpressions().get(0))
        .isEqualTo(new ConditionalExpression("col1", true, Operator.GTE));
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", 123, Operator.GTE));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 456L, Operator.GTE));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 1.23F, Operator.GTE));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 4.56D, Operator.GTE));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", "text", Operator.GTE));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(
            new ConditionalExpression(
                "col7", "blob1".getBytes(StandardCharsets.UTF_8), Operator.GTE));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.GTE));
  }

  @Test
  public void putIf_WithIsLessThanConditions_ShouldBuildProperly() {
    // Arrange

    // Act
    PutIf actual =
        ConditionBuilder.putIf(ConditionBuilder.column("col1").isLessThanBoolean(true))
            .and(ConditionBuilder.column("col2").isLessThanInt(123))
            .and(ConditionBuilder.column("col3").isLessThanBigInt(456L))
            .and(ConditionBuilder.column("col4").isLessThanFloat(1.23F))
            .and(ConditionBuilder.column("col5").isLessThanDouble(4.56))
            .and(ConditionBuilder.column("col6").isLessThanText("text"))
            .and(
                ConditionBuilder.column("col7")
                    .isLessThanBlob("blob1".getBytes(StandardCharsets.UTF_8)))
            .and(
                ConditionBuilder.column("col8")
                    .isLessThanBlob(ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8))))
            .build();

    // Assert
    assertThat(actual.getExpressions().size()).isEqualTo(8);
    assertThat(actual.getExpressions().get(0))
        .isEqualTo(new ConditionalExpression("col1", true, Operator.LT));
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", 123, Operator.LT));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 456L, Operator.LT));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 1.23F, Operator.LT));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 4.56D, Operator.LT));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", "text", Operator.LT));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(
            new ConditionalExpression(
                "col7", "blob1".getBytes(StandardCharsets.UTF_8), Operator.LT));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.LT));
  }

  @Test
  public void putIf_WithIsLessThanOrEqualToConditions_ShouldBuildProperly() {
    // Arrange

    // Act
    PutIf actual =
        ConditionBuilder.putIf(ConditionBuilder.column("col1").isLessThanOrEqualToBoolean(true))
            .and(ConditionBuilder.column("col2").isLessThanOrEqualToInt(123))
            .and(ConditionBuilder.column("col3").isLessThanOrEqualToBigInt(456L))
            .and(ConditionBuilder.column("col4").isLessThanOrEqualToFloat(1.23F))
            .and(ConditionBuilder.column("col5").isLessThanOrEqualToDouble(4.56))
            .and(ConditionBuilder.column("col6").isLessThanOrEqualToText("text"))
            .and(
                ConditionBuilder.column("col7")
                    .isLessThanOrEqualToBlob("blob1".getBytes(StandardCharsets.UTF_8)))
            .and(
                ConditionBuilder.column("col8")
                    .isLessThanOrEqualToBlob(
                        ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8))))
            .build();

    // Assert
    assertThat(actual.getExpressions().size()).isEqualTo(8);
    assertThat(actual.getExpressions().get(0))
        .isEqualTo(new ConditionalExpression("col1", true, Operator.LTE));
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", 123, Operator.LTE));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 456L, Operator.LTE));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 1.23F, Operator.LTE));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 4.56D, Operator.LTE));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", "text", Operator.LTE));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(
            new ConditionalExpression(
                "col7", "blob1".getBytes(StandardCharsets.UTF_8), Operator.LTE));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.LTE));
  }

  @Test
  public void putIfExists_ShouldBuildProperly() {
    // Arrange

    // Act
    PutIfExists actual = ConditionBuilder.putIfExists();

    // Assert
    assertThat(actual.getExpressions()).isEmpty();
  }

  @Test
  public void putIfNotExists_ShouldBuildProperly() {
    // Arrange

    // Act
    PutIfNotExists actual = ConditionBuilder.putIfNotExists();

    // Assert
    assertThat(actual.getExpressions()).isEmpty();
  }

  @Test
  public void deleteIf_WithIsEqualToConditions_ShouldBuildProperly() {
    // Arrange

    // Act
    DeleteIf actual =
        ConditionBuilder.deleteIf(ConditionBuilder.column("col1").isEqualToBoolean(true))
            .and(ConditionBuilder.column("col2").isEqualToInt(123))
            .and(ConditionBuilder.column("col3").isEqualToBigInt(456L))
            .and(ConditionBuilder.column("col4").isEqualToFloat(1.23F))
            .and(ConditionBuilder.column("col5").isEqualToDouble(4.56))
            .and(ConditionBuilder.column("col6").isEqualToText("text"))
            .and(
                ConditionBuilder.column("col7")
                    .isEqualToBlob("blob1".getBytes(StandardCharsets.UTF_8)))
            .and(
                ConditionBuilder.column("col8")
                    .isEqualToBlob(ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8))))
            .build();

    // Assert
    assertThat(actual.getExpressions().size()).isEqualTo(8);
    assertThat(actual.getExpressions().get(0))
        .isEqualTo(new ConditionalExpression("col1", true, Operator.EQ));
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", 123, Operator.EQ));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 456L, Operator.EQ));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 1.23F, Operator.EQ));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 4.56D, Operator.EQ));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", "text", Operator.EQ));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(
            new ConditionalExpression(
                "col7", "blob1".getBytes(StandardCharsets.UTF_8), Operator.EQ));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.EQ));
  }

  @Test
  public void deleteIf_WithIsNotEqualToConditions_ShouldBuildProperly() {
    // Arrange

    // Act
    DeleteIf actual =
        ConditionBuilder.deleteIf(ConditionBuilder.column("col1").isNotEqualToBoolean(true))
            .and(ConditionBuilder.column("col2").isNotEqualToInt(123))
            .and(ConditionBuilder.column("col3").isNotEqualToBigInt(456L))
            .and(ConditionBuilder.column("col4").isNotEqualToFloat(1.23F))
            .and(ConditionBuilder.column("col5").isNotEqualToDouble(4.56))
            .and(ConditionBuilder.column("col6").isNotEqualToText("text"))
            .and(
                ConditionBuilder.column("col7")
                    .isNotEqualToBlob("blob1".getBytes(StandardCharsets.UTF_8)))
            .and(
                ConditionBuilder.column("col8")
                    .isNotEqualToBlob(ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8))))
            .build();

    // Assert
    assertThat(actual.getExpressions().size()).isEqualTo(8);
    assertThat(actual.getExpressions().get(0))
        .isEqualTo(new ConditionalExpression("col1", true, Operator.NE));
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", 123, Operator.NE));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 456L, Operator.NE));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 1.23F, Operator.NE));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 4.56D, Operator.NE));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", "text", Operator.NE));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(
            new ConditionalExpression(
                "col7", "blob1".getBytes(StandardCharsets.UTF_8), Operator.NE));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.NE));
  }

  @Test
  public void deleteIf_WithIsGreaterThanConditions_ShouldBuildProperly() {
    // Arrange

    // Act
    DeleteIf actual =
        ConditionBuilder.deleteIf(ConditionBuilder.column("col1").isGreaterThanBoolean(true))
            .and(ConditionBuilder.column("col2").isGreaterThanInt(123))
            .and(ConditionBuilder.column("col3").isGreaterThanBigInt(456L))
            .and(ConditionBuilder.column("col4").isGreaterThanFloat(1.23F))
            .and(ConditionBuilder.column("col5").isGreaterThanDouble(4.56))
            .and(ConditionBuilder.column("col6").isGreaterThanText("text"))
            .and(
                ConditionBuilder.column("col7")
                    .isGreaterThanBlob("blob1".getBytes(StandardCharsets.UTF_8)))
            .and(
                ConditionBuilder.column("col8")
                    .isGreaterThanBlob(ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8))))
            .build();

    // Assert
    assertThat(actual.getExpressions().size()).isEqualTo(8);
    assertThat(actual.getExpressions().get(0))
        .isEqualTo(new ConditionalExpression("col1", true, Operator.GT));
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", 123, Operator.GT));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 456L, Operator.GT));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 1.23F, Operator.GT));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 4.56D, Operator.GT));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", "text", Operator.GT));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(
            new ConditionalExpression(
                "col7", "blob1".getBytes(StandardCharsets.UTF_8), Operator.GT));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.GT));
  }

  @Test
  public void deleteIf_WithIsGreaterThanOrEqualToConditions_ShouldBuildProperly() {
    // Arrange

    // Act
    DeleteIf actual =
        ConditionBuilder.deleteIf(
                ConditionBuilder.column("col1").isGreaterThanOrEqualToBoolean(true))
            .and(ConditionBuilder.column("col2").isGreaterThanOrEqualToInt(123))
            .and(ConditionBuilder.column("col3").isGreaterThanOrEqualToBigInt(456L))
            .and(ConditionBuilder.column("col4").isGreaterThanOrEqualToFloat(1.23F))
            .and(ConditionBuilder.column("col5").isGreaterThanOrEqualToDouble(4.56))
            .and(ConditionBuilder.column("col6").isGreaterThanOrEqualToText("text"))
            .and(
                ConditionBuilder.column("col7")
                    .isGreaterThanOrEqualToBlob("blob1".getBytes(StandardCharsets.UTF_8)))
            .and(
                ConditionBuilder.column("col8")
                    .isGreaterThanOrEqualToBlob(
                        ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8))))
            .build();

    // Assert
    assertThat(actual.getExpressions().size()).isEqualTo(8);
    assertThat(actual.getExpressions().get(0))
        .isEqualTo(new ConditionalExpression("col1", true, Operator.GTE));
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", 123, Operator.GTE));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 456L, Operator.GTE));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 1.23F, Operator.GTE));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 4.56D, Operator.GTE));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", "text", Operator.GTE));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(
            new ConditionalExpression(
                "col7", "blob1".getBytes(StandardCharsets.UTF_8), Operator.GTE));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.GTE));
  }

  @Test
  public void deleteIf_WithIsLessThanConditions_ShouldBuildProperly() {
    // Arrange

    // Act
    DeleteIf actual =
        ConditionBuilder.deleteIf(ConditionBuilder.column("col1").isLessThanBoolean(true))
            .and(ConditionBuilder.column("col2").isLessThanInt(123))
            .and(ConditionBuilder.column("col3").isLessThanBigInt(456L))
            .and(ConditionBuilder.column("col4").isLessThanFloat(1.23F))
            .and(ConditionBuilder.column("col5").isLessThanDouble(4.56))
            .and(ConditionBuilder.column("col6").isLessThanText("text"))
            .and(
                ConditionBuilder.column("col7")
                    .isLessThanBlob("blob1".getBytes(StandardCharsets.UTF_8)))
            .and(
                ConditionBuilder.column("col8")
                    .isLessThanBlob(ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8))))
            .build();

    // Assert
    assertThat(actual.getExpressions().size()).isEqualTo(8);
    assertThat(actual.getExpressions().get(0))
        .isEqualTo(new ConditionalExpression("col1", true, Operator.LT));
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", 123, Operator.LT));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 456L, Operator.LT));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 1.23F, Operator.LT));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 4.56D, Operator.LT));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", "text", Operator.LT));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(
            new ConditionalExpression(
                "col7", "blob1".getBytes(StandardCharsets.UTF_8), Operator.LT));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.LT));
  }

  @Test
  public void deleteIf_WithIsLessThanOrEqualToConditions_ShouldBuildProperly() {
    // Arrange

    // Act
    DeleteIf actual =
        ConditionBuilder.deleteIf(ConditionBuilder.column("col1").isLessThanOrEqualToBoolean(true))
            .and(ConditionBuilder.column("col2").isLessThanOrEqualToInt(123))
            .and(ConditionBuilder.column("col3").isLessThanOrEqualToBigInt(456L))
            .and(ConditionBuilder.column("col4").isLessThanOrEqualToFloat(1.23F))
            .and(ConditionBuilder.column("col5").isLessThanOrEqualToDouble(4.56))
            .and(ConditionBuilder.column("col6").isLessThanOrEqualToText("text"))
            .and(
                ConditionBuilder.column("col7")
                    .isLessThanOrEqualToBlob("blob1".getBytes(StandardCharsets.UTF_8)))
            .and(
                ConditionBuilder.column("col8")
                    .isLessThanOrEqualToBlob(
                        ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8))))
            .build();

    // Assert
    assertThat(actual.getExpressions().size()).isEqualTo(8);
    assertThat(actual.getExpressions().get(0))
        .isEqualTo(new ConditionalExpression("col1", true, Operator.LTE));
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", 123, Operator.LTE));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 456L, Operator.LTE));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 1.23F, Operator.LTE));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 4.56D, Operator.LTE));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", "text", Operator.LTE));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(
            new ConditionalExpression(
                "col7", "blob1".getBytes(StandardCharsets.UTF_8), Operator.LTE));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.LTE));
  }

  @Test
  public void deleteIfExists_ShouldBuildProperly() {
    // Arrange

    // Act
    DeleteIfExists actual = ConditionBuilder.deleteIfExists();

    // Assert
    assertThat(actual.getExpressions()).isEmpty();
  }
}
