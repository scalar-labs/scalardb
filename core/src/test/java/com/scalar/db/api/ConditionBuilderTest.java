package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.ConditionBuilder.IfBuilder;
import com.scalar.db.api.ConditionalExpression.Operator;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class ConditionBuilderTest {

  @Test
  public void putIf_eqConditionGiven_ShouldBuildProperly() {
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().eqBoolean("col1", true),
        new ConditionalExpression("col1", true, Operator.EQ));
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().eqInt("col1", 789),
        new ConditionalExpression("col1", 789, Operator.EQ));
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().eqBigInt("col1", 789L),
        new ConditionalExpression("col1", 789L, Operator.EQ));
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().eqFloat("col1", 7.89F),
        new ConditionalExpression("col1", 7.89F, Operator.EQ));
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().eqDouble("col1", 7.89D),
        new ConditionalExpression("col1", 7.89D, Operator.EQ));
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().eqText("col1", "text2"),
        new ConditionalExpression("col1", "text2", Operator.EQ));
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().eqBlob("col1", "blob3".getBytes(StandardCharsets.UTF_8)),
        new ConditionalExpression("col1", "blob3".getBytes(StandardCharsets.UTF_8), Operator.EQ));
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf()
            .eqBlob("col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8))),
        new ConditionalExpression(
            "col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8)), Operator.EQ));
  }

  @Test
  public void putIf_neConditionGiven_ShouldBuildProperly() {
    xxxIf_neConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().neBoolean("col1", true),
        new ConditionalExpression("col1", true, Operator.NE));
    xxxIf_neConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().neInt("col1", 789),
        new ConditionalExpression("col1", 789, Operator.NE));
    xxxIf_neConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().neBigInt("col1", 789L),
        new ConditionalExpression("col1", 789L, Operator.NE));
    xxxIf_neConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().neFloat("col1", 7.89F),
        new ConditionalExpression("col1", 7.89F, Operator.NE));
    xxxIf_neConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().neDouble("col1", 7.89D),
        new ConditionalExpression("col1", 7.89D, Operator.NE));
    xxxIf_neConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().neText("col1", "text2"),
        new ConditionalExpression("col1", "text2", Operator.NE));
    xxxIf_neConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().neBlob("col1", "blob3".getBytes(StandardCharsets.UTF_8)),
        new ConditionalExpression("col1", "blob3".getBytes(StandardCharsets.UTF_8), Operator.NE));
    xxxIf_neConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf()
            .neBlob("col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8))),
        new ConditionalExpression(
            "col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8)), Operator.NE));
  }

  @Test
  public void putIf_gtConditionGiven_ShouldBuildProperly() {
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().gtBoolean("col1", true),
        new ConditionalExpression("col1", true, Operator.GT));
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().gtInt("col1", 789),
        new ConditionalExpression("col1", 789, Operator.GT));
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().gtBigInt("col1", 789L),
        new ConditionalExpression("col1", 789L, Operator.GT));
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().gtFloat("col1", 7.89F),
        new ConditionalExpression("col1", 7.89F, Operator.GT));
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().gtDouble("col1", 7.89D),
        new ConditionalExpression("col1", 7.89D, Operator.GT));
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().gtText("col1", "text2"),
        new ConditionalExpression("col1", "text2", Operator.GT));
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().gtBlob("col1", "blob3".getBytes(StandardCharsets.UTF_8)),
        new ConditionalExpression("col1", "blob3".getBytes(StandardCharsets.UTF_8), Operator.GT));
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf()
            .gtBlob("col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8))),
        new ConditionalExpression(
            "col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8)), Operator.GT));
  }

  @Test
  public void putIf_gteConditionGiven_ShouldBuildProperly() {
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().gteBoolean("col1", true),
        new ConditionalExpression("col1", true, Operator.GTE));
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().gteInt("col1", 789),
        new ConditionalExpression("col1", 789, Operator.GTE));
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().gteBigInt("col1", 789L),
        new ConditionalExpression("col1", 789L, Operator.GTE));
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().gteFloat("col1", 7.89F),
        new ConditionalExpression("col1", 7.89F, Operator.GTE));
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().gteDouble("col1", 7.89D),
        new ConditionalExpression("col1", 7.89D, Operator.GTE));
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().gteText("col1", "text2"),
        new ConditionalExpression("col1", "text2", Operator.GTE));
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().gteBlob("col1", "blob3".getBytes(StandardCharsets.UTF_8)),
        new ConditionalExpression("col1", "blob3".getBytes(StandardCharsets.UTF_8), Operator.GTE));
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf()
            .gteBlob("col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8))),
        new ConditionalExpression(
            "col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8)), Operator.GTE));
  }

  @Test
  public void putIf_ltConditionGiven_ShouldBuildProperly() {
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().ltBoolean("col1", true),
        new ConditionalExpression("col1", true, Operator.LT));
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().ltInt("col1", 789),
        new ConditionalExpression("col1", 789, Operator.LT));
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().ltBigInt("col1", 789L),
        new ConditionalExpression("col1", 789L, Operator.LT));
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().ltFloat("col1", 7.89F),
        new ConditionalExpression("col1", 7.89F, Operator.LT));
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().ltDouble("col1", 7.89D),
        new ConditionalExpression("col1", 7.89D, Operator.LT));
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().ltText("col1", "text2"),
        new ConditionalExpression("col1", "text2", Operator.LT));
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().ltBlob("col1", "blob3".getBytes(StandardCharsets.UTF_8)),
        new ConditionalExpression("col1", "blob3".getBytes(StandardCharsets.UTF_8), Operator.LT));
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf()
            .ltBlob("col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8))),
        new ConditionalExpression(
            "col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8)), Operator.LT));
  }

  @Test
  public void putIf_lteConditionGiven_ShouldBuildProperly() {
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().lteBoolean("col1", true),
        new ConditionalExpression("col1", true, Operator.LTE));
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().lteInt("col1", 789),
        new ConditionalExpression("col1", 789, Operator.LTE));
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().lteBigInt("col1", 789L),
        new ConditionalExpression("col1", 789L, Operator.LTE));
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().lteFloat("col1", 7.89F),
        new ConditionalExpression("col1", 7.89F, Operator.LTE));
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().lteDouble("col1", 7.89D),
        new ConditionalExpression("col1", 7.89D, Operator.LTE));
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().lteText("col1", "text2"),
        new ConditionalExpression("col1", "text2", Operator.LTE));
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf().lteBlob("col1", "blob3".getBytes(StandardCharsets.UTF_8)),
        new ConditionalExpression("col1", "blob3".getBytes(StandardCharsets.UTF_8), Operator.LTE));
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        true,
        ConditionBuilder.putIf()
            .lteBlob("col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8))),
        new ConditionalExpression(
            "col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8)), Operator.LTE));
  }

  @Test
  public void putIfExists_ShouldBuildProperly() {
    // Arrange

    // Act
    MutationCondition actual = ConditionBuilder.putIfExists();

    // Assert
    assertThat(actual instanceof PutIfExists).isTrue();
    assertThat(actual.getExpressions()).isEmpty();
  }

  @Test
  public void putIfNotExists_ShouldBuildProperly() {
    // Arrange

    // Act
    MutationCondition actual = ConditionBuilder.putIfNotExists();

    // Assert
    assertThat(actual instanceof PutIfNotExists).isTrue();
    assertThat(actual.getExpressions()).isEmpty();
  }

  @Test
  public void deleteIf_eqConditionGiven_ShouldBuildProperly() {
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().eqBoolean("col1", true),
        new ConditionalExpression("col1", true, Operator.EQ));
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().eqInt("col1", 789),
        new ConditionalExpression("col1", 789, Operator.EQ));
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().eqBigInt("col1", 789L),
        new ConditionalExpression("col1", 789L, Operator.EQ));
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().eqFloat("col1", 7.89F),
        new ConditionalExpression("col1", 7.89F, Operator.EQ));
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().eqDouble("col1", 7.89D),
        new ConditionalExpression("col1", 7.89D, Operator.EQ));
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().eqText("col1", "text2"),
        new ConditionalExpression("col1", "text2", Operator.EQ));
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().eqBlob("col1", "blob3".getBytes(StandardCharsets.UTF_8)),
        new ConditionalExpression("col1", "blob3".getBytes(StandardCharsets.UTF_8), Operator.EQ));
    xxxIf_eqConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf()
            .eqBlob("col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8))),
        new ConditionalExpression(
            "col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8)), Operator.EQ));
  }

  @Test
  public void deleteIf_neConditionGiven_ShouldBuildProperly() {
    xxxIf_neConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().neBoolean("col1", true),
        new ConditionalExpression("col1", true, Operator.NE));
    xxxIf_neConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().neInt("col1", 789),
        new ConditionalExpression("col1", 789, Operator.NE));
    xxxIf_neConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().neBigInt("col1", 789L),
        new ConditionalExpression("col1", 789L, Operator.NE));
    xxxIf_neConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().neFloat("col1", 7.89F),
        new ConditionalExpression("col1", 7.89F, Operator.NE));
    xxxIf_neConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().neDouble("col1", 7.89D),
        new ConditionalExpression("col1", 7.89D, Operator.NE));
    xxxIf_neConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().neText("col1", "text2"),
        new ConditionalExpression("col1", "text2", Operator.NE));
    xxxIf_neConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().neBlob("col1", "blob3".getBytes(StandardCharsets.UTF_8)),
        new ConditionalExpression("col1", "blob3".getBytes(StandardCharsets.UTF_8), Operator.NE));
    xxxIf_neConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf()
            .neBlob("col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8))),
        new ConditionalExpression(
            "col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8)), Operator.NE));
  }

  @Test
  public void deleteIf_gtConditionGiven_ShouldBuildProperly() {
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().gtBoolean("col1", true),
        new ConditionalExpression("col1", true, Operator.GT));
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().gtInt("col1", 789),
        new ConditionalExpression("col1", 789, Operator.GT));
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().gtBigInt("col1", 789L),
        new ConditionalExpression("col1", 789L, Operator.GT));
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().gtFloat("col1", 7.89F),
        new ConditionalExpression("col1", 7.89F, Operator.GT));
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().gtDouble("col1", 7.89D),
        new ConditionalExpression("col1", 7.89D, Operator.GT));
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().gtText("col1", "text2"),
        new ConditionalExpression("col1", "text2", Operator.GT));
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().gtBlob("col1", "blob3".getBytes(StandardCharsets.UTF_8)),
        new ConditionalExpression("col1", "blob3".getBytes(StandardCharsets.UTF_8), Operator.GT));
    xxxIf_gtConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf()
            .gtBlob("col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8))),
        new ConditionalExpression(
            "col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8)), Operator.GT));
  }

  @Test
  public void deleteIf_gteConditionGiven_ShouldBuildProperly() {
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().gteBoolean("col1", true),
        new ConditionalExpression("col1", true, Operator.GTE));
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().gteInt("col1", 789),
        new ConditionalExpression("col1", 789, Operator.GTE));
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().gteBigInt("col1", 789L),
        new ConditionalExpression("col1", 789L, Operator.GTE));
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().gteFloat("col1", 7.89F),
        new ConditionalExpression("col1", 7.89F, Operator.GTE));
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().gteDouble("col1", 7.89D),
        new ConditionalExpression("col1", 7.89D, Operator.GTE));
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().gteText("col1", "text2"),
        new ConditionalExpression("col1", "text2", Operator.GTE));
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().gteBlob("col1", "blob3".getBytes(StandardCharsets.UTF_8)),
        new ConditionalExpression("col1", "blob3".getBytes(StandardCharsets.UTF_8), Operator.GTE));
    xxxIf_gteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf()
            .gteBlob("col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8))),
        new ConditionalExpression(
            "col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8)), Operator.GTE));
  }

  @Test
  public void deleteIf_ltConditionGiven_ShouldBuildProperly() {
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().ltBoolean("col1", true),
        new ConditionalExpression("col1", true, Operator.LT));
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().ltInt("col1", 789),
        new ConditionalExpression("col1", 789, Operator.LT));
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().ltBigInt("col1", 789L),
        new ConditionalExpression("col1", 789L, Operator.LT));
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().ltFloat("col1", 7.89F),
        new ConditionalExpression("col1", 7.89F, Operator.LT));
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().ltDouble("col1", 7.89D),
        new ConditionalExpression("col1", 7.89D, Operator.LT));
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().ltText("col1", "text2"),
        new ConditionalExpression("col1", "text2", Operator.LT));
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().ltBlob("col1", "blob3".getBytes(StandardCharsets.UTF_8)),
        new ConditionalExpression("col1", "blob3".getBytes(StandardCharsets.UTF_8), Operator.LT));
    xxxIf_ltConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf()
            .ltBlob("col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8))),
        new ConditionalExpression(
            "col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8)), Operator.LT));
  }

  @Test
  public void deleteIf_lteConditionGiven_ShouldBuildProperly() {
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().lteBoolean("col1", true),
        new ConditionalExpression("col1", true, Operator.LTE));
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().lteInt("col1", 789),
        new ConditionalExpression("col1", 789, Operator.LTE));
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().lteBigInt("col1", 789L),
        new ConditionalExpression("col1", 789L, Operator.LTE));
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().lteFloat("col1", 7.89F),
        new ConditionalExpression("col1", 7.89F, Operator.LTE));
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().lteDouble("col1", 7.89D),
        new ConditionalExpression("col1", 7.89D, Operator.LTE));
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().lteText("col1", "text2"),
        new ConditionalExpression("col1", "text2", Operator.LTE));
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf().lteBlob("col1", "blob3".getBytes(StandardCharsets.UTF_8)),
        new ConditionalExpression("col1", "blob3".getBytes(StandardCharsets.UTF_8), Operator.LTE));
    xxxIf_lteConditionGiven_ShouldBuildProperly(
        false,
        ConditionBuilder.deleteIf()
            .lteBlob("col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8))),
        new ConditionalExpression(
            "col1", ByteBuffer.wrap("blob4".getBytes(StandardCharsets.UTF_8)), Operator.LTE));
  }

  @Test
  public void deleteIfExists_ShouldBuildProperly() {
    // Arrange

    // Act
    MutationCondition actual = ConditionBuilder.deleteIfExists();

    // Assert
    assertThat(actual instanceof DeleteIfExists).isTrue();
    assertThat(actual.getExpressions()).isEmpty();
  }

  private void xxxIf_eqConditionGiven_ShouldBuildProperly(
      boolean isPutIf, IfBuilder ifBuilder, ConditionalExpression firstConditionalExpression) {
    // Arrange

    // Act
    MutationCondition actual =
        ifBuilder
            .andEqBoolean("col2", false)
            .andEqInt("col3", 123)
            .andEqBigInt("col4", 456L)
            .andEqFloat("col5", 1.23F)
            .andEqDouble("col6", 4.56D)
            .andEqText("col7", "text")
            .andEqBlob("col8", "blob1".getBytes(StandardCharsets.UTF_8))
            .andEqBlob("col9", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)))
            .build();

    // Assert
    if (isPutIf) {
      assertThat(actual instanceof PutIf).isTrue();
    } else {
      assertThat(actual instanceof DeleteIf).isTrue();
    }
    assertThat(actual.getExpressions().size()).isEqualTo(9);
    assertThat(actual.getExpressions().get(0)).isEqualTo(firstConditionalExpression);
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", false, Operator.EQ));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 123, Operator.EQ));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 456L, Operator.EQ));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 1.23F, Operator.EQ));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", 4.56D, Operator.EQ));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(new ConditionalExpression("col7", "text", Operator.EQ));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", "blob1".getBytes(StandardCharsets.UTF_8), Operator.EQ));
    assertThat(actual.getExpressions().get(8))
        .isEqualTo(
            new ConditionalExpression(
                "col9", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.EQ));
  }

  private void xxxIf_neConditionGiven_ShouldBuildProperly(
      boolean isPutIf, IfBuilder ifBuilder, ConditionalExpression firstConditionalExpression) {
    // Act
    MutationCondition actual =
        ifBuilder
            .andNeBoolean("col2", false)
            .andNeInt("col3", 123)
            .andNeBigInt("col4", 456L)
            .andNeFloat("col5", 1.23F)
            .andNeDouble("col6", 4.56D)
            .andNeText("col7", "text")
            .andNeBlob("col8", "blob1".getBytes(StandardCharsets.UTF_8))
            .andNeBlob("col9", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)))
            .build();

    // Assert
    if (isPutIf) {
      assertThat(actual instanceof PutIf).isTrue();
    } else {
      assertThat(actual instanceof DeleteIf).isTrue();
    }
    assertThat(actual.getExpressions().size()).isEqualTo(9);
    assertThat(actual.getExpressions().get(0)).isEqualTo(firstConditionalExpression);
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", false, Operator.NE));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 123, Operator.NE));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 456L, Operator.NE));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 1.23F, Operator.NE));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", 4.56D, Operator.NE));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(new ConditionalExpression("col7", "text", Operator.NE));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", "blob1".getBytes(StandardCharsets.UTF_8), Operator.NE));
    assertThat(actual.getExpressions().get(8))
        .isEqualTo(
            new ConditionalExpression(
                "col9", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.NE));
  }

  private void xxxIf_gtConditionGiven_ShouldBuildProperly(
      boolean isPutIf, IfBuilder ifBuilder, ConditionalExpression firstConditionalExpression) {
    // Act
    MutationCondition actual =
        ifBuilder
            .andGtBoolean("col2", false)
            .andGtInt("col3", 123)
            .andGtBigInt("col4", 456L)
            .andGtFloat("col5", 1.23F)
            .andGtDouble("col6", 4.56D)
            .andGtText("col7", "text")
            .andGtBlob("col8", "blob1".getBytes(StandardCharsets.UTF_8))
            .andGtBlob("col9", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)))
            .build();

    // Assert
    if (isPutIf) {
      assertThat(actual instanceof PutIf).isTrue();
    } else {
      assertThat(actual instanceof DeleteIf).isTrue();
    }
    assertThat(actual.getExpressions().size()).isEqualTo(9);
    assertThat(actual.getExpressions().get(0)).isEqualTo(firstConditionalExpression);
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", false, Operator.GT));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 123, Operator.GT));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 456L, Operator.GT));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 1.23F, Operator.GT));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", 4.56D, Operator.GT));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(new ConditionalExpression("col7", "text", Operator.GT));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", "blob1".getBytes(StandardCharsets.UTF_8), Operator.GT));
    assertThat(actual.getExpressions().get(8))
        .isEqualTo(
            new ConditionalExpression(
                "col9", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.GT));
  }

  private void xxxIf_gteConditionGiven_ShouldBuildProperly(
      boolean isPutIf, IfBuilder ifBuilder, ConditionalExpression firstConditionalExpression) {
    // Act
    MutationCondition actual =
        ifBuilder
            .andGteBoolean("col2", false)
            .andGteInt("col3", 123)
            .andGteBigInt("col4", 456L)
            .andGteFloat("col5", 1.23F)
            .andGteDouble("col6", 4.56D)
            .andGteText("col7", "text")
            .andGteBlob("col8", "blob1".getBytes(StandardCharsets.UTF_8))
            .andGteBlob("col9", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)))
            .build();

    // Assert
    if (isPutIf) {
      assertThat(actual instanceof PutIf).isTrue();
    } else {
      assertThat(actual instanceof DeleteIf).isTrue();
    }
    assertThat(actual.getExpressions().size()).isEqualTo(9);
    assertThat(actual.getExpressions().get(0)).isEqualTo(firstConditionalExpression);
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", false, Operator.GTE));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 123, Operator.GTE));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 456L, Operator.GTE));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 1.23F, Operator.GTE));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", 4.56D, Operator.GTE));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(new ConditionalExpression("col7", "text", Operator.GTE));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", "blob1".getBytes(StandardCharsets.UTF_8), Operator.GTE));
    assertThat(actual.getExpressions().get(8))
        .isEqualTo(
            new ConditionalExpression(
                "col9", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.GTE));
  }

  private void xxxIf_ltConditionGiven_ShouldBuildProperly(
      boolean isPutIf, IfBuilder ifBuilder, ConditionalExpression firstConditionalExpression) {
    // Act
    MutationCondition actual =
        ifBuilder
            .andLtBoolean("col2", false)
            .andLtInt("col3", 123)
            .andLtBigInt("col4", 456L)
            .andLtFloat("col5", 1.23F)
            .andLtDouble("col6", 4.56D)
            .andLtText("col7", "text")
            .andLtBlob("col8", "blob1".getBytes(StandardCharsets.UTF_8))
            .andLtBlob("col9", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)))
            .build();

    // Assert
    if (isPutIf) {
      assertThat(actual instanceof PutIf).isTrue();
    } else {
      assertThat(actual instanceof DeleteIf).isTrue();
    }
    assertThat(actual.getExpressions().size()).isEqualTo(9);
    assertThat(actual.getExpressions().get(0)).isEqualTo(firstConditionalExpression);
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", false, Operator.LT));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 123, Operator.LT));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 456L, Operator.LT));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 1.23F, Operator.LT));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", 4.56D, Operator.LT));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(new ConditionalExpression("col7", "text", Operator.LT));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", "blob1".getBytes(StandardCharsets.UTF_8), Operator.LT));
    assertThat(actual.getExpressions().get(8))
        .isEqualTo(
            new ConditionalExpression(
                "col9", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.LT));
  }

  private void xxxIf_lteConditionGiven_ShouldBuildProperly(
      boolean isPutIf, IfBuilder ifBuilder, ConditionalExpression firstConditionalExpression) {
    // Act
    MutationCondition actual =
        ifBuilder
            .andLteBoolean("col2", false)
            .andLteInt("col3", 123)
            .andLteBigInt("col4", 456L)
            .andLteFloat("col5", 1.23F)
            .andLteDouble("col6", 4.56D)
            .andLteText("col7", "text")
            .andLteBlob("col8", "blob1".getBytes(StandardCharsets.UTF_8))
            .andLteBlob("col9", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)))
            .build();

    // Assert
    if (isPutIf) {
      assertThat(actual instanceof PutIf).isTrue();
    } else {
      assertThat(actual instanceof DeleteIf).isTrue();
    }
    assertThat(actual.getExpressions().size()).isEqualTo(9);
    assertThat(actual.getExpressions().get(0)).isEqualTo(firstConditionalExpression);
    assertThat(actual.getExpressions().get(1))
        .isEqualTo(new ConditionalExpression("col2", false, Operator.LTE));
    assertThat(actual.getExpressions().get(2))
        .isEqualTo(new ConditionalExpression("col3", 123, Operator.LTE));
    assertThat(actual.getExpressions().get(3))
        .isEqualTo(new ConditionalExpression("col4", 456L, Operator.LTE));
    assertThat(actual.getExpressions().get(4))
        .isEqualTo(new ConditionalExpression("col5", 1.23F, Operator.LTE));
    assertThat(actual.getExpressions().get(5))
        .isEqualTo(new ConditionalExpression("col6", 4.56D, Operator.LTE));
    assertThat(actual.getExpressions().get(6))
        .isEqualTo(new ConditionalExpression("col7", "text", Operator.LTE));
    assertThat(actual.getExpressions().get(7))
        .isEqualTo(
            new ConditionalExpression(
                "col8", "blob1".getBytes(StandardCharsets.UTF_8), Operator.LTE));
    assertThat(actual.getExpressions().get(8))
        .isEqualTo(
            new ConditionalExpression(
                "col9", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.LTE));
  }
}
