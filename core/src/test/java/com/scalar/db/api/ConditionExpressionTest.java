package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class ConditionExpressionTest {

  @Test
  public void constructor_ProperArgsGiven_ShouldConstructProperly() {
    // Arrange

    // Act
    ConditionalExpression expression1 =
        new ConditionalExpression("col1", new TextValue("aaa"), Operator.EQ);
    ConditionalExpression expression2 = new ConditionalExpression("col2", true, Operator.EQ);
    ConditionalExpression expression3 = new ConditionalExpression("col3", 123, Operator.NE);
    ConditionalExpression expression4 = new ConditionalExpression("col4", 456L, Operator.GT);
    ConditionalExpression expression5 = new ConditionalExpression("col5", 1.23F, Operator.GTE);
    ConditionalExpression expression6 = new ConditionalExpression("col6", 4.56D, Operator.LT);
    ConditionalExpression expression7 = new ConditionalExpression("col7", "text", Operator.LTE);
    ConditionalExpression expression8 =
        new ConditionalExpression("col8", "blob".getBytes(StandardCharsets.UTF_8), Operator.EQ);
    ConditionalExpression expression9 =
        new ConditionalExpression(
            "col9", ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)), Operator.NE);

    // Assert
    assertThat(expression1.getColumnName()).isEqualTo("col1");
    assertThat(expression1.getValue()).isEqualTo(new TextValue("aaa"));
    assertThat(expression1.getTextValue()).isEqualTo("aaa");
    assertThat(expression1.getValueAsObject()).isEqualTo("aaa");
    assertThat(expression1.getOperator()).isEqualTo(Operator.EQ);

    assertThat(expression2.getColumnName()).isEqualTo("col2");
    assertThat(expression2.getValue()).isEqualTo(new BooleanValue(true));
    assertThat(expression2.getBooleanValue()).isTrue();
    assertThat(expression2.getValueAsObject()).isEqualTo(true);
    assertThat(expression2.getOperator()).isEqualTo(Operator.EQ);

    assertThat(expression3.getColumnName()).isEqualTo("col3");
    assertThat(expression3.getValue()).isEqualTo(new IntValue(123));
    assertThat(expression3.getIntValue()).isEqualTo(123);
    assertThat(expression3.getValueAsObject()).isEqualTo(123);
    assertThat(expression3.getOperator()).isEqualTo(Operator.NE);

    assertThat(expression4.getColumnName()).isEqualTo("col4");
    assertThat(expression4.getValue()).isEqualTo(new BigIntValue(456L));
    assertThat(expression4.getBigIntValue()).isEqualTo(456L);
    assertThat(expression4.getValueAsObject()).isEqualTo(456L);
    assertThat(expression4.getOperator()).isEqualTo(Operator.GT);

    assertThat(expression5.getColumnName()).isEqualTo("col5");
    assertThat(expression5.getValue()).isEqualTo(new FloatValue(1.23F));
    assertThat(expression5.getFloatValue()).isEqualTo(1.23F);
    assertThat(expression5.getValueAsObject()).isEqualTo(1.23F);
    assertThat(expression5.getOperator()).isEqualTo(Operator.GTE);

    assertThat(expression6.getColumnName()).isEqualTo("col6");
    assertThat(expression6.getValue()).isEqualTo(new DoubleValue(4.56D));
    assertThat(expression6.getDoubleValue()).isEqualTo(4.56D);
    assertThat(expression6.getValueAsObject()).isEqualTo(4.56D);
    assertThat(expression6.getOperator()).isEqualTo(Operator.LT);

    assertThat(expression7.getColumnName()).isEqualTo("col7");
    assertThat(expression7.getValue()).isEqualTo(new TextValue("text"));
    assertThat(expression7.getTextValue()).isEqualTo("text");
    assertThat(expression7.getValueAsObject()).isEqualTo("text");
    assertThat(expression7.getOperator()).isEqualTo(Operator.LTE);

    assertThat(expression8.getColumnName()).isEqualTo("col8");
    assertThat(expression8.getValue())
        .isEqualTo(new BlobValue("blob".getBytes(StandardCharsets.UTF_8)));
    assertThat(expression8.getBlobValue())
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThat(expression8.getBlobValueAsByteBuffer())
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThat(expression8.getBlobValueAsBytes())
        .isEqualTo("blob".getBytes(StandardCharsets.UTF_8));
    assertThat(expression8.getValueAsObject())
        .isEqualTo(ByteBuffer.wrap("blob".getBytes(StandardCharsets.UTF_8)));
    assertThat(expression8.getOperator()).isEqualTo(Operator.EQ);

    assertThat(expression9.getColumnName()).isEqualTo("col9");
    assertThat(expression9.getValue())
        .isEqualTo(new BlobValue("blob2".getBytes(StandardCharsets.UTF_8)));
    assertThat(expression9.getBlobValue())
        .isEqualTo(ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)));
    assertThat(expression9.getBlobValueAsByteBuffer())
        .isEqualTo(ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)));
    assertThat(expression9.getBlobValueAsBytes())
        .isEqualTo("blob2".getBytes(StandardCharsets.UTF_8));
    assertThat(expression9.getValueAsObject())
        .isEqualTo(ByteBuffer.wrap("blob2".getBytes(StandardCharsets.UTF_8)));
    assertThat(expression9.getOperator()).isEqualTo(Operator.NE);
  }
}
