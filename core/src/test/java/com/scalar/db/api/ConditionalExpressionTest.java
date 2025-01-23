package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.junit.jupiter.api.Test;

public class ConditionalExpressionTest {

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
    assertThat(expression1.getName()).isEqualTo("col1");
    assertThat(expression1.getValue()).isEqualTo(new TextValue("aaa"));
    assertThat(expression1.getTextValue()).isEqualTo("aaa");
    assertThat(expression1.getValueAsObject()).isEqualTo("aaa");
    assertThat(expression1.getOperator()).isEqualTo(Operator.EQ);

    assertThat(expression2.getName()).isEqualTo("col2");
    assertThat(expression2.getValue()).isEqualTo(new BooleanValue(true));
    assertThat(expression2.getBooleanValue()).isTrue();
    assertThat(expression2.getValueAsObject()).isEqualTo(true);
    assertThat(expression2.getOperator()).isEqualTo(Operator.EQ);

    assertThat(expression3.getName()).isEqualTo("col3");
    assertThat(expression3.getValue()).isEqualTo(new IntValue(123));
    assertThat(expression3.getIntValue()).isEqualTo(123);
    assertThat(expression3.getValueAsObject()).isEqualTo(123);
    assertThat(expression3.getOperator()).isEqualTo(Operator.NE);

    assertThat(expression4.getName()).isEqualTo("col4");
    assertThat(expression4.getValue()).isEqualTo(new BigIntValue(456L));
    assertThat(expression4.getBigIntValue()).isEqualTo(456L);
    assertThat(expression4.getValueAsObject()).isEqualTo(456L);
    assertThat(expression4.getOperator()).isEqualTo(Operator.GT);

    assertThat(expression5.getName()).isEqualTo("col5");
    assertThat(expression5.getValue()).isEqualTo(new FloatValue(1.23F));
    assertThat(expression5.getFloatValue()).isEqualTo(1.23F);
    assertThat(expression5.getValueAsObject()).isEqualTo(1.23F);
    assertThat(expression5.getOperator()).isEqualTo(Operator.GTE);

    assertThat(expression6.getName()).isEqualTo("col6");
    assertThat(expression6.getValue()).isEqualTo(new DoubleValue(4.56D));
    assertThat(expression6.getDoubleValue()).isEqualTo(4.56D);
    assertThat(expression6.getValueAsObject()).isEqualTo(4.56D);
    assertThat(expression6.getOperator()).isEqualTo(Operator.LT);

    assertThat(expression7.getName()).isEqualTo("col7");
    assertThat(expression7.getValue()).isEqualTo(new TextValue("text"));
    assertThat(expression7.getTextValue()).isEqualTo("text");
    assertThat(expression7.getValueAsObject()).isEqualTo("text");
    assertThat(expression7.getOperator()).isEqualTo(Operator.LTE);

    assertThat(expression8.getName()).isEqualTo("col8");
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

    assertThat(expression9.getName()).isEqualTo("col9");
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

  @Test
  public void constructor_ColumnAndOperatorGiven_ShouldConstructProperly() {
    // Arrange

    // Act
    ConditionalExpression expression1 =
        new ConditionalExpression(BooleanColumn.of("col1", true), Operator.EQ);
    ConditionalExpression expression2 =
        new ConditionalExpression(IntColumn.of("col2", 123), Operator.NE);
    ConditionalExpression expression3 =
        new ConditionalExpression(BigIntColumn.of("col3", 456L), Operator.GT);
    ConditionalExpression expression4 =
        new ConditionalExpression(FloatColumn.of("col4", 1.23F), Operator.GTE);
    ConditionalExpression expression5 =
        new ConditionalExpression(DoubleColumn.of("col5", 4.56D), Operator.LT);
    ConditionalExpression expression6 =
        new ConditionalExpression(TextColumn.of("col6", "text"), Operator.LTE);
    ConditionalExpression expression7 =
        new ConditionalExpression(
            BlobColumn.of("col7", "blob".getBytes(StandardCharsets.UTF_8)), Operator.EQ);
    ConditionalExpression expression8 =
        new ConditionalExpression(DateColumn.of("col8", LocalDate.ofEpochDay(123)), Operator.NE);
    ConditionalExpression expression9 =
        new ConditionalExpression(TimeColumn.of("col9", LocalTime.ofSecondOfDay(456)), Operator.GT);
    ConditionalExpression expression10 =
        new ConditionalExpression(
            TimestampColumn.of(
                "col10", LocalDateTime.of(LocalDate.ofEpochDay(123), LocalTime.ofSecondOfDay(456))),
            Operator.LT);
    ConditionalExpression expression11 =
        new ConditionalExpression(
            TimestampTZColumn.of("col10", Instant.ofEpochMilli(12345)), Operator.GTE);

    // Assert
    assertThat(expression1.getColumn()).isEqualTo(BooleanColumn.of("col1", true));
    assertThat(expression1.getOperator()).isEqualTo(Operator.EQ);

    assertThat(expression2.getColumn()).isEqualTo(IntColumn.of("col2", 123));
    assertThat(expression2.getOperator()).isEqualTo(Operator.NE);

    assertThat(expression3.getColumn()).isEqualTo(BigIntColumn.of("col3", 456L));
    assertThat(expression3.getOperator()).isEqualTo(Operator.GT);

    assertThat(expression4.getColumn()).isEqualTo(FloatColumn.of("col4", 1.23F));
    assertThat(expression4.getOperator()).isEqualTo(Operator.GTE);

    assertThat(expression5.getColumn()).isEqualTo(DoubleColumn.of("col5", 4.56D));
    assertThat(expression5.getOperator()).isEqualTo(Operator.LT);

    assertThat(expression6.getColumn()).isEqualTo(TextColumn.of("col6", "text"));
    assertThat(expression6.getOperator()).isEqualTo(Operator.LTE);

    assertThat(expression7.getColumn())
        .isEqualTo(BlobColumn.of("col7", "blob".getBytes(StandardCharsets.UTF_8)));
    assertThat(expression7.getOperator()).isEqualTo(Operator.EQ);
    assertThat(expression8.getColumn()).isEqualTo(DateColumn.of("col8", LocalDate.ofEpochDay(123)));
    assertThat(expression8.getOperator()).isEqualTo(Operator.NE);
    assertThat(expression9.getColumn())
        .isEqualTo(TimeColumn.of("col9", LocalTime.ofSecondOfDay(456)));
    assertThat(expression9.getOperator()).isEqualTo(Operator.GT);
    assertThat(expression10.getColumn())
        .isEqualTo(
            TimestampColumn.of(
                "col10",
                LocalDateTime.of(LocalDate.ofEpochDay(123), LocalTime.ofSecondOfDay(456))));
    assertThat(expression10.getOperator()).isEqualTo(Operator.LT);
    assertThat(expression11.getColumn())
        .isEqualTo(TimestampTZColumn.of("col10", Instant.ofEpochMilli(12345)));
    assertThat(expression11.getOperator()).isEqualTo(Operator.GTE);
  }
}
