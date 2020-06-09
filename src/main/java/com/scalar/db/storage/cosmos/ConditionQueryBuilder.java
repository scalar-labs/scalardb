package com.scalar.db.storage.cosmos;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.MutationConditionVisitor;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.ValueVisitor;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes a query statement for a stored procedure of Cosmos DB from conditions
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public class ConditionQueryBuilder implements MutationConditionVisitor, ValueVisitor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConditionQueryBuilder.class);
  private final StringBuilder builder;

  public ConditionQueryBuilder() {
    builder = new StringBuilder();
    builder.append("SELECT * FROM Record r WHERE ");
  }

  public String build() {
    int length = builder.length();
    return builder.substring(0, length - 5); // remove the last " AND "
  }

  public void withPartitionKey(String partitionKey) {
    builder.append("partitionKey = " + partitionKey + " AND ");
  }

  public void withClusteringKey(Key key) {
    key.forEach(
        v -> {
          builder.append(v.getName() + " = ");
          v.accept(this);
          builder.append(" AND ");
        });
  }

  /**
   * Adds {@code PutIf}-specific conditions to the query
   *
   * @param condition {@code PutIf} condition
   */
  @Override
  public void visit(PutIf condition) {
    condition
        .getExpressions()
        .forEach(
            e -> {
              if (e.getValue() instanceof BlobValue) {
                builder.append("CONVERT(Record." + e.getName() + " USING utf8) ");
              } else {
                builder.append(e.getName() + " ");
              }
              appendOperator(e);
              e.getValue().accept(this);
              builder.append(" AND ");
            });
  }

  /**
   * Adds {@code PutIfExists}-specific conditions to the query
   *
   * @param condition {@code PutIfExists} condition
   */
  @Override
  public void visit(PutIfExists condition) {
    builder.append(" AND ");
  }

  /**
   * Adds {@code PutIfNotExists}-specific conditions to the query
   *
   * @param condition {@code PutIfNotExists} condition
   */
  @Override
  public void visit(PutIfNotExists condition) {
    builder.append(" AND ");
  }

  /**
   * Adds {@code DeleteIf}-specific conditions to the query
   *
   * @param condition {@code DeleteIf} condition
   */
  @Override
  public void visit(DeleteIf condition) {
    condition
        .getExpressions()
        .forEach(
            e -> {
              if (e.getValue() instanceof BlobValue) {
                builder.append("CONVERT(Record." + e.getName() + " USING utf8) ");
              } else {
                builder.append(e.getName() + " ");
              }
              appendOperator(e);
              e.getValue().accept(this);
              builder.append(" AND ");
            });
  }

  /**
   * Adds {@code DeleteIfExists}-specific conditions to the query
   *
   * @param condition {@code DeleteIfExists} condition
   */
  @Override
  public void visit(DeleteIfExists condition) {
    builder.append(" AND ");
  }

  /**
   * Sets the specified {@code BooleanValue} to the query
   *
   * @param value a {@code BooleanValue} to be set
   */
  @Override
  public void visit(BooleanValue value) {
    builder.append(value.get());
  }

  /**
   * Sets the specified {@code IntValue} to the query
   *
   * @param value a {@code IntValue} to be set
   */
  @Override
  public void visit(IntValue value) {
    builder.append(value.get());
  }

  /**
   * Sets the specified {@code BigIntValue} to the query
   *
   * @param value a {@code BigIntValue} to be set
   */
  @Override
  public void visit(BigIntValue value) {
    builder.append(value.get());
  }

  /**
   * Sets the specified {@code FloatValue} to the query
   *
   * @param value a {@code FloatValue} to be set
   */
  @Override
  public void visit(FloatValue value) {
    builder.append(value.get());
  }

  /**
   * Sets the specified {@code DoubleValue} to the query
   *
   * @param value a {@code DoubleValue} to be set
   */
  @Override
  public void visit(DoubleValue value) {
    builder.append(value.get());
  }

  /**
   * Sets the specified {@code TextValue} to the query
   *
   * @param value a {@code TextValue} to be set
   */
  @Override
  public void visit(TextValue value) {
    value.getString().ifPresent(s -> builder.append(s));
  }

  /**
   * Sets the specified {@code BlobValue} to the query
   *
   * @param value a {@code BlobValue} to be set
   */
  @Override
  public void visit(BlobValue value) {
    value
        .get()
        .ifPresent(
            b -> {
              ByteBuffer buffer = (ByteBuffer) ByteBuffer.allocate(b.length).put(b).flip();
              builder.append(new String(buffer.array()));
            });
  }

  private void appendOperator(ConditionalExpression e) {
    switch (e.getOperator()) {
      case EQ:
        builder.append("= ");
      case NE:
        builder.append("!= ");
      case GT:
        builder.append("> ");
      case GTE:
        builder.append(">= ");
      case LT:
        builder.append("< ");
      case LTE:
        builder.append("<= ");
      default:
        // never comes here because ConditionalExpression accepts only above operators
        throw new IllegalArgumentException(e.getOperator() + " is not supported");
    }
  }
}
