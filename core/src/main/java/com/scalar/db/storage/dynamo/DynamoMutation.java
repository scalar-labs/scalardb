package com.scalar.db.storage.dynamo;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** A utility class for a mutation */
@Immutable
public class DynamoMutation extends DynamoOperation {

  DynamoMutation(Mutation mutation, TableMetadata metadata) {
    super(mutation, metadata);
  }

  @Nonnull
  public Map<String, AttributeValue> getValueMapWithKey() {
    Put put = (Put) getOperation();
    Map<String, AttributeValue> values = getKeyMap();
    values.putAll(toMap(put.getPartitionKey().get()));
    put.getClusteringKey().ifPresent(k -> values.putAll(toMap(k.get())));
    values.putAll(toMap(put.getValues().values()));

    return values;
  }

  @Nonnull
  public String getIfNotExistsCondition() {
    List<String> expressions = new ArrayList<>();
    expressions.add("attribute_not_exists(" + PARTITION_KEY + ")");
    if (getOperation().getClusteringKey().isPresent()) {
      expressions.add("attribute_not_exists(" + CLUSTERING_KEY + ")");
    }

    return String.join(" AND ", expressions);
  }

  @Nonnull
  public String getIfExistsCondition() {
    List<String> expressions = new ArrayList<>();
    expressions.add("attribute_exists(" + PARTITION_KEY + ")");
    if (getOperation().getClusteringKey().isPresent()) {
      expressions.add("attribute_exists(" + CLUSTERING_KEY + ")");
    }

    return String.join(" AND ", expressions);
  }

  @Nonnull
  public String getCondition() {
    ConditionExpressionBuilder builder =
        new ConditionExpressionBuilder(CONDITION_COLUMN_NAME_ALIAS, CONDITION_VALUE_ALIAS);
    Mutation mutation = (Mutation) getOperation();
    mutation.getCondition().ifPresent(c -> c.accept(builder));

    return builder.build();
  }

  @Nonnull
  public String getUpdateExpression() {
    return getUpdateExpression(false);
  }

  @Nonnull
  public String getUpdateExpressionWithKey() {
    return getUpdateExpression(true);
  }

  private String getUpdateExpression(boolean withKey) {
    Put put = (Put) getOperation();
    List<String> expressions = new ArrayList<>();
    int i = 0;

    if (withKey) {
      for (Value<?> unusedKey : put.getPartitionKey().get()) {
        expressions.add(COLUMN_NAME_ALIAS + i + " = " + VALUE_ALIAS + i);
        i++;
      }
      if (put.getClusteringKey().isPresent()) {
        for (Value<?> unusedKey : put.getClusteringKey().get().get()) {
          expressions.add(COLUMN_NAME_ALIAS + i + " = " + VALUE_ALIAS + i);
          i++;
        }
      }
    }

    for (String unusedName : put.getValues().keySet()) {
      expressions.add(COLUMN_NAME_ALIAS + i + " = " + VALUE_ALIAS + i);
      i++;
    }

    return "SET " + String.join(", ", expressions);
  }

  @Nonnull
  public Map<String, String> getColumnMap() {
    return getColumnMap(false);
  }

  @Nonnull
  public Map<String, String> getColumnMapWithKey() {
    return getColumnMap(true);
  }

  private Map<String, String> getColumnMap(boolean withKey) {
    Put put = (Put) getOperation();
    Map<String, String> columnMap = new HashMap<>();
    int i = 0;

    if (withKey) {
      for (Value<?> key : put.getPartitionKey().get()) {
        columnMap.put(COLUMN_NAME_ALIAS + i, key.getName());
        i++;
      }
      if (put.getClusteringKey().isPresent()) {
        for (Value<?> key : put.getClusteringKey().get().get()) {
          columnMap.put(COLUMN_NAME_ALIAS + i, key.getName());
          i++;
        }
      }
    }

    for (String name : put.getValues().keySet()) {
      columnMap.put(COLUMN_NAME_ALIAS + i, name);
      i++;
    }

    return columnMap;
  }

  @Nonnull
  public Map<String, String> getConditionColumnMap() {
    Map<String, String> ret = new HashMap<>();
    Mutation mutation = (Mutation) getOperation();
    if (mutation.getCondition().isPresent()) {
      int index = 0;
      for (ConditionalExpression expression : mutation.getCondition().get().getExpressions()) {
        ret.put(CONDITION_COLUMN_NAME_ALIAS + index, expression.getName());
        index++;
      }
    }
    return ret;
  }

  @Nonnull
  public Map<String, AttributeValue> getConditionBindMap() {
    ValueBinder binder = new ValueBinder(CONDITION_VALUE_ALIAS);
    Mutation mutation = (Mutation) getOperation();
    mutation
        .getCondition()
        .ifPresent(c -> c.getExpressions().forEach(e -> e.getValue().accept(binder)));

    return binder.build();
  }

  @Nonnull
  public Map<String, AttributeValue> getValueBindMap() {
    return getValueBindMap(false);
  }

  @Nonnull
  public Map<String, AttributeValue> getValueBindMapWithKey() {
    return getValueBindMap(true);
  }

  private Map<String, AttributeValue> getValueBindMap(boolean withKey) {
    ValueBinder binder = new ValueBinder(VALUE_ALIAS);
    Put put = (Put) getOperation();

    if (withKey) {
      put.getPartitionKey().get().forEach(v -> v.accept(binder));
      put.getClusteringKey().ifPresent(k -> k.get().forEach(v -> v.accept(binder)));
    }

    put.getValues().forEach((a, v) -> v.accept(binder));

    return binder.build();
  }
}
