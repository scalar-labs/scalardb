package com.scalar.db.util;

import com.google.protobuf.ByteString;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.rpc.MutateCondition;
import com.scalar.db.rpc.Order;
import java.util.Map;
import java.util.stream.Collectors;

public final class ProtoUtils {
  private ProtoUtils() {}

  public static Get toGet(com.scalar.db.rpc.Get get) {
    Key partitionKey = toKey(get.getPartitionKey());
    Key clusteringKey;
    if (get.hasClusteringKey()) {
      clusteringKey = toKey(get.getClusteringKey());
    } else {
      clusteringKey = null;
    }

    Get ret = new Get(partitionKey, clusteringKey);
    if (!get.getNamespace().isEmpty()) {
      ret.forNamespace(get.getNamespace());
    }
    if (!get.getTable().isEmpty()) {
      ret.forTable(get.getTable());
    }
    ret.withConsistency(toConsistency(get.getConsistency()));
    ret.withProjections(get.getProjectionList());
    return ret;
  }

  public static com.scalar.db.rpc.Get toGet(Get get) {
    com.scalar.db.rpc.Get.Builder builder = com.scalar.db.rpc.Get.newBuilder();
    builder.setPartitionKey(toKey(get.getPartitionKey()));
    get.getClusteringKey().ifPresent(k -> builder.setClusteringKey(toKey(k)));
    get.forNamespace().ifPresent(builder::setNamespace);
    get.forTable().ifPresent(builder::setTable);
    return builder
        .setConsistency(toConsistency(get.getConsistency()))
        .addAllProjection(get.getProjections())
        .build();
  }

  private static Key toKey(com.scalar.db.rpc.Key key) {
    return new Key(
        key.getValueList().stream().map(ProtoUtils::toValue).collect(Collectors.toList()));
  }

  private static com.scalar.db.rpc.Key toKey(Key key) {
    com.scalar.db.rpc.Key.Builder builder = com.scalar.db.rpc.Key.newBuilder();
    key.forEach(v -> builder.addValue(toValue(v)));
    return builder.build();
  }

  private static Value<?> toValue(com.scalar.db.rpc.Value value) {
    switch (value.getValueCase()) {
      case BOOLEAN_VALUE:
        return new BooleanValue(value.getName(), value.getBooleanValue());
      case INT_VALUE:
        return new IntValue(value.getName(), value.getIntValue());
      case BIGINT_VALUE:
        return new BigIntValue(value.getName(), value.getBigintValue());
      case FLOAT_VALUE:
        return new FloatValue(value.getName(), value.getFloatValue());
      case DOUBLE_VALUE:
        return new DoubleValue(value.getName(), value.getDoubleValue());
      case TEXT_VALUE:
        if (value.getTextValue().hasValue()) {
          return new TextValue(value.getName(), value.getTextValue().getValue());
        } else {
          return new TextValue(value.getName(), (String) null);
        }
      case BLOB_VALUE:
        if (value.getBlobValue().hasValue()) {
          return new BlobValue(value.getName(), value.getBlobValue().getValue().toByteArray());
        } else {
          return new BlobValue(value.getName(), (byte[]) null);
        }
      default:
        throw new AssertionError();
    }
  }

  private static com.scalar.db.rpc.Value toValue(Value<?> value) {
    com.scalar.db.rpc.Value.Builder builder =
        com.scalar.db.rpc.Value.newBuilder().setName(value.getName());
    if (value instanceof BooleanValue) {
      return builder.setBooleanValue(value.getAsBoolean()).build();
    } else if (value instanceof IntValue) {
      return builder.setIntValue(value.getAsInt()).build();
    } else if (value instanceof BigIntValue) {
      return builder.setBigintValue(value.getAsLong()).build();
    } else if (value instanceof FloatValue) {
      return builder.setFloatValue(value.getAsFloat()).build();
    } else if (value instanceof DoubleValue) {
      return builder.setDoubleValue(value.getAsDouble()).build();
    } else if (value instanceof TextValue) {
      com.scalar.db.rpc.Value.TextValue.Builder textValueBuilder =
          com.scalar.db.rpc.Value.TextValue.newBuilder();
      value.getAsString().ifPresent(textValueBuilder::setValue);
      return builder.setTextValue(textValueBuilder).build();
    } else if (value instanceof BlobValue) {
      com.scalar.db.rpc.Value.BlobValue.Builder blobValueBuilder =
          com.scalar.db.rpc.Value.BlobValue.newBuilder();
      value.getAsBytes().ifPresent(v -> blobValueBuilder.setValue(ByteString.copyFrom(v)));
      return builder.setBlobValue(blobValueBuilder).build();
    } else {
      throw new AssertionError();
    }
  }

  private static Consistency toConsistency(com.scalar.db.rpc.Consistency consistency) {
    switch (consistency) {
      case CONSISTENCY_SEQUENTIAL:
        return Consistency.SEQUENTIAL;
      case CONSISTENCY_EVENTUAL:
        return Consistency.EVENTUAL;
      case CONSISTENCY_LINEARIZABLE:
        return Consistency.LINEARIZABLE;
      default:
        throw new AssertionError();
    }
  }

  private static com.scalar.db.rpc.Consistency toConsistency(Consistency consistency) {
    switch (consistency) {
      case SEQUENTIAL:
        return com.scalar.db.rpc.Consistency.CONSISTENCY_SEQUENTIAL;
      case EVENTUAL:
        return com.scalar.db.rpc.Consistency.CONSISTENCY_EVENTUAL;
      case LINEARIZABLE:
        return com.scalar.db.rpc.Consistency.CONSISTENCY_LINEARIZABLE;
      default:
        throw new AssertionError();
    }
  }

  public static Scan toScan(com.scalar.db.rpc.Scan scan) {
    Scan ret = new Scan(toKey(scan.getPartitionKey()));
    if (scan.hasStartClusteringKey()) {
      ret.withStart(toKey(scan.getStartClusteringKey()), scan.getStartInclusive());
    }
    if (scan.hasEndClusteringKey()) {
      ret.withEnd(toKey(scan.getEndClusteringKey()), scan.getEndInclusive());
    }
    scan.getOrderingList().forEach(o -> ret.withOrdering(toOrdering(o)));
    ret.withLimit(scan.getLimit());
    if (!scan.getNamespace().isEmpty()) {
      ret.forNamespace(scan.getNamespace());
    }
    if (!scan.getTable().isEmpty()) {
      ret.forTable(scan.getTable());
    }
    ret.withConsistency(toConsistency(scan.getConsistency()));
    ret.withProjections(scan.getProjectionList());
    return ret;
  }

  public static com.scalar.db.rpc.Scan toScan(Scan scan) {
    com.scalar.db.rpc.Scan.Builder builder = com.scalar.db.rpc.Scan.newBuilder();
    builder.setPartitionKey(toKey(scan.getPartitionKey()));
    scan.getStartClusteringKey()
        .ifPresent(
            k ->
                builder
                    .setStartClusteringKey(toKey(k))
                    .setStartInclusive(scan.getStartInclusive()));
    scan.getEndClusteringKey()
        .ifPresent(
            k -> builder.setEndClusteringKey(toKey(k)).setEndInclusive(scan.getEndInclusive()));
    scan.getOrderings().forEach(o -> builder.addOrdering(toOrdering(o)));
    builder.setLimit(scan.getLimit());
    scan.forNamespace().ifPresent(builder::setNamespace);
    scan.forTable().ifPresent(builder::setTable);
    return builder
        .setConsistency(toConsistency(scan.getConsistency()))
        .addAllProjection(scan.getProjections())
        .build();
  }

  private static Scan.Ordering toOrdering(com.scalar.db.rpc.Ordering ordering) {
    return new Scan.Ordering(ordering.getName(), toOrder(ordering.getOrder()));
  }

  private static com.scalar.db.rpc.Ordering toOrdering(Scan.Ordering ordering) {
    return com.scalar.db.rpc.Ordering.newBuilder()
        .setName(ordering.getName())
        .setOrder(toOrder(ordering.getOrder()))
        .build();
  }

  private static Scan.Ordering.Order toOrder(com.scalar.db.rpc.Order order) {
    switch (order) {
      case ORDER_ASC:
        return Scan.Ordering.Order.ASC;
      case ORDER_DESC:
        return Scan.Ordering.Order.DESC;
      default:
        throw new AssertionError();
    }
  }

  private static com.scalar.db.rpc.Order toOrder(Scan.Ordering.Order order) {
    switch (order) {
      case ASC:
        return com.scalar.db.rpc.Order.ORDER_ASC;
      case DESC:
        return com.scalar.db.rpc.Order.ORDER_DESC;
      default:
        throw new AssertionError();
    }
  }

  public static Mutation toMutation(com.scalar.db.rpc.Mutation mutation) {
    Key partitionKey = toKey(mutation.getPartitionKey());
    Key clusteringKey;
    if (mutation.hasClusteringKey()) {
      clusteringKey = toKey(mutation.getClusteringKey());
    } else {
      clusteringKey = null;
    }

    Mutation ret;
    if (mutation.getType() == com.scalar.db.rpc.Mutation.Type.PUT) {
      Put put = new Put(partitionKey, clusteringKey);
      mutation.getValueList().forEach(v -> put.withValue(toValue(v)));
      ret = put;
    } else {
      ret = new Delete(partitionKey, clusteringKey);
    }
    if (!mutation.getNamespace().isEmpty()) {
      ret.forNamespace(mutation.getNamespace());
    }
    if (!mutation.getTable().isEmpty()) {
      ret.forTable(mutation.getTable());
    }
    ret.withConsistency(toConsistency(mutation.getConsistency()));
    if (mutation.hasCondition()) {
      ret.withCondition(toCondition(mutation.getCondition()));
    }
    return ret;
  }

  public static com.scalar.db.rpc.Mutation toMutation(Mutation mutation) {
    com.scalar.db.rpc.Mutation.Builder builder =
        com.scalar.db.rpc.Mutation.newBuilder().setPartitionKey(toKey(mutation.getPartitionKey()));
    mutation.getClusteringKey().ifPresent(k -> builder.setClusteringKey(toKey(k)));
    if (mutation instanceof Put) {
      builder.setType(com.scalar.db.rpc.Mutation.Type.PUT);
      ((Put) mutation).getValues().values().forEach(v -> builder.addValue(toValue(v)));
    } else {
      builder.setType(com.scalar.db.rpc.Mutation.Type.DELETE);
    }
    mutation.forNamespace().ifPresent(builder::setNamespace);
    mutation.forTable().ifPresent(builder::setTable);
    builder.setConsistency(toConsistency(mutation.getConsistency()));
    mutation.getCondition().ifPresent(c -> builder.setCondition(toCondition(c)));
    return builder.build();
  }

  private static MutationCondition toCondition(com.scalar.db.rpc.MutateCondition condition) {
    switch (condition.getType()) {
      case PUT_IF:
        return new PutIf(
            condition.getExpressionList().stream()
                .map(ProtoUtils::toExpression)
                .collect(Collectors.toList()));
      case PUT_IF_EXISTS:
        return new PutIfExists();
      case PUT_IF_NOT_EXISTS:
        return new PutIfNotExists();
      case DELETE_IF:
        return new DeleteIf(
            condition.getExpressionList().stream()
                .map(ProtoUtils::toExpression)
                .collect(Collectors.toList()));
      case DELETE_IF_EXISTS:
        return new DeleteIfExists();
      default:
        throw new AssertionError();
    }
  }

  private static com.scalar.db.rpc.MutateCondition toCondition(MutationCondition condition) {
    if (condition instanceof PutIf) {
      MutateCondition.Builder builder =
          MutateCondition.newBuilder().setType(com.scalar.db.rpc.MutateCondition.Type.PUT_IF);
      condition.getExpressions().forEach(e -> builder.addExpression(toExpression(e)));
      return builder.build();
    } else if (condition instanceof PutIfExists) {
      return com.scalar.db.rpc.MutateCondition.newBuilder()
          .setType(com.scalar.db.rpc.MutateCondition.Type.PUT_IF_EXISTS)
          .build();
    } else if (condition instanceof PutIfNotExists) {
      return com.scalar.db.rpc.MutateCondition.newBuilder()
          .setType(com.scalar.db.rpc.MutateCondition.Type.PUT_IF_NOT_EXISTS)
          .build();
    } else if (condition instanceof DeleteIf) {
      MutateCondition.Builder builder =
          MutateCondition.newBuilder().setType(com.scalar.db.rpc.MutateCondition.Type.DELETE_IF);
      condition.getExpressions().forEach(e -> builder.addExpression(toExpression(e)));
      return builder.build();
    } else if (condition instanceof DeleteIfExists) {
      return com.scalar.db.rpc.MutateCondition.newBuilder()
          .setType(com.scalar.db.rpc.MutateCondition.Type.DELETE_IF_EXISTS)
          .build();
    } else {
      throw new AssertionError();
    }
  }

  private static ConditionalExpression toExpression(
      com.scalar.db.rpc.ConditionalExpression expression) {
    return new ConditionalExpression(
        expression.getName(), toValue(expression.getValue()), toOperator(expression.getOperator()));
  }

  private static com.scalar.db.rpc.ConditionalExpression toExpression(
      ConditionalExpression expression) {
    return com.scalar.db.rpc.ConditionalExpression.newBuilder()
        .setName(expression.getName())
        .setValue(toValue(expression.getValue()))
        .setOperator(toOperator(expression.getOperator()))
        .build();
  }

  private static ConditionalExpression.Operator toOperator(
      com.scalar.db.rpc.ConditionalExpression.Operator operator) {
    switch (operator) {
      case EQ:
        return ConditionalExpression.Operator.EQ;
      case NE:
        return ConditionalExpression.Operator.NE;
      case GT:
        return ConditionalExpression.Operator.GT;
      case GTE:
        return ConditionalExpression.Operator.GTE;
      case LT:
        return ConditionalExpression.Operator.LT;
      case LTE:
        return ConditionalExpression.Operator.LTE;
      default:
        throw new AssertionError();
    }
  }

  private static com.scalar.db.rpc.ConditionalExpression.Operator toOperator(
      ConditionalExpression.Operator operator) {
    switch (operator) {
      case EQ:
        return com.scalar.db.rpc.ConditionalExpression.Operator.EQ;
      case NE:
        return com.scalar.db.rpc.ConditionalExpression.Operator.NE;
      case GT:
        return com.scalar.db.rpc.ConditionalExpression.Operator.GT;
      case GTE:
        return com.scalar.db.rpc.ConditionalExpression.Operator.GTE;
      case LT:
        return com.scalar.db.rpc.ConditionalExpression.Operator.LT;
      case LTE:
        return com.scalar.db.rpc.ConditionalExpression.Operator.LTE;
      default:
        throw new AssertionError();
    }
  }

  public static Result toResult(com.scalar.db.rpc.Result result, TableMetadata metadata) {
    return new ResultImpl(
        result.getValueList().stream()
            .map(ProtoUtils::toValue)
            .collect(Collectors.toMap(Value::getName, v -> v)),
        metadata);
  }

  public static com.scalar.db.rpc.Result toResult(Result result) {
    com.scalar.db.rpc.Result.Builder builder = com.scalar.db.rpc.Result.newBuilder();
    result.getValues().values().forEach(v -> builder.addValue(toValue(v)));
    return builder.build();
  }

  public static TableMetadata toTableMetadata(com.scalar.db.rpc.TableMetadata tableMetadata) {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    tableMetadata.getColumnMap().forEach((n, t) -> builder.addColumn(n, toDataType(t)));
    tableMetadata.getPartitionKeyNameList().forEach(builder::addPartitionKey);
    Map<String, Order> clusteringOrderMap = tableMetadata.getClusteringOrderMap();
    tableMetadata
        .getClusteringKeyNameList()
        .forEach(n -> builder.addClusteringKey(n, toOrder(clusteringOrderMap.get(n))));
    tableMetadata.getSecondaryIndexNameList().forEach(builder::addSecondaryIndex);
    return builder.build();
  }

  public static com.scalar.db.rpc.TableMetadata toTableMetadata(TableMetadata tableMetadata) {
    com.scalar.db.rpc.TableMetadata.Builder builder = com.scalar.db.rpc.TableMetadata.newBuilder();
    tableMetadata
        .getColumnNames()
        .forEach(n -> builder.putColumn(n, toDataType(tableMetadata.getColumnDataType(n))));
    tableMetadata.getPartitionKeyNames().forEach(builder::addPartitionKeyName);
    tableMetadata
        .getClusteringKeyNames()
        .forEach(
            n -> {
              builder.addClusteringKeyName(n);
              builder.putClusteringOrder(n, toOrder(tableMetadata.getClusteringOrder(n)));
            });
    tableMetadata.getSecondaryIndexNames().forEach(builder::addSecondaryIndexName);
    return builder.build();
  }

  private static DataType toDataType(com.scalar.db.rpc.DataType dataType) {
    switch (dataType) {
      case DATA_TYPE_BOOLEAN:
        return DataType.BOOLEAN;
      case DATA_TYPE_INT:
        return DataType.INT;
      case DATA_TYPE_BIGINT:
        return DataType.BIGINT;
      case DATA_TYPE_FLOAT:
        return DataType.FLOAT;
      case DATA_TYPE_DOUBLE:
        return DataType.DOUBLE;
      case DATA_TYPE_TEXT:
        return DataType.TEXT;
      case DATA_TYPE_BLOB:
        return DataType.BLOB;
      default:
        throw new AssertionError();
    }
  }

  private static com.scalar.db.rpc.DataType toDataType(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return com.scalar.db.rpc.DataType.DATA_TYPE_BOOLEAN;
      case INT:
        return com.scalar.db.rpc.DataType.DATA_TYPE_INT;
      case BIGINT:
        return com.scalar.db.rpc.DataType.DATA_TYPE_BIGINT;
      case FLOAT:
        return com.scalar.db.rpc.DataType.DATA_TYPE_FLOAT;
      case DOUBLE:
        return com.scalar.db.rpc.DataType.DATA_TYPE_DOUBLE;
      case TEXT:
        return com.scalar.db.rpc.DataType.DATA_TYPE_TEXT;
      case BLOB:
        return com.scalar.db.rpc.DataType.DATA_TYPE_BLOB;
      default:
        throw new AssertionError();
    }
  }

  public static com.scalar.db.rpc.TransactionState toTransactionState(TransactionState state) {
    switch (state) {
      case COMMITTED:
        return com.scalar.db.rpc.TransactionState.TRANSACTION_STATE_COMMITTED;
      case ABORTED:
        return com.scalar.db.rpc.TransactionState.TRANSACTION_STATE_ABORTED;
      case UNKNOWN:
      default:
        return com.scalar.db.rpc.TransactionState.TRANSACTION_STATE_UNKNOWN;
    }
  }

  public static TransactionState toTransactionState(com.scalar.db.rpc.TransactionState state) {
    switch (state) {
      case TRANSACTION_STATE_COMMITTED:
        return TransactionState.COMMITTED;
      case TRANSACTION_STATE_ABORTED:
        return TransactionState.ABORTED;
      case TRANSACTION_STATE_UNKNOWN:
        return TransactionState.UNKNOWN;
      default:
        throw new AssertionError();
    }
  }
}
