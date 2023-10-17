package com.scalar.db.util;

import com.google.protobuf.ByteString;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteBuilder;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.ScanBuilder;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class ProtoUtils {
  private ProtoUtils() {}

  public static Get toGet(com.scalar.db.rpc.Get get, TableMetadata metadata) {
    GetBuilder.PartitionKeyOrIndexKey partitionKeyOrIndexKeyBuilder =
        Get.newBuilder().namespace(get.getNamespace()).table(get.getTable());

    Key partitionKeyOrIndexKey = toKey(get.getPartitionKey(), metadata);
    if (isIndexKey(partitionKeyOrIndexKey, metadata)) {
      return partitionKeyOrIndexKeyBuilder
          .indexKey(partitionKeyOrIndexKey)
          .consistency(toConsistency(get.getConsistency()))
          .projections(get.getProjectionsList())
          .build();
    } else {
      GetBuilder.BuildableGet buildableGet =
          partitionKeyOrIndexKeyBuilder.partitionKey(partitionKeyOrIndexKey);
      if (get.hasClusteringKey()) {
        buildableGet = buildableGet.clusteringKey(toKey(get.getClusteringKey(), metadata));
      }
      return buildableGet
          .consistency(toConsistency(get.getConsistency()))
          .projections(get.getProjectionsList())
          .build();
    }
  }

  public static com.scalar.db.rpc.Get toGet(Get get) {
    com.scalar.db.rpc.Get.Builder builder = com.scalar.db.rpc.Get.newBuilder();
    builder.setPartitionKey(toKey(get.getPartitionKey()));
    get.getClusteringKey().ifPresent(k -> builder.setClusteringKey(toKey(k)));
    get.forNamespace().ifPresent(builder::setNamespace);
    get.forTable().ifPresent(builder::setTable);
    return builder
        .setConsistency(toConsistency(get.getConsistency()))
        .addAllProjections(get.getProjections())
        .build();
  }

  private static Key toKey(com.scalar.db.rpc.Key key, TableMetadata metadata) {
    Key.Builder builder = Key.newBuilder();

    // For backward compatibility
    if (!key.getValueList().isEmpty()) {
      key.getValueList().forEach(v -> builder.add(toColumn(v.getName(), v)));
    } else {
      key.getColumnsList().forEach(c -> builder.add(toColumn(c, metadata)));
    }

    return builder.build();
  }

  private static com.scalar.db.rpc.Key toKey(Key key) {
    com.scalar.db.rpc.Key.Builder builder = com.scalar.db.rpc.Key.newBuilder();
    key.getColumns().forEach(c -> builder.addColumns(toColumn(c)));
    return builder.build();
  }

  private static Column<?> toColumn(com.scalar.db.rpc.Column column, TableMetadata metadata) {
    switch (column.getValueCase()) {
      case BOOLEAN_VALUE:
        return BooleanColumn.of(column.getName(), column.getBooleanValue());
      case INT_VALUE:
        return IntColumn.of(column.getName(), column.getIntValue());
      case BIGINT_VALUE:
        return BigIntColumn.of(column.getName(), column.getBigintValue());
      case FLOAT_VALUE:
        return FloatColumn.of(column.getName(), column.getFloatValue());
      case DOUBLE_VALUE:
        return DoubleColumn.of(column.getName(), column.getDoubleValue());
      case TEXT_VALUE:
        return TextColumn.of(column.getName(), column.getTextValue());
      case BLOB_VALUE:
        return BlobColumn.of(column.getName(), column.getBlobValue().toByteArray());
      case VALUE_NOT_SET:
        switch (metadata.getColumnDataType(column.getName())) {
          case BOOLEAN:
            return BooleanColumn.ofNull(column.getName());
          case INT:
            return IntColumn.ofNull(column.getName());
          case BIGINT:
            return BigIntColumn.ofNull(column.getName());
          case FLOAT:
            return FloatColumn.ofNull(column.getName());
          case DOUBLE:
            return DoubleColumn.ofNull(column.getName());
          case TEXT:
            return TextColumn.ofNull(column.getName());
          case BLOB:
            return BlobColumn.ofNull(column.getName());
          default:
            throw new AssertionError();
        }
      default:
        throw new AssertionError();
    }
  }

  private static com.scalar.db.rpc.Column toColumn(Column<?> column) {
    com.scalar.db.rpc.Column.Builder builder =
        com.scalar.db.rpc.Column.newBuilder().setName(column.getName());

    if (column.hasNullValue()) {
      return builder.build();
    }

    if (column instanceof BooleanColumn) {
      return builder.setBooleanValue(column.getBooleanValue()).build();
    } else if (column instanceof IntColumn) {
      return builder.setIntValue(column.getIntValue()).build();
    } else if (column instanceof BigIntColumn) {
      return builder.setBigintValue(column.getBigIntValue()).build();
    } else if (column instanceof FloatColumn) {
      return builder.setFloatValue(column.getFloatValue()).build();
    } else if (column instanceof DoubleColumn) {
      return builder.setDoubleValue(column.getDoubleValue()).build();
    } else if (column instanceof TextColumn) {
      assert column.getTextValue() != null;
      return builder.setTextValue(column.getTextValue()).build();
    } else if (column instanceof BlobColumn) {
      assert column.getBlobValueAsBytes() != null;
      return builder.setBlobValue(ByteString.copyFrom(column.getBlobValueAsBytes())).build();
    } else {
      throw new AssertionError();
    }
  }

  /**
   * This method is for backward compatibility.
   *
   * @param columnName a column name
   * @param value a value
   * @return a converted column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  private static Column<?> toColumn(String columnName, com.scalar.db.rpc.Value value) {
    switch (value.getValueCase()) {
      case BOOLEAN_VALUE:
        return BooleanColumn.of(columnName, value.getBooleanValue());
      case INT_VALUE:
        return IntColumn.of(columnName, value.getIntValue());
      case BIGINT_VALUE:
        return BigIntColumn.of(columnName, value.getBigintValue());
      case FLOAT_VALUE:
        return FloatColumn.of(columnName, value.getFloatValue());
      case DOUBLE_VALUE:
        return DoubleColumn.of(columnName, value.getDoubleValue());
      case TEXT_VALUE:
        if (value.getTextValue().hasValue()) {
          return TextColumn.of(columnName, value.getTextValue().getValue());
        } else {
          return TextColumn.ofNull(columnName);
        }
      case BLOB_VALUE:
        if (value.getBlobValue().hasValue()) {
          return BlobColumn.of(columnName, value.getBlobValue().getValue().toByteArray());
        } else {
          return BlobColumn.ofNull(columnName);
        }
      case VALUE_NOT_SET:
        throw new IllegalArgumentException("The value is not set");
      default:
        throw new AssertionError();
    }
  }

  /**
   * This method is for backward compatibility.
   *
   * @param column a column
   * @return a converted value
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  private static com.scalar.db.rpc.Value toValue(Column<?> column) {
    com.scalar.db.rpc.Value.Builder builder =
        com.scalar.db.rpc.Value.newBuilder().setName(column.getName());

    if (column instanceof BooleanColumn) {
      return builder.setBooleanValue(column.getBooleanValue()).build();
    } else if (column instanceof IntColumn) {
      return builder.setIntValue(column.getIntValue()).build();
    } else if (column instanceof BigIntColumn) {
      return builder.setBigintValue(column.getBigIntValue()).build();
    } else if (column instanceof FloatColumn) {
      return builder.setFloatValue(column.getFloatValue()).build();
    } else if (column instanceof DoubleColumn) {
      return builder.setDoubleValue(column.getDoubleValue()).build();
    } else if (column instanceof TextColumn) {
      com.scalar.db.rpc.Value.TextValue.Builder textValueBuilder =
          com.scalar.db.rpc.Value.TextValue.newBuilder();
      if (!column.hasNullValue()) {
        assert column.getTextValue() != null;
        textValueBuilder.setValue(column.getTextValue());
      }
      return builder.setTextValue(textValueBuilder.build()).build();
    } else if (column instanceof BlobColumn) {
      com.scalar.db.rpc.Value.BlobValue.Builder blobValueBuilder =
          com.scalar.db.rpc.Value.BlobValue.newBuilder();
      if (!column.hasNullValue()) {
        assert column.getBlobValueAsBytes() != null;
        blobValueBuilder.setValue(ByteString.copyFrom(column.getBlobValueAsBytes()));
      }
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
      case UNRECOGNIZED:
        throw new IllegalArgumentException("The consistency is not set");
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

  public static Scan toScan(com.scalar.db.rpc.Scan scan, TableMetadata metadata) {
    ScanBuilder.PartitionKeyOrIndexKeyOrAll partitionKeyOrIndexKeyOrAll =
        Scan.newBuilder().namespace(scan.getNamespace()).table(scan.getTable());

    if (scan.hasPartitionKey()) {
      Key partitionKeyOrIndexKey = toKey(scan.getPartitionKey(), metadata);
      if (isIndexKey(partitionKeyOrIndexKey, metadata)) {
        return partitionKeyOrIndexKeyOrAll
            .indexKey(partitionKeyOrIndexKey)
            .limit(scan.getLimit())
            .consistency(toConsistency(scan.getConsistency()))
            .projections(scan.getProjectionsList())
            .build();
      } else {
        ScanBuilder.BuildableScan buildableScan =
            partitionKeyOrIndexKeyOrAll.partitionKey(partitionKeyOrIndexKey);
        if (scan.hasStartClusteringKey()) {
          buildableScan =
              buildableScan.start(
                  toKey(scan.getStartClusteringKey(), metadata), scan.getStartInclusive());
        }
        if (scan.hasEndClusteringKey()) {
          buildableScan =
              buildableScan.end(
                  toKey(scan.getEndClusteringKey(), metadata), scan.getEndInclusive());
        }
        for (com.scalar.db.rpc.Ordering ordering : scan.getOrderingsList()) {
          buildableScan = buildableScan.ordering(toOrdering(ordering));
        }
        return buildableScan
            .limit(scan.getLimit())
            .consistency(toConsistency(scan.getConsistency()))
            .projections(scan.getProjectionsList())
            .build();
      }
    } else {
      return partitionKeyOrIndexKeyOrAll
          .all()
          .limit(scan.getLimit())
          .consistency(toConsistency(scan.getConsistency()))
          .projections(scan.getProjectionsList())
          .build();
    }
  }

  private static boolean isIndexKey(Key key, TableMetadata metadata) {
    List<Column<?>> columns = key.getColumns();
    if (columns.size() == 1) {
      String name = columns.get(0).getName();
      return metadata.getSecondaryIndexNames().contains(name);
    }
    return false;
  }

  public static com.scalar.db.rpc.Scan toScan(Scan scan) {
    com.scalar.db.rpc.Scan.Builder builder = com.scalar.db.rpc.Scan.newBuilder();

    if (!(scan instanceof ScanAll)) {
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
      scan.getOrderings().forEach(o -> builder.addOrderings(toOrdering(o)));
    }

    builder.setLimit(scan.getLimit());
    scan.forNamespace().ifPresent(builder::setNamespace);
    scan.forTable().ifPresent(builder::setTable);
    return builder
        .setConsistency(toConsistency(scan.getConsistency()))
        .addAllProjections(scan.getProjections())
        .build();
  }

  private static Scan.Ordering toOrdering(com.scalar.db.rpc.Ordering ordering) {
    switch (ordering.getOrder()) {
      case ORDER_ASC:
        return Scan.Ordering.asc(ordering.getName());
      case ORDER_DESC:
        return Scan.Ordering.desc(ordering.getName());
      case UNRECOGNIZED:
        throw new IllegalArgumentException("The order is unrecognized");
      default:
        throw new AssertionError();
    }
  }

  private static com.scalar.db.rpc.Ordering toOrdering(Scan.Ordering ordering) {
    return com.scalar.db.rpc.Ordering.newBuilder()
        .setName(ordering.getColumnName())
        .setOrder(toOrder(ordering.getOrder()))
        .build();
  }

  private static Scan.Ordering.Order toOrder(com.scalar.db.rpc.Order order) {
    switch (order) {
      case ORDER_ASC:
        return Scan.Ordering.Order.ASC;
      case ORDER_DESC:
        return Scan.Ordering.Order.DESC;
      case UNRECOGNIZED:
        throw new IllegalArgumentException("The order is unrecognized");
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

  public static Mutation toMutation(com.scalar.db.rpc.Mutation mutation, TableMetadata metadata) {
    Key partitionKey = toKey(mutation.getPartitionKey(), metadata);
    Key clusteringKey;
    if (mutation.hasClusteringKey()) {
      clusteringKey = toKey(mutation.getClusteringKey(), metadata);
    } else {
      clusteringKey = null;
    }

    if (mutation.getType() == com.scalar.db.rpc.Mutation.Type.PUT) {
      PutBuilder.Buildable buildable =
          Put.newBuilder()
              .namespace(mutation.getNamespace())
              .table(mutation.getTable())
              .partitionKey(partitionKey);
      if (mutation.hasClusteringKey()) {
        buildable.clusteringKey(clusteringKey);
      }

      // For backward compatibility
      if (!mutation.getValueList().isEmpty()) {
        mutation.getValueList().forEach(v -> buildable.value(toColumn(v.getName(), v)));
      } else {
        mutation.getColumnsList().forEach(c -> buildable.value(toColumn(c, metadata)));
      }

      if (mutation.hasCondition()) {
        buildable.condition(toCondition(mutation.getCondition(), metadata));
      }
      return buildable
          .consistency(toConsistency(mutation.getConsistency()))
          .blind(mutation.getBlind())
          .build();
    } else {
      DeleteBuilder.Buildable buildable =
          Delete.newBuilder()
              .namespace(mutation.getNamespace())
              .table(mutation.getTable())
              .partitionKey(partitionKey);
      if (mutation.hasClusteringKey()) {
        buildable.clusteringKey(clusteringKey);
      }
      if (mutation.hasCondition()) {
        buildable.condition(toCondition(mutation.getCondition(), metadata));
      }
      return buildable.consistency(toConsistency(mutation.getConsistency())).build();
    }
  }

  public static com.scalar.db.rpc.Mutation toMutation(Mutation mutation) {
    com.scalar.db.rpc.Mutation.Builder builder =
        com.scalar.db.rpc.Mutation.newBuilder().setPartitionKey(toKey(mutation.getPartitionKey()));
    mutation.getClusteringKey().ifPresent(k -> builder.setClusteringKey(toKey(k)));
    if (mutation instanceof Put) {
      builder.setType(com.scalar.db.rpc.Mutation.Type.PUT);
      ((Put) mutation).getColumns().values().forEach(c -> builder.addColumns(toColumn(c)));
      builder.setBlind(((Put) mutation).isBlind());
    } else {
      builder.setType(com.scalar.db.rpc.Mutation.Type.DELETE);
    }
    mutation.forNamespace().ifPresent(builder::setNamespace);
    mutation.forTable().ifPresent(builder::setTable);
    builder.setConsistency(toConsistency(mutation.getConsistency()));
    mutation.getCondition().ifPresent(c -> builder.setCondition(toCondition(c)));
    return builder.build();
  }

  private static MutationCondition toCondition(
      com.scalar.db.rpc.MutateCondition condition, TableMetadata metadata) {
    switch (condition.getType()) {
      case PUT_IF:
        return ConditionBuilder.putIf(
            condition.getExpressionsList().stream()
                .map(e -> toExpression(e, metadata))
                .collect(Collectors.toList()));
      case PUT_IF_EXISTS:
        return ConditionBuilder.putIfExists();
      case PUT_IF_NOT_EXISTS:
        return ConditionBuilder.putIfNotExists();
      case DELETE_IF:
        return ConditionBuilder.deleteIf(
            condition.getExpressionsList().stream()
                .map(e -> toExpression(e, metadata))
                .collect(Collectors.toList()));
      case DELETE_IF_EXISTS:
        return ConditionBuilder.deleteIfExists();
      case UNRECOGNIZED:
        throw new IllegalArgumentException("The condition type is unrecognized");
      default:
        throw new AssertionError();
    }
  }

  private static com.scalar.db.rpc.MutateCondition toCondition(MutationCondition condition) {
    if (condition instanceof PutIf) {
      com.scalar.db.rpc.MutateCondition.Builder builder =
          com.scalar.db.rpc.MutateCondition.newBuilder()
              .setType(com.scalar.db.rpc.MutateCondition.Type.PUT_IF);
      condition.getExpressions().forEach(e -> builder.addExpressions(toExpression(e)));
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
      com.scalar.db.rpc.MutateCondition.Builder builder =
          com.scalar.db.rpc.MutateCondition.newBuilder()
              .setType(com.scalar.db.rpc.MutateCondition.Type.DELETE_IF);
      condition.getExpressions().forEach(e -> builder.addExpressions(toExpression(e)));
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
      com.scalar.db.rpc.ConditionalExpression expression, TableMetadata metadata) {
    // For backward compatibility
    if (expression.hasValue()) {
      return ConditionBuilder.buildConditionalExpression(
          toColumn(expression.getName(), expression.getValue()),
          toOperator(expression.getOperator()));
    } else {
      return ConditionBuilder.buildConditionalExpression(
          toColumn(expression.getColumn(), metadata), toOperator(expression.getOperator()));
    }
  }

  private static com.scalar.db.rpc.ConditionalExpression toExpression(
      ConditionalExpression expression) {
    return com.scalar.db.rpc.ConditionalExpression.newBuilder()
        .setColumn(toColumn(expression.getColumn()))
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
      case IS_NULL:
        return ConditionalExpression.Operator.IS_NULL;
      case IS_NOT_NULL:
        return ConditionalExpression.Operator.IS_NOT_NULL;
      case UNRECOGNIZED:
        throw new IllegalArgumentException("The condition operator is unrecognized");
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
      case IS_NULL:
        return com.scalar.db.rpc.ConditionalExpression.Operator.IS_NULL;
      case IS_NOT_NULL:
        return com.scalar.db.rpc.ConditionalExpression.Operator.IS_NOT_NULL;
      default:
        throw new AssertionError();
    }
  }

  public static Result toResult(com.scalar.db.rpc.Result result, TableMetadata metadata) {
    Map<String, Column<?>> columns =
        result.getColumnsList().stream()
            .collect(
                Collectors.toMap(com.scalar.db.rpc.Column::getName, c -> toColumn(c, metadata)));
    return new ResultImpl(columns, metadata);
  }

  public static com.scalar.db.rpc.Result toResult(Result result) {
    com.scalar.db.rpc.Result.Builder builder = com.scalar.db.rpc.Result.newBuilder();
    result.getColumns().values().forEach(c -> builder.addColumns(toColumn(c)));
    return builder.build();
  }

  /**
   * This method is for backward compatibility.
   *
   * @param get a get
   * @return whether the request is from client or not
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public static boolean isRequestFromOldClient(com.scalar.db.rpc.Get get) {
    // If the partition key of the get has "Value", then it's from an old client
    return !get.getPartitionKey().getValueList().isEmpty();
  }

  /**
   * This method is for backward compatibility.
   *
   * @param scan a scan
   * @return whether the request is from client or not
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public static boolean isRequestFromOldClient(com.scalar.db.rpc.Scan scan) {
    if (!scan.hasPartitionKey()) {
      // If the scan doesn't have partition key, it indicates ScanAll operation, then it's from a
      // new client
      return false;
    }

    // If the partition key of the scan has "Value", then it's from an old client
    return !scan.getPartitionKey().getValueList().isEmpty();
  }

  /**
   * This method is for backward compatibility.
   *
   * @param result a result
   * @return a converted result
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public static com.scalar.db.rpc.Result toResultWithValue(Result result) {
    com.scalar.db.rpc.Result.Builder builder = com.scalar.db.rpc.Result.newBuilder();
    result.getColumns().values().forEach(c -> builder.addValue(toValue(c)));
    return builder.build();
  }

  public static TableMetadata toTableMetadata(com.scalar.db.rpc.TableMetadata tableMetadata) {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    tableMetadata.getColumnsMap().forEach((n, t) -> builder.addColumn(n, toDataType(t)));
    tableMetadata.getPartitionKeyNamesList().forEach(builder::addPartitionKey);
    Map<String, com.scalar.db.rpc.Order> clusteringOrderMap =
        tableMetadata.getClusteringOrdersMap();
    tableMetadata
        .getClusteringKeyNamesList()
        .forEach(n -> builder.addClusteringKey(n, toOrder(clusteringOrderMap.get(n))));
    tableMetadata.getSecondaryIndexNamesList().forEach(builder::addSecondaryIndex);
    return builder.build();
  }

  public static com.scalar.db.rpc.TableMetadata toTableMetadata(TableMetadata tableMetadata) {
    com.scalar.db.rpc.TableMetadata.Builder builder = com.scalar.db.rpc.TableMetadata.newBuilder();
    tableMetadata
        .getColumnNames()
        .forEach(n -> builder.putColumns(n, toDataType(tableMetadata.getColumnDataType(n))));
    tableMetadata.getPartitionKeyNames().forEach(builder::addPartitionKeyNames);
    tableMetadata
        .getClusteringKeyNames()
        .forEach(
            n -> {
              builder.addClusteringKeyNames(n);
              builder.putClusteringOrders(n, toOrder(tableMetadata.getClusteringOrder(n)));
            });
    tableMetadata.getSecondaryIndexNames().forEach(builder::addSecondaryIndexNames);
    return builder.build();
  }

  public static DataType toDataType(com.scalar.db.rpc.DataType dataType) {
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
      case UNRECOGNIZED:
        throw new IllegalArgumentException("The data type is unrecognized");
      default:
        throw new AssertionError();
    }
  }

  public static com.scalar.db.rpc.DataType toDataType(DataType dataType) {
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
        return com.scalar.db.rpc.TransactionState.TRANSACTION_STATE_UNKNOWN;
      default:
        throw new AssertionError();
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
      case UNRECOGNIZED:
        throw new IllegalArgumentException("The transaction state is unrecognized");
      default:
        throw new AssertionError();
    }
  }
}
