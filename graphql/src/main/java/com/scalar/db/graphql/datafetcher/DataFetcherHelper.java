package com.scalar.db.graphql.datafetcher;

import static java.util.stream.Collectors.toList;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Selection;
import com.scalar.db.graphql.schema.CommonSchema;
import com.scalar.db.graphql.schema.Constants;
import com.scalar.db.graphql.schema.DeleteConditionType;
import com.scalar.db.graphql.schema.PutConditionType;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import graphql.ErrorType;
import graphql.GraphQLError;
import graphql.GraphqlErrorBuilder;
import graphql.Scalars;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLScalarType;
import graphql.schema.SelectedField;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataFetcherHelper {
  private final TableGraphQlModel tableGraphQlModel;

  public DataFetcherHelper(TableGraphQlModel tableGraphQlModel) {
    this.tableGraphQlModel = tableGraphQlModel;
  }

  static DistributedTransaction getCurrentTransaction(DataFetchingEnvironment environment) {
    return environment.getGraphQlContext().get(Constants.CONTEXT_TRANSACTION_KEY);
  }

  static Value<?> createValueFromMap(String name, Map<String, Object> map) {
    Object v = getOneScalarValue(map);
    Value<?> value;
    if (v instanceof Integer) {
      value = new IntValue(name, (Integer) v);
    } else if (v instanceof Long) {
      value = new BigIntValue(name, (Long) v);
    } else if (v instanceof Float) {
      value = new FloatValue(name, (Float) v);
    } else if (v instanceof Double) {
      value = new DoubleValue(name, (Double) v);
    } else if (v instanceof String) {
      value = new TextValue(name, (String) v);
    } else if (v instanceof Boolean) {
      value = new BooleanValue(name, (Boolean) v);
    } else {
      throw new IllegalArgumentException("Unexpected value: " + v.getClass());
    }
    return value;
  }

  private static Object getOneScalarValue(Map<String, Object> map) {
    List<?> values =
        Stream.of(
                (Integer) map.get("intValue"),
                (Long) map.get("bigIntValue"),
                (Float) map.get("floatValue"),
                (Double) map.get("doubleValue"),
                (String) map.get("textValue"),
                (Boolean) map.get("booleanValue"))
            .filter(Objects::nonNull)
            .collect(toList());
    if (values.size() != 1) {
      throw new IllegalArgumentException(
          "One and only one of intValue, bigIntValue, floatValue, doubleValue, textValue, and booleanValue must be specified.");
    }
    return values.get(0);
  }

  static GraphQLError getGraphQLError(Exception exception, DataFetchingEnvironment environment) {
    String exName = exception.getClass().getSimpleName();
    return GraphqlErrorBuilder.newError(environment)
        .message("Scalar DB %s happened while fetching data: %s", exName, exception.getMessage())
        .extensions(ImmutableMap.of(Constants.ERRORS_EXTENSIONS_EXCEPTION_KEY, exName))
        .errorType(ErrorType.DataFetchingException)
        .build();
  }

  Key createPartitionKeyFromKeyArgument(Map<String, Object> keyArg) {
    return createKeyFromKeyNames(keyArg, tableGraphQlModel.getPartitionKeyNames());
  }

  Key createClusteringKeyFromKeyArgument(Map<String, Object> keyArg) {
    return createKeyFromKeyNames(keyArg, tableGraphQlModel.getClusteringKeyNames());
  }

  private Key createKeyFromKeyNames(Map<String, Object> keyArg, LinkedHashSet<String> keyNames) {
    List<Value<?>> keyValues =
        keyNames.stream().map(name -> createValue(name, keyArg.get(name))).collect(toList());
    return new Key(keyValues);
  }

  private Value<?> createValue(String columnName, Object inputValue) {
    switch (tableGraphQlModel.getColumnDataType(columnName)) {
      case BOOLEAN:
        return new BooleanValue(columnName, (Boolean) inputValue);
      case INT:
        return new IntValue(columnName, (Integer) inputValue);
      case BIGINT:
        return new BigIntValue(columnName, (Long) inputValue);
      case FLOAT:
        return new FloatValue(columnName, (Float) inputValue);
      case DOUBLE:
        return new DoubleValue(columnName, (Double) inputValue);
      case TEXT:
        return new TextValue(columnName, (String) inputValue);
      case BLOB:
      default:
        throw new IllegalArgumentException("Invalid column");
    }
  }

  /**
   * Create a list of Scalar DB {@link ConditionalExpression} objects from a list of
   * ConditionalExpression in GraphQL request.
   *
   * @param graphQlConditionalExpressions expressions field input in PutCondition or DeleteCondition
   * @return A list of {@link ConditionalExpression}
   */
  private List<ConditionalExpression> createConditionalExpressions(
      List<Map<String, Object>> graphQlConditionalExpressions) {
    return graphQlConditionalExpressions.stream()
        .map(
            ex ->
                new ConditionalExpression(
                    (String) ex.get("name"),
                    createValueFromMap("", ex),
                    ConditionalExpression.Operator.valueOf((String) ex.get("operator"))))
        .collect(toList());
  }

  @SuppressWarnings("unchecked")
  Put createPut(Map<String, Object> putInput) {
    Map<String, Object> keyArg = (Map<String, Object>) putInput.get("key"); // table_Key
    Map<String, Object> valuesArg = (Map<String, Object>) putInput.get("values"); // table_PutValues
    Map<String, Object> conditionArg =
        (Map<String, Object>) putInput.get("condition"); // PutCondition

    Key partitionKey = createPartitionKeyFromKeyArgument(keyArg);
    Key clusteringKey = createClusteringKeyFromKeyArgument(keyArg);
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(tableGraphQlModel.getNamespaceName())
            .forTable(tableGraphQlModel.getTableName());
    String consistency = (String) putInput.get("consistency");
    if (consistency != null) {
      put.withConsistency(Consistency.valueOf(consistency));
    }

    Map<String, GraphQLScalarType> inputNameToGraphQLScalarTypeMap =
        tableGraphQlModel.getPutValuesObjectType().getFields().stream()
            .collect(
                Collectors.toMap(
                    GraphQLInputObjectField::getName, f -> (GraphQLScalarType) f.getType()));

    valuesArg.forEach(
        (name, value) -> {
          GraphQLScalarType graphQLScalarType = inputNameToGraphQLScalarTypeMap.get(name);
          if (Scalars.GraphQLInt.equals(graphQLScalarType)) {
            put.withValue(name, (Integer) value);
          } else if (CommonSchema.BIG_INT_SCALAR.equals(graphQLScalarType)) {
            put.withValue(name, (Long) value);
          } else if (CommonSchema.FLOAT_32_SCALAR.equals(graphQLScalarType)) {
            put.withValue(name, (Float) value);
          } else if (Scalars.GraphQLFloat.equals(graphQLScalarType)) {
            put.withValue(name, (Double) value);
          } else if (Scalars.GraphQLString.equals(graphQLScalarType)) {
            put.withValue(name, (String) value);
          } else if (Scalars.GraphQLBoolean.equals(graphQLScalarType)) {
            put.withValue(name, (Boolean) value);
          } else {
            throw new IllegalArgumentException("Invalid type");
          }
        });

    if (conditionArg != null) {
      PutConditionType putConditionType =
          PutConditionType.valueOf((String) conditionArg.get("type"));
      switch (putConditionType) {
        case PutIf:
          List<Map<String, Object>> expressionsArg =
              (List<Map<String, Object>>) conditionArg.get("expressions");
          if (expressionsArg == null || expressionsArg.isEmpty()) {
            throw new IllegalArgumentException("Empty expressions passed to PutIf condition");
          }
          put.withCondition(new PutIf(createConditionalExpressions(expressionsArg)));
          break;
        case PutIfExists:
          put.withCondition(new PutIfExists());
          break;
        case PutIfNotExists:
          put.withCondition(new PutIfNotExists());
          break;
      }
    }
    return put;
  }

  @SuppressWarnings("unchecked")
  Delete createDelete(Map<String, Object> deleteInput) {
    Map<String, Object> keyArg = (Map<String, Object>) deleteInput.get("key"); // table_Key
    Map<String, Object> conditionArg =
        (Map<String, Object>) deleteInput.get("condition"); // DeleteCondition

    Key partitionKey = createPartitionKeyFromKeyArgument(keyArg);
    Key clusteringKey = createClusteringKeyFromKeyArgument(keyArg);
    Delete delete =
        new Delete(partitionKey, clusteringKey)
            .forNamespace(tableGraphQlModel.getNamespaceName())
            .forTable(tableGraphQlModel.getTableName());
    String consistency = (String) deleteInput.get("consistency");
    if (consistency != null) {
      delete.withConsistency(Consistency.valueOf(consistency));
    }

    if (conditionArg != null) {
      DeleteConditionType deleteConditionType =
          DeleteConditionType.valueOf((String) conditionArg.get("type"));
      switch (deleteConditionType) {
        case DeleteIf:
          List<Map<String, Object>> expressionsArg =
              (List<Map<String, Object>>) conditionArg.get("expressions");
          if (expressionsArg == null || expressionsArg.isEmpty()) {
            throw new IllegalArgumentException("Empty expressions passed to PutIf condition");
          }
          delete.withCondition(new DeleteIf(createConditionalExpressions(expressionsArg)));
          break;
        case DeleteIfExists:
          delete.withCondition(new DeleteIfExists());
          break;
      }
    }
    return delete;
  }

  void addProjections(Selection selection, DataFetchingEnvironment environment) {
    // Add specified field names of the get or scan GraphQL input to the Selection object as
    // projections. The fields are filtered by "*/*" since they are represented in the following
    // format:
    // <object_type_name>_(Get|Scan)PayLoad.<object_type_name>/<object_type_name>.<field_name>
    List<String> selectedFieldNames =
        environment.getSelectionSet().getFields("*/*").stream()
            .map(SelectedField::getName)
            .collect(Collectors.toList());
    selection.withProjections(selectedFieldNames);
  }

  String getNamespaceName() {
    return tableGraphQlModel.getNamespaceName();
  }

  String getTableName() {
    return tableGraphQlModel.getTableName();
  }

  String getObjectTypeName() {
    return tableGraphQlModel.getObjectType().getName();
  }
}
