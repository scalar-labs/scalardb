package com.scalar.db.graphql.datafetcher;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.graphql.GraphQlConstants;
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
import graphql.execution.AbortExecutionException;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

final class DataFetcherUtils {
  private DataFetcherUtils() {}

  static DistributedTransaction getCurrentTransaction(DataFetchingEnvironment environment) {
    return environment.getGraphQlContext().get(GraphQlConstants.CONTEXT_TRANSACTION_KEY);
  }

  static Key createPartitionKeyFromKeyArgument(
      TableGraphQlModel tableGraphQlModel, Map<String, Object> keyArg) {
    List<Value<?>> values =
        tableGraphQlModel.getPartitionKeyNames().stream()
            .map(name -> createValue(tableGraphQlModel, name, keyArg.get(name)))
            .collect(Collectors.toList());
    return new Key(values);
  }

  static Key createClusteringKeyFromKeyArgument(
      TableGraphQlModel tableGraphQlModel, Map<String, Object> keyArg) {
    List<Value<?>> values =
        tableGraphQlModel.getClusteringKeyNames().stream()
            .map(name -> createValue(tableGraphQlModel, name, keyArg.get(name)))
            .collect(Collectors.toList());
    return new Key(values);
  }

  private static Value<?> createValue(
      TableGraphQlModel tableGraphQlModel, String columnName, Object inputValue) {
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

  static Collection<String> getProjections(DataFetchingEnvironment environment) {
    // Add specified field names of the get or scan GraphQL input to the Selection object as
    // projections. The fields are filtered by "*/*" since they are represented in the following
    // format:
    // <object_type_name>_(Get|Scan)PayLoad.<object_type_name>/<object_type_name>.<field_name>
    return environment.getSelectionSet().getFields("*/*").stream()
        .map(SelectedField::getName)
        .collect(Collectors.toList());
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
    Set<String> valueKeys =
        Sets.intersection(map.keySet(), GraphQlConstants.SCALAR_VALUE_FIELD_NAMES);
    if (valueKeys.size() != 1) {
      throw new IllegalArgumentException(
          "One and only one of "
              + GraphQlConstants.SCALAR_VALUE_FIELD_NAMES
              + " must be specified.");
    }
    return map.get(valueKeys.stream().findFirst().get());
  }

  static List<ConditionalExpression> createConditionalExpressions(
      List<Map<String, Object>> graphQlConditionalExpressions) {
    // Create a list of Scalar DB ConditionalExpression objects from a list of ConditionalExpression
    // in GraphQL request.
    return graphQlConditionalExpressions.stream()
        .map(
            ex ->
                new ConditionalExpression(
                    (String) ex.get("name"),
                    createValueFromMap("", ex),
                    ConditionalExpression.Operator.valueOf((String) ex.get("operator"))))
        .collect(Collectors.toList());
  }

  static GraphQLError createGraphQLError(Exception exception, DataFetchingEnvironment environment) {
    String exName = exception.getClass().getSimpleName();
    return GraphqlErrorBuilder.newError(environment)
        .message("Scalar DB %s happened while fetching data: %s", exName, exception.getMessage())
        .extensions(ImmutableMap.of("exception", exName))
        .errorType(ErrorType.DataFetchingException)
        .build();
  }

  static void failIfConsensusCommitTransactionalTable(TableGraphQlModel tableGraphQlModel) {
    if (tableGraphQlModel.isConsensusCommitTransactionalTable()) {
      throw new AbortExecutionException(
          "the table "
              + tableGraphQlModel.getTableName()
              + " has transactional meta columns, but being accessed with the storage method");
    }
  }
}
