package com.scalar.db.graphql.instrumentation.validation;

import com.google.common.collect.Sets;
import com.scalar.db.graphql.schema.Constants;
import com.scalar.db.graphql.schema.DeleteConditionType;
import com.scalar.db.graphql.schema.PutConditionType;
import graphql.ErrorType;
import graphql.GraphQLError;
import graphql.GraphqlErrorBuilder;
import graphql.execution.instrumentation.fieldvalidation.FieldAndArguments;
import graphql.execution.instrumentation.fieldvalidation.FieldValidation;
import graphql.execution.instrumentation.fieldvalidation.FieldValidationEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ConditionalExpressionValidation implements FieldValidation {
  @Override
  public List<GraphQLError> validateFields(FieldValidationEnvironment validationEnvironment) {
    List<GraphQLError> errors = new ArrayList<>();
    for (FieldAndArguments fieldAndArguments : validationEnvironment.getFields()) {
      String fieldName = fieldAndArguments.getField().getName();
      if (fieldName.endsWith("_put")) {
        validatePut(fieldAndArguments.getArgumentValue("put"), fieldAndArguments)
            .ifPresent(errors::add);
      } else if (fieldName.endsWith("_delete")) {
        validateDelete(fieldAndArguments.getArgumentValue("delete"), fieldAndArguments)
            .ifPresent(errors::add);
      } else if (fieldName.endsWith("_bulkPut")) {
        List<Map<String, Map<String, Object>>> puts = fieldAndArguments.getArgumentValue("put");
        puts.forEach(put -> validatePut(put, fieldAndArguments).ifPresent(errors::add));
      } else if (fieldName.endsWith("_bulkDelete")) {
        List<Map<String, Map<String, Object>>> deletes =
            fieldAndArguments.getArgumentValue("delete");
        deletes.forEach(delete -> validateDelete(delete, fieldAndArguments).ifPresent(errors::add));
      } else if (fieldName.endsWith("_mutate")) {
        List<Map<String, Map<String, Object>>> puts = fieldAndArguments.getArgumentValue("put");
        if (puts != null) {
          puts.forEach(put -> validatePut(put, fieldAndArguments).ifPresent(errors::add));
        }
        List<Map<String, Map<String, Object>>> deletes =
            fieldAndArguments.getArgumentValue("delete");
        if (deletes != null) {
          deletes.forEach(
              delete -> validateDelete(delete, fieldAndArguments).ifPresent(errors::add));
        }
      }
    }

    return errors;
  }

  @SuppressWarnings("unchecked")
  private Optional<GraphQLError> validatePut(
      Map<String, Map<String, Object>> put, FieldAndArguments fieldAndArguments) {
    Map<String, Object> condition = put.get("condition");
    if (condition == null) {
      return Optional.empty();
    }
    String type = (String) condition.get("type");
    List<Map<String, Object>> expressions =
        (List<Map<String, Object>>) condition.get("expressions");
    switch (PutConditionType.valueOf(type)) {
      case PutIf:
        return validateIfConditionType(type, expressions, fieldAndArguments);
      case PutIfExists:
      case PutIfNotExists:
        return validateIfExistsConditionType(type, expressions, fieldAndArguments);
    }
    return Optional.empty();
  }

  @SuppressWarnings("unchecked")
  private Optional<GraphQLError> validateDelete(
      Map<String, Map<String, Object>> delete, FieldAndArguments fieldAndArguments) {
    Map<String, Object> condition = delete.get("condition");
    if (condition == null) {
      return Optional.empty();
    }
    String type = (String) condition.get("type");
    List<Map<String, Object>> expressions =
        (List<Map<String, Object>>) condition.get("expressions");
    switch (DeleteConditionType.valueOf(type)) {
      case DeleteIf:
        return validateIfConditionType(type, expressions, fieldAndArguments);
      case DeleteIfExists:
        return validateIfExistsConditionType(type, expressions, fieldAndArguments);
    }
    return Optional.empty();
  }

  private Optional<GraphQLError> validateIfConditionType(
      String type, List<Map<String, Object>> expressions, FieldAndArguments fieldAndArguments) {
    if (expressions == null || expressions.isEmpty()) {
      return Optional.of(
          createError(
              "expressions must be present for " + type + " condition type", fieldAndArguments));
    } else if (expressions.stream()
        .anyMatch(ex -> Sets.intersection(ex.keySet(), Constants.SCALAR_VALUE_KEYS).size() != 1)) {
      return Optional.of(
          createError(
              "expression must have only one of " + Constants.SCALAR_VALUE_KEYS,
              fieldAndArguments));
    }
    return Optional.empty();
  }

  private Optional<GraphQLError> validateIfExistsConditionType(
      String type, List<Map<String, Object>> expressions, FieldAndArguments fieldAndArguments) {
    if (expressions != null) {
      return Optional.of(
          createError(
              "expressions must not be present for " + type + " condition type",
              fieldAndArguments));
    }
    return Optional.empty();
  }

  private GraphQLError createError(String message, FieldAndArguments fieldAndArguments) {
    return GraphqlErrorBuilder.newError()
        .message(message)
        .errorType(ErrorType.ValidationError)
        .path(fieldAndArguments.getPath())
        .location(fieldAndArguments.getField().getSourceLocation())
        .build();
  }
}
