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

public class ConditionalExpressionValidation implements FieldValidation {
  @Override
  public List<GraphQLError> validateFields(FieldValidationEnvironment validationEnvironment) {
    List<GraphQLError> errors = new ArrayList<>();
    for (FieldAndArguments fieldAndArguments : validationEnvironment.getFields()) {
      String fieldName = fieldAndArguments.getField().getName();
      if (fieldName.endsWith("_put")) {
        validatePut(errors, fieldAndArguments.getArgumentValue("put"), fieldAndArguments);
      } else if (fieldName.endsWith("_delete")) {
        validateDelete(errors, fieldAndArguments.getArgumentValue("delete"), fieldAndArguments);
      } else if (fieldName.endsWith("_bulkPut")) {
        List<Map<String, Map<String, Object>>> puts = fieldAndArguments.getArgumentValue("put");
        puts.forEach(put -> validatePut(errors, put, fieldAndArguments));
      } else if (fieldName.endsWith("_bulkDelete")) {
        List<Map<String, Map<String, Object>>> deletes =
            fieldAndArguments.getArgumentValue("delete");
        deletes.forEach(delete -> validateDelete(errors, delete, fieldAndArguments));
      } else if (fieldName.endsWith("_mutate")) {
        List<Map<String, Map<String, Object>>> puts = fieldAndArguments.getArgumentValue("put");
        if (puts != null) {
          puts.forEach(put -> validatePut(errors, put, fieldAndArguments));
        }
        List<Map<String, Map<String, Object>>> deletes =
            fieldAndArguments.getArgumentValue("delete");
        if (deletes != null) {
          deletes.forEach(delete -> validateDelete(errors, delete, fieldAndArguments));
        }
      }
    }

    return errors;
  }

  @SuppressWarnings("unchecked")
  private void validatePut(
      List<GraphQLError> errors,
      Map<String, Map<String, Object>> put,
      FieldAndArguments fieldAndArguments) {
    Map<String, Object> condition = put.get("condition");
    if (condition == null) {
      return;
    }
    String type = (String) condition.get("type");
    List<Map<String, Object>> expressions =
        (List<Map<String, Object>>) condition.get("expressions");
    switch (PutConditionType.valueOf(type)) {
      case PutIf:
        validateIfConditionType(errors, type, expressions, fieldAndArguments);
        break;
      case PutIfExists:
      case PutIfNotExists:
        validateIfExistsConditionType(errors, type, expressions, fieldAndArguments);
        break;
    }
  }

  @SuppressWarnings("unchecked")
  private void validateDelete(
      List<GraphQLError> errors,
      Map<String, Map<String, Object>> delete,
      FieldAndArguments fieldAndArguments) {
    Map<String, Object> condition = delete.get("condition");
    if (condition == null) {
      return;
    }
    String type = (String) condition.get("type");
    List<Map<String, Object>> expressions =
        (List<Map<String, Object>>) condition.get("expressions");
    switch (DeleteConditionType.valueOf(type)) {
      case DeleteIf:
        validateIfConditionType(errors, type, expressions, fieldAndArguments);
        break;
      case DeleteIfExists:
        validateIfExistsConditionType(errors, type, expressions, fieldAndArguments);
        break;
    }
  }

  private void validateIfConditionType(
      List<GraphQLError> errors,
      String type,
      List<Map<String, Object>> expressions,
      FieldAndArguments fieldAndArguments) {
    if (expressions == null || expressions.isEmpty()) {
      errors.add(
          createError(
              "expressions must be present for " + type + " condition type", fieldAndArguments));
    } else if (expressions.stream()
        .anyMatch(ex -> Sets.intersection(ex.keySet(), Constants.SCALAR_VALUE_KEYS).size() != 1)) {
      errors.add(
          createError(
              "expression must have only one of " + Constants.SCALAR_VALUE_KEYS,
              fieldAndArguments));
    }
  }

  private void validateIfExistsConditionType(
      List<GraphQLError> errors,
      String type,
      List<Map<String, Object>> expressions,
      FieldAndArguments fieldAndArguments) {
    if (expressions != null) {
      errors.add(
          createError(
              "expressions must not be present for " + type + " condition type",
              fieldAndArguments));
    }
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
