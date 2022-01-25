package com.scalar.db.graphql.instrumentation.validation;

import com.google.common.collect.Sets;
import com.scalar.db.graphql.schema.Constants;
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
import java.util.Set;

public class ScanStartAndEndValidation implements FieldValidation {
  @SuppressWarnings("unchecked")
  @Override
  public List<GraphQLError> validateFields(FieldValidationEnvironment validationEnvironment) {
    List<GraphQLError> errors = new ArrayList<>();
    for (FieldAndArguments fieldAndArguments : validationEnvironment.getFields()) {
      String fieldName = fieldAndArguments.getField().getName();
      if (fieldName.endsWith("_scan")) {
        Map<String, Object> scan = fieldAndArguments.getArgumentValue("scan");
        List<Map<String, Object>> start = (List<Map<String, Object>>) scan.get("start");
        if (start != null) {
          start.forEach(
              clusteringKey ->
                  validateClusteringKey("start", clusteringKey, fieldAndArguments)
                      .ifPresent(errors::add));
        }
        List<Map<String, Object>> end = (List<Map<String, Object>>) scan.get("end");
        if (end != null) {
          end.forEach(
              clusteringKey ->
                  validateClusteringKey("end", clusteringKey, fieldAndArguments)
                      .ifPresent(errors::add));
        }
      }
    }
    return errors;
  }

  private Optional<GraphQLError> validateClusteringKey(
      String argName, Map<String, Object> clusteringKey, FieldAndArguments fieldAndArguments) {
    Set<String> valueKeys = Sets.intersection(clusteringKey.keySet(), Constants.SCALAR_VALUE_KEYS);
    if (valueKeys.size() == 1) {
      return Optional.empty();
    } else {
      return Optional.of(
          GraphqlErrorBuilder.newError()
              .message(
                  "the %s clustering key must have only one of %s",
                  argName, Constants.SCALAR_VALUE_KEYS)
              .errorType(ErrorType.ValidationError)
              .path(fieldAndArguments.getPath())
              .location(fieldAndArguments.getField().getSourceLocation())
              .build());
    }
  }
}
