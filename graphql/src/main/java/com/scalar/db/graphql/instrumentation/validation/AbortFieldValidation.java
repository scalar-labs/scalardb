package com.scalar.db.graphql.instrumentation.validation;

import com.google.common.collect.ImmutableList;
import graphql.ErrorType;
import graphql.GraphQLError;
import graphql.GraphqlErrorBuilder;
import graphql.execution.instrumentation.fieldvalidation.FieldValidation;
import graphql.execution.instrumentation.fieldvalidation.FieldValidationEnvironment;
import graphql.language.Argument;
import graphql.language.BooleanValue;
import graphql.language.Directive;
import graphql.language.Field;
import graphql.language.NullValue;
import graphql.language.OperationDefinition;
import graphql.language.Selection;
import graphql.language.SourceLocation;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class AbortFieldValidation implements FieldValidation {
  @SuppressWarnings("rawtypes")
  @Override
  public List<GraphQLError> validateFields(FieldValidationEnvironment validationEnvironment) {
    OperationDefinition operationDefinition =
        validationEnvironment.getExecutionContext().getOperationDefinition();
    List<Selection> selections = operationDefinition.getSelectionSet().getSelections();
    Optional<Selection> abortField =
        selections.stream()
            .filter(selection -> "abort".equals(((Field) selection).getName()))
            .findFirst();
    if (!abortField.isPresent()) {
      return Collections.emptyList();
    }

    ImmutableList.Builder<GraphQLError> errors = ImmutableList.builder();
    List<Directive> transactionDirectives = operationDefinition.getDirectives("transaction");
    SourceLocation location = abortField.get().getSourceLocation();
    if (transactionDirectives.isEmpty()) {
      errors.add(
          createError(
              "transaction directive with txId is required to abort transaction", location));
    } else {
      Directive directive = transactionDirectives.get(0); // @transaction is not repeatable
      Argument txIdArg = directive.getArgument("txId");
      Argument commitArg = directive.getArgument("commit");
      if (txIdArg == null
          || txIdArg.getValue() == null
          || txIdArg.getValue() instanceof NullValue) {
        errors.add(
            createError(
                "transaction directive with txId is required to abort transaction", location));
      } else if (commitArg != null
          && commitArg.getValue() != null
          && ((BooleanValue) commitArg.getValue()).isValue()) {
        errors.add(createError("abort and commit cannot be run at once", location));
      }
    }

    if (selections.size() != 1) {
      errors.add(createError("abort cannot be used together with other fields", location));
    }

    return errors.build();
  }

  private GraphQLError createError(String message, SourceLocation location) {
    return GraphqlErrorBuilder.newError()
        .message(message)
        .location(location)
        .errorType(ErrorType.ValidationError)
        .build();
  }
}
