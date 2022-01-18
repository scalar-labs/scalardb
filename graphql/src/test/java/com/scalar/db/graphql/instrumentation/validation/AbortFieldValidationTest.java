package com.scalar.db.graphql.instrumentation.validation;

import static org.assertj.core.api.Assertions.assertThat;

import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.execution.instrumentation.fieldvalidation.FieldValidationInstrumentation;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import graphql.schema.idl.TypeRuntimeWiring;
import org.junit.BeforeClass;
import org.junit.Test;

public class AbortFieldValidationTest extends FieldValidationTestBase {
  private static GraphQL graphQl;

  @BeforeClass
  public static void setUp() {
    // In tests, we create an executable schema with an SDL-first approach
    String testSchema =
        "directive @transaction(commit: Boolean, txId: String) on QUERY | MUTATION\n"
            + "type Query { hello: String }\n"
            + "type Mutation {\n"
            + "  hello: String\n"
            + "  abort: Boolean!\n"
            + "}";
    TypeDefinitionRegistry typeRegistry = new SchemaParser().parse(testSchema);
    // Set a data fetcher that always returns true
    RuntimeWiring wiring =
        RuntimeWiring.newRuntimeWiring()
            .type(
                TypeRuntimeWiring.newTypeWiring("Mutation")
                    .dataFetcher("abort", environment -> true))
            .build();
    GraphQLSchema schema = new SchemaGenerator().makeExecutableSchema(typeRegistry, wiring);
    graphQl =
        GraphQL.newGraphQL(schema)
            .instrumentation(new FieldValidationInstrumentation(new AbortFieldValidation()))
            .build();
  }

  @Test
  public void validateFields_WhenProperAbortFieldGiven_ShouldValidate() {
    // Act
    ExecutionResult result = graphQl.execute("mutation @transaction(txId: \"xyz\") { abort }");

    // Assert
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void validateFields_WhenOtherFieldIsPresent_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute("mutation @transaction(txId: \"xyz\") {\n  hello\n  abort\n}");

    // Assert
    assertThat(result.getErrors()).hasSize(1);
    assertValidationError(result.getErrors().get(0), 3, 3);
  }

  @Test
  public void validateFields_WhenNoTransactionDirective_ShouldValidate() {
    // Act
    ExecutionResult result = graphQl.execute("mutation { abort }");

    // Assert
    assertThat(result.getErrors()).hasSize(1);
    assertValidationError(result.getErrors().get(0), 1, 12);
  }

  @Test
  public void validateFields_WhenTransactionDirectiveWithNoTxIdArg_ShouldSetError() {
    // Act
    ExecutionResult result = graphQl.execute("mutation @transaction(commit: false) { abort }");

    // Assert
    assertThat(result.getErrors()).hasSize(1);
    assertValidationError(result.getErrors().get(0), 1, 40);
  }

  @Test
  public void validateFields_WhenTransactionDirectiveWithNullTxId_ShouldValidate() {
    // Act
    ExecutionResult result = graphQl.execute("mutation @transaction(txId: null) {\n  abort }");

    // Assert
    assertThat(result.getErrors()).hasSize(1);
    assertValidationError(result.getErrors().get(0), 2, 3);
  }

  @Test
  public void validateFields_WhenCommitTrue_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute("mutation @transaction(txId: \"xyz\", commit: true) {\n  abort }");

    // Assert
    assertThat(result.getErrors()).hasSize(1);
    assertValidationError(result.getErrors().get(0), 2, 3);
  }

  @Test
  public void validateFields_WhenCommitFalse_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute("mutation @transaction(txId: \"xyz\", commit: false) {\n  abort }");

    // Assert
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void
      validateFields_WhenNoTransactionDirectiveAndOtherField_ShouldSetTwoValidationErrors() {
    // Act
    ExecutionResult result = graphQl.execute("mutation { abort\n hello }");

    // Assert
    assertThat(result.getErrors()).hasSize(2);
    assertValidationError(result.getErrors().get(0), 1, 12);
    assertValidationError(result.getErrors().get(1), 1, 12);
  }
}
