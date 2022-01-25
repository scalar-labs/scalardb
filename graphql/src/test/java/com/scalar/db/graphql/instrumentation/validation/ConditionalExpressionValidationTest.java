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

public class ConditionalExpressionValidationTest extends FieldValidationTestBase {
  private static GraphQL graphQl;

  @BeforeClass
  public static void setUp() {
    // In tests, we create an executable schema with an SDL-first approach
    String testSchema =
        "type Query { hello: String }\n"
            + "type Mutation {\n"
            + "  table1_put(put: table1_PutInput!): Boolean!\n"
            + "  table1_delete(delete: table1_DeleteInput!): Boolean!\n"
            + "  table1_bulkPut(put: [table1_PutInput!]!): Boolean!\n"
            + "  table1_bulkDelete(delete: [table1_DeleteInput!]!): Boolean!\n"
            + "  table1_mutate(delete: [table1_DeleteInput!], put: [table1_PutInput!]): Boolean!\n"
            + "}\n"
            + "input table1_PutInput {\n"
            + "  condition: PutCondition\n" // other PutInput fields are omitted for testing
            + "}\n"
            + "input table1_DeleteInput {\n"
            + "  condition: DeleteCondition\n" // other DeleteInput fields are omitted for testing
            + "}\n"
            + "input PutCondition {\n"
            + "  expressions: [ConditionalExpression!]\n"
            + "  type: PutConditionType!\n"
            + "}\n"
            + "input DeleteCondition {\n"
            + "  expressions: [ConditionalExpression!]\n"
            + "  type: DeleteConditionType!\n"
            + "}\n"
            + "input ConditionalExpression {\n"
            + "  bigIntValue: Int\n" // using Int instead of custom scalar BigInt for testing
            + "  booleanValue: Boolean\n"
            + "  doubleValue: Float\n"
            + "  floatValue: Float\n" // using Float instead of custom scalar Float32 for testing
            + "  intValue: Int\n"
            + "  name: String!\n"
            + "  operator: ConditionalExpressionOperator!\n"
            + "  textValue: String\n"
            + "}\n"
            + "enum PutConditionType {\n"
            + "  PutIf\n"
            + "  PutIfExists\n"
            + "  PutIfNotExists\n"
            + "}\n"
            + "enum DeleteConditionType {\n"
            + "  DeleteIf\n"
            + "  DeleteIfExists\n"
            + "}\n"
            + "enum ConditionalExpressionOperator {\n"
            + "  EQ\n" // other ConditionalExpressionOperator fields are omitted for testing
            + "}";
    TypeDefinitionRegistry typeRegistry = new SchemaParser().parse(testSchema);
    RuntimeWiring wiring =
        RuntimeWiring.newRuntimeWiring()
            .type(
                TypeRuntimeWiring.newTypeWiring("Mutation")
                    .dataFetcher("table1_put", environment -> true)
                    .dataFetcher("table1_delete", environment -> true)
                    .dataFetcher("table1_bulkPut", environment -> true)
                    .dataFetcher("table1_bulkDelete", environment -> true)
                    .dataFetcher("table1_mutate", environment -> true))
            .build();
    GraphQLSchema schema = new SchemaGenerator().makeExecutableSchema(typeRegistry, wiring);
    graphQl =
        GraphQL.newGraphQL(schema)
            .instrumentation(
                new FieldValidationInstrumentation(new ConditionalExpressionValidation()))
            .build();
  }

  //
  // Tests for put
  //

  @Test
  public void validateFields_PutWithoutConditionGiven_ShouldValidate() {
    // Act
    ExecutionResult result = graphQl.execute("mutation { table1_put(put: {}) }");

    // Assert
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void validateFields_PutIfWithValidExpressionsGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_put(put: {\n"
                + "    condition: {\n"
                + "      type: PutIf,\n"
                + "      expressions: [{name: \"col1\", operator: EQ, intValue: 0}]\n"
                + "    }\n"
                + "  })\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void validateFields_PutIfWithNoExpressionsGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_put(put: {\n"
                + "    condition: {\n" // invalid
                + "      type: PutIf\n"
                + "    }\n"
                + "  })\n"
                + "}");

    // Assert
    assertThat(result.getErrors())
        .hasOnlyOneElementSatisfying(error -> assertValidationError(error, 2, 3));
  }

  @Test
  public void validateFields_PutIfWithEmptyExpressionsGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_put(put: {\n"
                + "    condition: {\n" // invalid
                + "      type: PutIf,\n"
                + "      expressions: []\n"
                + "    }\n"
                + "  })\n"
                + "}");

    // Assert
    assertThat(result.getErrors())
        .hasOnlyOneElementSatisfying(error -> assertValidationError(error, 2, 3));
  }

  @Test
  public void validateFields_PutIfWithMultipleValuesExpressionsGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_put(put: {\n"
                + "    condition: {\n" // invalid
                + "      type: PutIf,\n"
                + "      expressions: [{name: \"col1\", operator: EQ, floatValue: 0.1, booleanValue: true}]\n"
                + "    }\n"
                + "  })\n"
                + "}");

    // Assert
    assertThat(result.getErrors())
        .hasOnlyOneElementSatisfying(error -> assertValidationError(error, 2, 3));
  }

  @Test
  public void validateFields_PutIfExistsWithNoExpressionsGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_put(put: {\n"
                + "    condition: {\n"
                + "      type: PutIfExists\n"
                + "    }\n"
                + "  })\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void validateFields_PutIfExistsWithExpressionsGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_put(put: {\n"
                + "    condition: {\n"
                + "      type: PutIfExists,\n"
                + "      expressions: []\n"
                + "    }\n"
                + "  })\n"
                + "}");

    // Assert
    assertThat(result.getErrors())
        .hasOnlyOneElementSatisfying(error -> assertValidationError(error, 2, 3));
  }

  @Test
  public void validateFields_PutIfNotExistsWithNoExpressionsGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_put(put: {\n"
                + "    condition: {\n"
                + "      type: PutIfNotExists\n"
                + "    }\n"
                + "  })\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void validateFields_PutIfNotExistsWithExpressionsGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_put(put: {\n"
                + "    condition: {\n" // invalid
                + "      type: PutIfNotExists,\n"
                + "      expressions: []\n"
                + "    }\n"
                + "  })\n"
                + "}");

    // Assert
    assertThat(result.getErrors())
        .hasOnlyOneElementSatisfying(error -> assertValidationError(error, 2, 3));
  }

  //
  // Tests for delete
  //

  @Test
  public void validateFields_DeleteWithoutConditionGiven_ShouldValidate() {
    // Act
    ExecutionResult result = graphQl.execute("mutation { table1_delete(delete: {}) }");

    // Assert
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void validateFields_DeleteIfWithValidExpressionsGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_delete(delete: {\n"
                + "    condition: {\n"
                + "      type: DeleteIf,\n"
                + "      expressions: [{name: \"col1\", operator: EQ, intValue: 0}]\n"
                + "    }\n"
                + "  })\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void validateFields_DeleteIfWithNoExpressionsGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_delete(delete: {\n"
                + "    condition: {\n" // invalid
                + "      type: DeleteIf\n"
                + "    }\n"
                + "  })\n"
                + "}");

    // Assert
    assertThat(result.getErrors())
        .hasOnlyOneElementSatisfying(error -> assertValidationError(error, 2, 3));
  }

  @Test
  public void validateFields_DeleteIfWithEmptyExpressionsGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_delete(delete: {\n"
                + "    condition: {\n" // invalid
                + "      type: DeleteIf,\n"
                + "      expressions: []\n"
                + "    }\n"
                + "  })\n"
                + "}");

    // Assert
    assertThat(result.getErrors())
        .hasOnlyOneElementSatisfying(error -> assertValidationError(error, 2, 3));
  }

  @Test
  public void validateFields_DeleteIfWithMultipleValuesExpressionsGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_delete(delete: {\n"
                + "    condition: {\n" // invalid
                + "      type: DeleteIf,\n"
                + "      expressions: [{name: \"col1\", operator: EQ, floatValue: 0.1, booleanValue: true}]\n"
                + "    }\n"
                + "  })\n"
                + "}");

    // Assert
    assertThat(result.getErrors())
        .hasOnlyOneElementSatisfying(error -> assertValidationError(error, 2, 3));
  }

  @Test
  public void validateFields_DeleteIfExistsWithNoExpressionsGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_delete(delete: {\n"
                + "    condition: {\n"
                + "      type: DeleteIfExists\n"
                + "    }\n"
                + "  })\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void validateFields_DeleteIfExistsWithExpressionsGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_delete(delete: {\n"
                + "    condition: {\n" // invalid
                + "      type: DeleteIfExists,\n"
                + "      expressions: []\n"
                + "    }\n"
                + "  })\n"
                + "}");

    // Assert
    assertThat(result.getErrors())
        .hasOnlyOneElementSatisfying(error -> assertValidationError(error, 2, 3));
  }

  //
  // Tests for bulkPut, bulkDelete, and mutate
  //

  @Test
  public void validateFields_BulkPutWithMultiplePutsGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_bulkPut(put: [\n"
                + "    {\n"
                + "      condition: {\n" // invalid
                + "        type: PutIf,\n"
                + "        expressions: [{name: \"col1\", operator: EQ, intValue: 0, textValue: \"xyz\"}]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      condition: {\n" // invalid
                + "        type: PutIf\n"
                + "      }\n"
                + "    }\n"
                + "  ])\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).hasSize(2);
    assertValidationError(result.getErrors().get(0), 2, 3);
    assertValidationError(result.getErrors().get(1), 2, 3);
  }

  @Test
  public void validateFields_BulkDeleteWithMultipleDeletesGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_bulkDelete(delete: [\n"
                + "    {\n"
                + "      condition: {\n" // invalid
                + "        type: DeleteIf,\n"
                + "        expressions: [{name: \"col1\", operator: EQ, intValue: 0, textValue: \"xyz\"}]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      condition: {\n" // invalid
                + "        type: DeleteIf\n"
                + "      }\n"
                + "    }\n"
                + "  ])\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).hasSize(2);
    assertValidationError(result.getErrors().get(0), 2, 3);
    assertValidationError(result.getErrors().get(1), 2, 3);
  }

  @Test
  public void validateFields_MutateWithMultiplePutsAndDeletesGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_mutate(\n"
                + "    put: [\n"
                + "      {\n"
                + "        condition: {\n" // invalid
                + "          type: PutIf,\n"
                + "          expressions: [{name: \"col1\", operator: EQ, bigIntValue: 10, doubleValue: 0.01}]\n"
                + "        }\n"
                + "      },\n"
                + "      {\n"
                + "        condition: {\n" // invalid
                + "          type: PutIf\n"
                + "        }\n"
                + "      }\n"
                + "    ],\n"
                + "    delete: [\n"
                + "      {\n"
                + "        condition: {\n" // invalid
                + "          type: DeleteIf,\n"
                + "          expressions: [{name: \"col1\", operator: EQ, intValue: 0, textValue: \"xyz\"}]\n"
                + "        }\n"
                + "      },\n"
                + "      {\n"
                + "        condition: {\n" // invalid
                + "          type: DeleteIf\n"
                + "        }\n"
                + "      }\n"
                + "    ]\n"
                + "  )\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).hasSize(4);
    assertValidationError(result.getErrors().get(0), 2, 3);
    assertValidationError(result.getErrors().get(1), 2, 3);
    assertValidationError(result.getErrors().get(2), 2, 3);
    assertValidationError(result.getErrors().get(3), 2, 3);
  }

  @Test
  public void validateFields_MutateWithMultiplePutsOnlyGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_mutate(\n"
                + "    put: [\n"
                + "      {\n"
                + "        condition: {\n" // invalid
                + "          type: PutIf,\n"
                + "          expressions: [{name: \"col1\", operator: EQ, intValue: 0, textValue: \"xyz\"}]\n"
                + "        }\n"
                + "      },\n"
                + "      {\n"
                + "        condition: {\n" // invalid
                + "          type: PutIf\n"
                + "        }\n"
                + "      }\n"
                + "    ]\n"
                + "  )\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).hasSize(2);
    assertValidationError(result.getErrors().get(0), 2, 3);
    assertValidationError(result.getErrors().get(1), 2, 3);
  }

  @Test
  public void validateFields_MutateWithMultipleDeletesOnlyGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_mutate(\n"
                + "    delete: [\n"
                + "      {\n"
                + "        condition: {\n" // invalid
                + "          type: DeleteIf,\n"
                + "          expressions: [{name: \"col1\", operator: EQ, intValue: 0, textValue: \"xyz\"}]\n"
                + "        }\n"
                + "      },\n"
                + "      {\n"
                + "        condition: {\n" // invalid
                + "          type: DeleteIf\n"
                + "        }\n"
                + "      }\n"
                + "    ]\n"
                + "  )\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).hasSize(2);
    assertValidationError(result.getErrors().get(0), 2, 3);
    assertValidationError(result.getErrors().get(1), 2, 3);
  }

  //
  // Tests for multiple mutation fields
  //

  @Test
  public void validateFields_MultipleMutationFieldsGiven1_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute("mutation { table1_put(put: {}), table1_delete(delete: {}) }");

    // Assert
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void validateFields_MultipleMutationFieldsGiven2_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "mutation {\n"
                + "  table1_put(\n"
                + "    put: {}\n"
                + "  ),\n"
                + "  table1_bulkDelete(delete: [\n"
                + "    {\n"
                + "      condition: {\n" // invalid
                + "        type: DeleteIf\n"
                + "      }\n"
                + "    }\n"
                + "  ])\n"
                + "}");

    // Assert
    assertThat(result.getErrors())
        .hasOnlyOneElementSatisfying(error -> assertValidationError(error, 5, 3));
  }
}
