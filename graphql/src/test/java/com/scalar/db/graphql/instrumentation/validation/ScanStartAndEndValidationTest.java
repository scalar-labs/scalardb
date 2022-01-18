package com.scalar.db.graphql.instrumentation.validation;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

public class ScanStartAndEndValidationTest extends FieldValidationTestBase {
  private static GraphQL graphQl;

  @BeforeClass
  public static void setUp() {
    // In tests, we create an executable schema with an SDL-first approach
    String testSchema =
        "type Query {\n"
            + "  table1_scan(scan: table1_ScanInput!): table1_ScanPayload\n"
            + "}\n"
            + "type table1 {\n"
            + "  col1: Int\n"
            + "  col2: String\n"
            + "  col3: String\n"
            + "}\n"
            + "type table1_ScanPayload {\n"
            + "  table1: [table1!]!\n"
            + "}\n"
            + "input table1_ScanInput {\n"
            + "  partitionKey: table1_PartitionKey!\n"
            + "  start: [table1_ClusteringKey!]\n"
            + "  end: [table1_ClusteringKey!]\n" // other fields are omitted for testing
            + "}\n"
            + "input table1_PartitionKey {\n"
            + "  col1: Int!\n"
            + "}\n"
            + "input table1_ClusteringKey {\n"
            + "  bigIntValue: Int\n" // using Int instead of custom scalar BigInt for testing
            + "  booleanValue: Boolean\n"
            + "  doubleValue: Float\n"
            + "  floatValue: Float\n" // using Float instead of custom scalar Float32 for testing
            + "  intValue: Int\n"
            + "  name: table1_ClusteringKeyName!\n"
            + "  textValue: String\n"
            + "}\n"
            + "enum table1_ClusteringKeyName {\n"
            + "  col2\n"
            + "}\n";
    TypeDefinitionRegistry typeRegistry = new SchemaParser().parse(testSchema);
    RuntimeWiring wiring =
        RuntimeWiring.newRuntimeWiring()
            .type(
                TypeRuntimeWiring.newTypeWiring("Query")
                    .dataFetcher(
                        "table1_scan",
                        environment ->
                            ImmutableMap.of(
                                "table1",
                                ImmutableList.of(
                                    ImmutableMap.of("col1", 1, "col2", "A", "col3", "B")))))
            .build();
    GraphQLSchema schema = new SchemaGenerator().makeExecutableSchema(typeRegistry, wiring);
    graphQl =
        GraphQL.newGraphQL(schema)
            .instrumentation(new FieldValidationInstrumentation(new ScanStartAndEndValidation()))
            .build();
  }

  @Test
  public void validateFields_BothStartAndEndNotGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "query {\n"
                + "  table1_scan(scan: {\n"
                + "    partitionKey: {col1: 1}\n"
                + "  }) {\n"
                + "    table1 { col3 }"
                + "  }\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void validateFields_ValidStartAndEndGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "query {\n"
                + "  table1_scan(scan: {\n"
                + "    partitionKey: {col1: 1},\n"
                + "    start: [{name: col2, textValue: \"A\"}],\n"
                + "    end: [{name: col2, textValue: \"Z\"}]\n"
                + "  }) {\n"
                + "    table1 { col3 }"
                + "  }\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void validateFields_StartWithNoValueGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "query {\n"
                + "  table1_scan(scan: {\n"
                + "    partitionKey: {col1: 1 },\n"
                + "    start: [{name: col2}]\n" // invalid
                + "  }) {\n"
                + "    table1 { col3 }"
                + "  }\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).hasSize(1);
    assertValidationError(result.getErrors().get(0), 2, 3);
  }

  @Test
  public void validateFields_StartWithMultipleValuesGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "query {\n"
                + "  table1_scan(scan: {\n"
                + "    partitionKey: {col1: 1},\n"
                + "    start: [{name: col2, textValue: \"A\", intValue: 0}]\n" // invalid
                + "  }) {\n"
                + "    table1 { col3 }"
                + "  }\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).hasSize(1);
    assertValidationError(result.getErrors().get(0), 2, 3);
  }

  @Test
  public void validateFields_EndWithNoValueGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "query {\n"
                + "  table1_scan(scan: {\n"
                + "    partitionKey: {col1: 1},\n"
                + "    end: [{name: col2}]\n" // invalid
                + "  }) {\n"
                + "    table1 { col3 }"
                + "  }\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).hasSize(1);
    assertValidationError(result.getErrors().get(0), 2, 3);
  }

  @Test
  public void validateFields_EndWithMultipleValuesGiven_ShouldValidate() {
    // Act
    ExecutionResult result =
        graphQl.execute(
            "query {\n"
                + "  table1_scan(scan: {\n"
                + "    partitionKey: {col1: 1},\n"
                + "    end: [{name: col2, doubleValue: 0.1, booleanValue: false}]\n" // invalid
                + "  }) {\n"
                + "    table1 { col3 }"
                + "  }\n"
                + "}");

    // Assert
    assertThat(result.getErrors()).hasSize(1);
    assertValidationError(result.getErrors().get(0), 2, 3);
  }
}
