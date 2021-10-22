package com.scalar.db.graphql.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import graphql.Scalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLEnumValueDefinition;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import java.util.List;
import org.junit.Test;

public class TableGraphQLModelTest {
  private static final String NAMESPACE_NAME = "namespace_1";
  private static final String TABLE_NAME = "table_1";
  private static final String COLUMN_NAME_1 = "column_1";
  private static final String COLUMN_NAME_2 = "column_2";
  private static final String COLUMN_NAME_3 = "column_3";
  private static final String COLUMN_NAME_4 = "column_4";
  private static final String COLUMN_NAME_5 = "column_5";

  private void assertNullableFieldDefinition(
      GraphQLFieldDefinition field, String name, GraphQLType type) {
    assertThat(field.getName()).isEqualTo(name);
    assertThat(field.getType()).isEqualTo(type);
  }

  private void assertNonNullInputObjectField(
      GraphQLInputObjectField field, String name, GraphQLType type) {
    assertThat(field.getName()).isEqualTo(name);
    assertThat(field.getType()).isInstanceOf(GraphQLNonNull.class);
    assertThat(((GraphQLNonNull) field.getType()).getWrappedType()).isEqualTo(type);
  }

  private void assertNullableInputObjectField(
      GraphQLInputObjectField field, String name, GraphQLType type) {
    assertThat(field.getName()).isEqualTo(name);
    assertThat(field.getType()).isEqualTo(type);
  }

  private void assertNonNullArgument(GraphQLArgument argument, String name, GraphQLType type) {
    assertThat(argument.getName()).isEqualTo(name);
    assertThat(argument.getType()).isInstanceOf(GraphQLNonNull.class);
    assertThat(((GraphQLNonNull) argument.getType()).getWrappedType()).isEqualTo(type);
  }

  private TableMetadata createTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn(COLUMN_NAME_1, DataType.TEXT)
        .addColumn(COLUMN_NAME_2, DataType.INT)
        .addColumn(COLUMN_NAME_3, DataType.TEXT)
        .addColumn(COLUMN_NAME_4, DataType.FLOAT)
        .addColumn(COLUMN_NAME_5, DataType.BOOLEAN)
        .addPartitionKey(COLUMN_NAME_1)
        .addPartitionKey(COLUMN_NAME_2)
        .addClusteringKey(COLUMN_NAME_3)
        .addClusteringKey(COLUMN_NAME_4)
        .build();
  }

  private TableMetadata createTableMetadataWithoutClusteringKey() {
    return TableMetadata.newBuilder()
        .addColumn(COLUMN_NAME_1, DataType.TEXT)
        .addColumn(COLUMN_NAME_2, DataType.INT)
        .addColumn(COLUMN_NAME_3, DataType.TEXT)
        .addColumn(COLUMN_NAME_4, DataType.FLOAT)
        .addColumn(COLUMN_NAME_5, DataType.BOOLEAN)
        .addPartitionKey(COLUMN_NAME_1)
        .addPartitionKey(COLUMN_NAME_2)
        .build();
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldSetFields() {
    // Act
    TableMetadata tableMetadata = createTableMetadata();
    TableGraphQLModel model = new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, tableMetadata);

    // Assert
    assertThat(model.getNamespaceName()).isEqualTo(NAMESPACE_NAME);
    assertThat(model.getTableName()).isEqualTo(TABLE_NAME);
    assertThat(model.getTableMetadata()).isEqualTo(tableMetadata);
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreateObjectTypeForTable() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    GraphQLObjectType objectType = model.getObjectType();
    assertThat(objectType.getName()).isEqualTo(TABLE_NAME);
    List<GraphQLFieldDefinition> fields = objectType.getFieldDefinitions();
    assertThat(fields.size()).isEqualTo(5);
    assertNullableFieldDefinition(fields.get(0), COLUMN_NAME_1, Scalars.GraphQLString);
    assertNullableFieldDefinition(fields.get(1), COLUMN_NAME_2, Scalars.GraphQLInt);
    assertNullableFieldDefinition(fields.get(2), COLUMN_NAME_3, Scalars.GraphQLString);
    assertNullableFieldDefinition(fields.get(3), COLUMN_NAME_4, Scalars.GraphQLFloat);
    assertNullableFieldDefinition(fields.get(4), COLUMN_NAME_5, Scalars.GraphQLBoolean);
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreatePrimaryKeyInputObjectType() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // input table_1_key {
    //   column_1: String!
    //   column_2: Int!
    //   column_3: String!
    //   column_4: Float!
    // }
    GraphQLInputObjectType objectType = model.getPrimaryKeyInputObjectType();
    assertThat(objectType.getName()).isEqualTo(TABLE_NAME + "_Key");
    List<GraphQLInputObjectField> fields = objectType.getFieldDefinitions();
    assertThat(fields.size()).isEqualTo(4);
    assertNonNullInputObjectField(fields.get(0), COLUMN_NAME_1, Scalars.GraphQLString);
    assertNonNullInputObjectField(fields.get(1), COLUMN_NAME_2, Scalars.GraphQLInt);
    assertNonNullInputObjectField(fields.get(2), COLUMN_NAME_3, Scalars.GraphQLString);
    assertNonNullInputObjectField(fields.get(3), COLUMN_NAME_4, Scalars.GraphQLFloat);
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreatePartitionKeyInputObjectType() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // input table_1_PartitionKey {
    //   column_1: String!
    //   column_2: Int!
    // }
    GraphQLInputObjectType objectType = model.getPartitionKeyInputObjectType();
    assertThat(objectType.getName()).isEqualTo(TABLE_NAME + "_PartitionKey");
    List<GraphQLInputObjectField> fields = objectType.getFieldDefinitions();
    assertThat(fields.size()).isEqualTo(2);
    assertNonNullInputObjectField(fields.get(0), COLUMN_NAME_1, Scalars.GraphQLString);
    assertNonNullInputObjectField(fields.get(1), COLUMN_NAME_2, Scalars.GraphQLInt);
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreateGetPayloadObjectType() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // type table_1_GetPayload {
    //  table_1: table_1
    // }
    GraphQLObjectType objectType = model.getGetPayloadObjectType();
    assertThat(objectType.getName()).isEqualTo(TABLE_NAME + "_GetPayload");
    List<GraphQLFieldDefinition> fields = objectType.getFieldDefinitions();
    assertThat(fields.size()).isEqualTo(1);
    assertNullableFieldDefinition(fields.get(0), TABLE_NAME, model.getObjectType());
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreateQueryGetField() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // type Query {
    //  table_1_get(key: table_1_Key!): table_1_GetPayload
    // }
    GraphQLFieldDefinition field = model.getQueryGetField();
    assertNullableFieldDefinition(field, TABLE_NAME + "_get", model.getGetPayloadObjectType());
    assertThat(field.getArguments().size()).isEqualTo(1);

    GraphQLArgument argument = field.getArguments().get(0);
    assertNonNullArgument(argument, "key", model.getPrimaryKeyInputObjectType());
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreateClusteringKeyEnum() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // enum table_1_ClusteringKeyName {
    //  column_2
    //  column_3
    // }
    GraphQLEnumType enumType = model.getClusteringKeyNameEnum();
    assertThat(enumType.getName()).isEqualTo(TABLE_NAME + "_ClusteringKeyName");
    assertThat(enumType.getValues().size()).isEqualTo(2);

    GraphQLEnumValueDefinition value1 = enumType.getValues().get(0);
    assertThat(value1.getName()).isEqualTo(COLUMN_NAME_3);

    GraphQLEnumValueDefinition value2 = enumType.getValues().get(1);
    assertThat(value2.getName()).isEqualTo(COLUMN_NAME_4);
  }

  @Test
  public void
      constructor_TableMetadataWithoutClusteringKeyGiven_ShouldNotCreateClusteringKeyEnum() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(
            NAMESPACE_NAME, TABLE_NAME, createTableMetadataWithoutClusteringKey());

    // Assert
    assertThat(model.getClusteringKeyNameEnum()).isNull();
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreateScanInputObjectType() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // input table_1_ScanInput {
    //  orderings: [table_1_ScanOrdering]
    //  start: [table_1_ScanBoundary]
    //  end: [table_1_ScanBoundary]
    //  limit: Int
    // }
    GraphQLInputObjectType objectType = model.getScanInputObjectType();
    assertThat(objectType.getName()).isEqualTo(TABLE_NAME + "_ScanInput");
    List<GraphQLInputObjectField> fields = objectType.getFieldDefinitions();
    assertThat(fields.size()).isEqualTo(4);

    GraphQLInputObjectField field = fields.get(0);
    assertThat(field.getName()).isEqualTo("orderings");
    assertThat(field.getType()).isInstanceOf(GraphQLList.class);
    assertThat(((GraphQLList) field.getType()).getWrappedType())
        .isEqualTo(model.getScanOrderingInputObjectType());

    field = fields.get(1);
    assertThat(field.getName()).isEqualTo("start");
    assertThat(field.getType()).isInstanceOf(GraphQLList.class);
    assertThat(((GraphQLList) field.getType()).getWrappedType())
        .isEqualTo(model.getScanBoundaryInputObjectType());

    field = fields.get(2);
    assertThat(field.getName()).isEqualTo("end");
    assertThat(field.getType()).isInstanceOf(GraphQLList.class);
    assertThat(((GraphQLList) field.getType()).getWrappedType())
        .isEqualTo(model.getScanBoundaryInputObjectType());

    assertNullableInputObjectField(fields.get(3), "limit", Scalars.GraphQLInt);
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreateScanOrderingInputObjectType() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // input table_1_ScanOrdering {
    //  name: table_1_ClusteringKeyName!
    //  order: ScanOrderingOrder!
    // }
    GraphQLInputObjectType inputObjectType = model.getScanOrderingInputObjectType();
    assertThat(inputObjectType.getName()).isEqualTo(TABLE_NAME + "_ScanOrdering");
    assertThat(inputObjectType.getFieldDefinitions().size()).isEqualTo(2);
    assertNonNullInputObjectField(
        inputObjectType.getFieldDefinitions().get(0), "name", model.getClusteringKeyNameEnum());

    GraphQLInputObjectField field = inputObjectType.getFieldDefinitions().get(1);
    assertThat(field.getName()).isEqualTo("order");
    assertThat(field.getType()).isInstanceOf(GraphQLNonNull.class);
    GraphQLType wrappedType = ((GraphQLNonNull) field.getType()).getWrappedType();
    assertThat(wrappedType).isInstanceOf(GraphQLTypeReference.class);
    assertThat(((GraphQLTypeReference) wrappedType).getName()).isEqualTo("ScanOrderingOrder");
  }

  @Test
  public void
      constructor_TableMetadataWithoutClusteringKeyGiven_ShouldCreateScanOrderingInputObjectTypeWithoutNameField() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(
            NAMESPACE_NAME, TABLE_NAME, createTableMetadataWithoutClusteringKey());

    // Assert
    // input table_1_ScanOrdering {
    //  order: ScanOrderingOrder!
    // }
    GraphQLInputObjectType inputObjectType = model.getScanOrderingInputObjectType();
    assertThat(inputObjectType.getName()).isEqualTo(TABLE_NAME + "_ScanOrdering");
    assertThat(inputObjectType.getFieldDefinitions().size()).isEqualTo(1);

    GraphQLInputObjectField field = inputObjectType.getFieldDefinitions().get(0);
    assertThat(field.getName()).isEqualTo("order");
    assertThat(field.getType()).isInstanceOf(GraphQLNonNull.class);
    GraphQLType wrappedType = ((GraphQLNonNull) field.getType()).getWrappedType();
    assertThat(wrappedType).isInstanceOf(GraphQLTypeReference.class);
    assertThat(((GraphQLTypeReference) wrappedType).getName()).isEqualTo("ScanOrderingOrder");
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreateScanBoundaryInputObjectType() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // input table_1_ScanBoundary {
    //  name: table_1_ClusteringKeyName!
    //  inclusive: Boolean
    //  intValue: Int
    //  floatValue: Float
    //  stringValue: String
    //  booleanValue: Boolean
    // }
    GraphQLInputObjectType inputObjectType = model.getScanBoundaryInputObjectType();
    assertThat(inputObjectType.getName()).isEqualTo(TABLE_NAME + "_ScanBoundary");
    List<GraphQLInputObjectField> fields = inputObjectType.getFieldDefinitions();
    assertThat(fields.size()).isEqualTo(6);
    assertNonNullInputObjectField(fields.get(0), "name", model.getClusteringKeyNameEnum());
    assertNullableInputObjectField(fields.get(1), "inclusive", Scalars.GraphQLBoolean);
    assertNullableInputObjectField(fields.get(2), "intValue", Scalars.GraphQLInt);
    assertNullableInputObjectField(fields.get(3), "floatValue", Scalars.GraphQLFloat);
    assertNullableInputObjectField(fields.get(4), "stringValue", Scalars.GraphQLString);
    assertNullableInputObjectField(fields.get(5), "booleanValue", Scalars.GraphQLBoolean);
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreateScanPayloadObjectType() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // type table_1_ScanPayload {
    //  table_1: [table_1]!
    // }
    GraphQLObjectType objectType = model.getScanPayloadObjectType();
    assertThat(objectType.getName()).isEqualTo(TABLE_NAME + "_ScanPayload");
    List<GraphQLFieldDefinition> fields = objectType.getFieldDefinitions();
    assertThat(fields.size()).isEqualTo(1);

    GraphQLFieldDefinition field = fields.get(0);
    assertThat(field.getName()).isEqualTo(TABLE_NAME);
    GraphQLType type = field.getType();
    assertThat(type).isInstanceOf(GraphQLNonNull.class);
    assertThat(((GraphQLNonNull) type).getWrappedType()).isInstanceOf(GraphQLList.class);
    assertThat(((GraphQLList) ((GraphQLNonNull) type).getWrappedType()).getWrappedType())
        .isEqualTo(model.getObjectType());
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreateQueryScanField() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // type Query {
    //  table_1_scan(key: table_1_PartitionKey!, input: table_1_ScanInput): table_1_ScanPayload
    // }
    GraphQLFieldDefinition field = model.getQueryScanField();
    assertNullableFieldDefinition(field, TABLE_NAME + "_scan", model.getScanPayloadObjectType());
    assertThat(field.getArguments().size()).isEqualTo(2);

    List<GraphQLArgument> arguments = field.getArguments();
    assertNonNullArgument(arguments.get(0), "key", model.getPartitionKeyInputObjectType());

    GraphQLArgument argument = arguments.get(1);
    assertThat(argument.getName()).isEqualTo("input");
    assertThat(argument.getType()).isEqualTo(model.getScanInputObjectType());
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreatePutValuesObjectType() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // input table_1_PutValues {
    //  column_5: Boolean
    // }
    GraphQLInputObjectType objectType = model.getPutValuesObjectType();
    assertThat(objectType.getName()).isEqualTo(TABLE_NAME + "_PutValues");
    List<GraphQLInputObjectField> fields = objectType.getFieldDefinitions();
    assertThat(fields.size()).isEqualTo(1);
    assertNullableInputObjectField(fields.get(0), COLUMN_NAME_5, Scalars.GraphQLBoolean);
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreatePutInputObjectType() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // input table_1_PutInput {
    //   key: table_1_Key!
    //   values: table_1_PutValues!
    //   condition: PutCondition
    // }
    GraphQLInputObjectType objectType = model.getPutInputObjectType();
    assertThat(objectType.getName()).isEqualTo(TABLE_NAME + "_PutInput");
    List<GraphQLInputObjectField> fields = objectType.getFieldDefinitions();
    assertThat(fields.size()).isEqualTo(3);
    assertNonNullInputObjectField(fields.get(0), "key", model.getPrimaryKeyInputObjectType());
    assertNonNullInputObjectField(fields.get(1), "values", model.getPutValuesObjectType());
    GraphQLInputType conditionType = fields.get(2).getType();
    assertThat(conditionType).isInstanceOf(GraphQLTypeReference.class);
    assertThat(((GraphQLTypeReference) conditionType).getName()).isEqualTo("PutCondition");
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreatePutPayloadObjectType() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // type table_1_PutPayload {
    //  table_1: table_1
    // }
    GraphQLObjectType objectType = model.getPutPayloadObjectType();
    assertThat(objectType.getName()).isEqualTo(TABLE_NAME + "_PutPayload");
    List<GraphQLFieldDefinition> fields = objectType.getFieldDefinitions();
    assertThat(fields.size()).isEqualTo(1);
    assertNullableFieldDefinition(fields.get(0), TABLE_NAME, model.getObjectType());
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreateMutationPutField() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // type Mutation {
    //  table_1_put(put: [table1_PutInput!]!): table_1_PutPayload
    // }
    GraphQLFieldDefinition field = model.getMutationPutField();
    assertNullableFieldDefinition(field, TABLE_NAME + "_put", model.getPutPayloadObjectType());
    assertThat(field.getArguments().size()).isEqualTo(1);

    GraphQLArgument argument = field.getArguments().get(0);
    assertThat(argument.getName()).isEqualTo("put");
    assertThat(argument.getType()).isInstanceOf(GraphQLNonNull.class);
    GraphQLType wrappedType1 = ((GraphQLNonNull) argument.getType()).getWrappedType();
    assertThat(wrappedType1).isInstanceOf(GraphQLList.class);
    GraphQLType wrappedType2 = ((GraphQLList) wrappedType1).getWrappedType();
    assertThat(wrappedType2).isInstanceOf(GraphQLNonNull.class);
    assertThat(((GraphQLNonNull) wrappedType2).getWrappedType())
        .isEqualTo(model.getPutInputObjectType());
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreateDeleteInputObjectType() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // input table_1_DeleteInput {
    //   key: table_1_Key!
    //   condition: DeleteCondition
    // }
    GraphQLInputObjectType objectType = model.getDeleteInputObjectType();
    assertThat(objectType.getName()).isEqualTo(TABLE_NAME + "_DeleteInput");
    List<GraphQLInputObjectField> fields = objectType.getFieldDefinitions();
    assertThat(fields.size()).isEqualTo(2);
    assertNonNullInputObjectField(fields.get(0), "key", model.getPrimaryKeyInputObjectType());
    GraphQLInputType conditionType = fields.get(1).getType();
    assertThat(conditionType).isInstanceOf(GraphQLTypeReference.class);
    assertThat(((GraphQLTypeReference) conditionType).getName()).isEqualTo("DeleteCondition");
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreateDeletePayloadObjectType() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // type table_1_DeletePayload {
    //  table_1: table_1
    // }
    GraphQLObjectType objectType = model.getDeletePayloadObjectType();
    assertThat(objectType.getName()).isEqualTo(TABLE_NAME + "_DeletePayload");
    List<GraphQLFieldDefinition> fields = objectType.getFieldDefinitions();
    assertThat(fields.size()).isEqualTo(1);
    assertNullableFieldDefinition(fields.get(0), TABLE_NAME, model.getObjectType());
  }

  @Test
  public void constructor_NonNullArgumentsGiven_ShouldCreateMutationDeleteField() {
    // Act
    TableGraphQLModel model =
        new TableGraphQLModel(NAMESPACE_NAME, TABLE_NAME, createTableMetadata());

    // Assert
    // type Mutation {
    //  table_1_delete(key: table_1_Key!, condition: DeleteCondition): table_1_DeletePayload
    // }
    GraphQLFieldDefinition field = model.getMutationDeleteField();
    assertNullableFieldDefinition(
        field, TABLE_NAME + "_delete", model.getDeletePayloadObjectType());
    assertThat(field.getArguments().size()).isEqualTo(2);

    List<GraphQLArgument> arguments = field.getArguments();
    assertNonNullArgument(arguments.get(0), "key", model.getPrimaryKeyInputObjectType());
    GraphQLArgument argument = arguments.get(1);
    assertThat(argument.getName()).isEqualTo("condition");
    assertThat(argument.getType()).isInstanceOf(GraphQLTypeReference.class);
    assertThat(((GraphQLTypeReference) argument.getType()).getName()).isEqualTo("DeleteCondition");
  }
}
