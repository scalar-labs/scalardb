package com.scalar.db.graphql.schema;

import static com.scalar.db.graphql.schema.SchemaUtils.dataTypeToGraphQLScalarType;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_PREFIX;
import static com.scalar.db.transaction.consensuscommit.Attribute.COMMITTED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.CREATED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.PREPARED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.VERSION;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLEnumType.newEnum;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLInputObjectField.newInputObjectField;
import static graphql.schema.GraphQLInputObjectType.newInputObject;
import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.schema.GraphQLObjectType.newObject;
import static graphql.schema.GraphQLTypeReference.typeRef;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import graphql.Scalars;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TableGraphQLModel {
  private static final Set<String> CONSENSUSCOMMIT_ATTRIBUTES =
      ImmutableSet.of(ID, STATE, VERSION, PREPARED_AT, COMMITTED_AT, CREATED_AT);

  private final String namespaceName;
  private final String tableName;
  private final TableMetadata tableMetadata;
  private final boolean transactionEnabled;
  private final List<String> fieldNames;
  private final Map<String, GraphQLScalarType> fieldNameGraphQLScalarTypeMap;

  private final GraphQLObjectType objectType;
  private final GraphQLInputObjectType primaryKeyInputObjectType;
  private final GraphQLInputObjectType partitionKeyInputObjectType;
  private final GraphQLFieldDefinition queryGetField;
  private final GraphQLInputObjectType getInputObjectType;
  private final GraphQLObjectType getPayloadObjectType;
  private final GraphQLFieldDefinition queryScanField;
  private final GraphQLInputObjectType scanInputObjectType;
  private final GraphQLInputObjectType scanOrderingInputObjectType;
  private final GraphQLInputObjectType scanBoundaryInputObjectType;
  private final GraphQLEnumType clusteringKeyNameEnum;
  private final GraphQLObjectType scanPayloadObjectType;
  private final GraphQLFieldDefinition mutationPutField;
  private final GraphQLInputObjectType putInputObjectType;
  private final GraphQLInputObjectType putValuesObjectType;
  private final GraphQLObjectType putPayloadObjectType;
  private final GraphQLFieldDefinition mutationDeleteField;
  private final GraphQLInputObjectType deleteInputObjectType;
  private final GraphQLObjectType deletePayloadObjectType;

  public TableGraphQLModel(String namespaceName, String tableName, TableMetadata tableMetadata) {
    this.namespaceName = Objects.requireNonNull(namespaceName);
    this.tableName = Objects.requireNonNull(tableName);
    this.tableMetadata = Objects.requireNonNull(tableMetadata);

    this.transactionEnabled = tableMetadata.getColumnNames().contains(ID);
    this.fieldNames =
        tableMetadata.getColumnNames().stream()
            .filter(
                colName ->
                    !CONSENSUSCOMMIT_ATTRIBUTES.contains(colName)
                        && !colName.startsWith(BEFORE_PREFIX))
            .collect(toList());
    this.fieldNameGraphQLScalarTypeMap =
        fieldNames.stream()
            .collect(
                toMap(
                    colName -> colName,
                    colName ->
                        dataTypeToGraphQLScalarType(tableMetadata.getColumnDataType(colName))));

    this.objectType = createObjectType();
    this.primaryKeyInputObjectType = createPrimaryKeyInputObjectType();
    this.partitionKeyInputObjectType = createPartitionKeyInputObjectType();

    this.getInputObjectType = createGetInputObjectType();
    this.getPayloadObjectType = createGetPayloadObjectType();
    this.queryGetField = createQueryGetField();

    if (tableMetadata.getClusteringKeyNames().isEmpty()) {
      this.clusteringKeyNameEnum = null;
    } else {
      this.clusteringKeyNameEnum = createClusteringKeyNameEnum();
    }
    this.scanOrderingInputObjectType = createScanOrderingInputObjectType();
    this.scanBoundaryInputObjectType = createScanBoundaryInputObjectType();
    this.scanInputObjectType = createScanInputObjectType();
    this.scanPayloadObjectType = createScanPayloadObjectType();
    this.queryScanField = createQueryScanField();

    this.putValuesObjectType = createPutValuesObjectType();
    this.putInputObjectType = createPutInputObjectType();
    this.putPayloadObjectType = createPutPayloadObjectType();
    this.mutationPutField = createMutationPutField();

    this.deleteInputObjectType = createDeleteInputObjectType();
    this.deletePayloadObjectType = createDeletePayloadObjectType();
    this.mutationDeleteField = createMutationDeleteField();
  }

  public LinkedHashSet<String> getPartitionKeyNames() {
    return tableMetadata.getPartitionKeyNames();
  }

  public LinkedHashSet<String> getClusteringKeyNames() {
    return tableMetadata.getClusteringKeyNames();
  }

  public DataType getColumnDataType(String columnName) {
    return tableMetadata.getColumnDataType(columnName);
  }

  private GraphQLObjectType createObjectType() {
    GraphQLObjectType.Builder builder = newObject().name(tableName);
    fieldNames.stream()
        .map(name -> newFieldDefinition().name(name).type(fieldNameGraphQLScalarTypeMap.get(name)))
        .forEach(builder::field);
    return builder.build();
  }

  private GraphQLInputObjectType createPrimaryKeyInputObjectType() {
    LinkedHashSet<String> keyNames = new LinkedHashSet<>();
    keyNames.addAll(tableMetadata.getPartitionKeyNames());
    keyNames.addAll(tableMetadata.getClusteringKeyNames());

    GraphQLInputObjectType.Builder builder = newInputObject().name(objectType.getName() + "_Key");
    keyNames.forEach(
        keyName -> {
          GraphQLScalarType type =
              dataTypeToGraphQLScalarType(tableMetadata.getColumnDataType(keyName));
          builder.field(newInputObjectField().name(keyName).type(nonNull(type)));
        });

    return builder.build();
  }

  private GraphQLInputObjectType createPartitionKeyInputObjectType() {
    GraphQLInputObjectType.Builder builder =
        newInputObject().name(objectType.getName() + "_PartitionKey");
    tableMetadata
        .getPartitionKeyNames()
        .forEach(
            keyName -> {
              GraphQLScalarType type =
                  dataTypeToGraphQLScalarType(tableMetadata.getColumnDataType(keyName));
              builder.field(newInputObjectField().name(keyName).type(nonNull(type)));
            });

    return builder.build();
  }

  private GraphQLInputObjectType createGetInputObjectType() {
    return newInputObject()
        .name(objectType.getName() + "_GetInput")
        .field(newInputObjectField().name("key").type(nonNull(primaryKeyInputObjectType)))
        .field(newInputObjectField().name("consistency").type(typeRef("Consistency")))
        .build();
  }

  private GraphQLObjectType createGetPayloadObjectType() {
    return newObject()
        .name(objectType.getName() + "_GetPayload")
        .field(newFieldDefinition().name(objectType.getName()).type(objectType))
        .build();
  }

  private GraphQLFieldDefinition createQueryGetField() {
    return newFieldDefinition()
        .name(objectType.getName() + "_get")
        .type(getPayloadObjectType)
        .argument(newArgument().name("get").type(nonNull(getInputObjectType)))
        .build();
  }

  private GraphQLEnumType createClusteringKeyNameEnum() {
    GraphQLEnumType.Builder builder = newEnum().name(objectType.getName() + "_ClusteringKeyName");
    tableMetadata.getClusteringKeyNames().forEach(builder::value);
    return builder.build();
  }

  private GraphQLInputObjectType createScanOrderingInputObjectType() {
    GraphQLInputObjectType.Builder builder =
        newInputObject().name(objectType.getName() + "_ScanOrdering");
    if (clusteringKeyNameEnum != null) {
      builder.field(newInputObjectField().name("name").type(nonNull(clusteringKeyNameEnum)));
    }
    return builder
        .field(newInputObjectField().name("order").type(nonNull(typeRef("ScanOrderingOrder"))))
        .build();
  }

  private GraphQLInputObjectType createScanBoundaryInputObjectType() {
    GraphQLInputObjectType.Builder builder =
        newInputObject().name(objectType.getName() + "_ScanBoundary");
    if (clusteringKeyNameEnum != null) {
      builder.field(newInputObjectField().name("name").type(nonNull(clusteringKeyNameEnum)));
    }
    return builder
        .field(newInputObjectField().name("inclusive").type(Scalars.GraphQLBoolean))
        .field(newInputObjectField().name("intValue").type(Scalars.GraphQLInt))
        .field(newInputObjectField().name("floatValue").type(Scalars.GraphQLFloat))
        .field(newInputObjectField().name("stringValue").type(Scalars.GraphQLString))
        .field(newInputObjectField().name("booleanValue").type(Scalars.GraphQLBoolean))
        .build();
  }

  private GraphQLInputObjectType createScanInputObjectType() {
    return newInputObject()
        .name(objectType.getName() + "_ScanInput")
        .field(newInputObjectField().name("orderings").type(list(scanOrderingInputObjectType)))
        .field(newInputObjectField().name("start").type(list(scanBoundaryInputObjectType)))
        .field(newInputObjectField().name("end").type(list(scanBoundaryInputObjectType)))
        .field(newInputObjectField().name("limit").type(Scalars.GraphQLInt))
        .build();
  }

  private GraphQLObjectType createScanPayloadObjectType() {
    return newObject()
        .name(objectType.getName() + "_ScanPayload")
        .field(newFieldDefinition().name(objectType.getName()).type(nonNull(list(objectType))))
        .build();
  }

  private GraphQLFieldDefinition createQueryScanField() {
    return newFieldDefinition()
        .name(objectType.getName() + "_scan")
        .type(scanPayloadObjectType)
        .argument(newArgument().name("key").type(nonNull(partitionKeyInputObjectType)))
        .argument(newArgument().name("input").type(scanInputObjectType))
        .build();
  }

  private GraphQLInputObjectType createPutValuesObjectType() {
    GraphQLInputObjectType.Builder inputValues =
        newInputObject().name(objectType.getName() + "_PutValues");

    LinkedHashSet<String> keyNames = new LinkedHashSet<>();
    keyNames.addAll(getPartitionKeyNames());
    keyNames.addAll(getClusteringKeyNames());

    fieldNames.stream()
        .filter(name -> !keyNames.contains(name))
        .map(name -> newInputObjectField().name(name).type(fieldNameGraphQLScalarTypeMap.get(name)))
        .forEach(inputValues::field);

    return inputValues.build();
  }

  private GraphQLInputObjectType createPutInputObjectType() {
    return newInputObject()
        .name(objectType.getName() + "_PutInput")
        .field(newInputObjectField().name("key").type(nonNull(primaryKeyInputObjectType)))
        .field(newInputObjectField().name("values").type(nonNull(putValuesObjectType)))
        .field(newInputObjectField().name("condition").type(typeRef("PutCondition")))
        .build();
  }

  private GraphQLObjectType createPutPayloadObjectType() {
    return newObject()
        .name(objectType.getName() + "_PutPayload")
        .field(newFieldDefinition().name(objectType.getName()).type(objectType))
        .build();
  }

  private GraphQLFieldDefinition createMutationPutField() {
    return newFieldDefinition()
        .name(objectType.getName() + "_put")
        .type(putPayloadObjectType)
        .argument(newArgument().name("put").type(nonNull(list(nonNull(putInputObjectType)))))
        .build();
  }

  private GraphQLInputObjectType createDeleteInputObjectType() {
    return newInputObject()
        .name(objectType.getName() + "_DeleteInput")
        .field(newInputObjectField().name("key").type(nonNull(primaryKeyInputObjectType)))
        .field(newInputObjectField().name("condition").type(typeRef("DeleteCondition")))
        .build();
  }

  private GraphQLObjectType createDeletePayloadObjectType() {
    return newObject()
        .name(objectType.getName() + "_DeletePayload")
        .field(newFieldDefinition().name(objectType.getName()).type(objectType))
        .build();
  }

  private GraphQLFieldDefinition createMutationDeleteField() {
    return newFieldDefinition()
        .name(objectType.getName() + "_delete")
        .type(deletePayloadObjectType)
        .argument(newArgument().name("delete").type(nonNull(list(nonNull(deleteInputObjectType)))))
        .build();
  }

  public String getNamespaceName() {
    return namespaceName;
  }

  public String getTableName() {
    return tableName;
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }

  public boolean getTransactionEnabled() {
    return transactionEnabled;
  }

  public List<String> getFieldNames() {
    return fieldNames;
  }

  public GraphQLObjectType getObjectType() {
    return objectType;
  }

  public GraphQLInputObjectType getPrimaryKeyInputObjectType() {
    return primaryKeyInputObjectType;
  }

  public GraphQLInputObjectType getPartitionKeyInputObjectType() {
    return partitionKeyInputObjectType;
  }

  public GraphQLFieldDefinition getQueryGetField() {
    return queryGetField;
  }

  public GraphQLInputObjectType getGetInputObjectType() {
    return getInputObjectType;
  }

  public GraphQLObjectType getGetPayloadObjectType() {
    return getPayloadObjectType;
  }

  public GraphQLFieldDefinition getQueryScanField() {
    return queryScanField;
  }

  public GraphQLInputObjectType getScanInputObjectType() {
    return scanInputObjectType;
  }

  public GraphQLInputObjectType getScanOrderingInputObjectType() {
    return scanOrderingInputObjectType;
  }

  public GraphQLInputObjectType getScanBoundaryInputObjectType() {
    return scanBoundaryInputObjectType;
  }

  public GraphQLEnumType getClusteringKeyNameEnum() {
    return clusteringKeyNameEnum;
  }

  public GraphQLObjectType getScanPayloadObjectType() {
    return scanPayloadObjectType;
  }

  public GraphQLFieldDefinition getMutationPutField() {
    return mutationPutField;
  }

  public GraphQLInputObjectType getPutInputObjectType() {
    return putInputObjectType;
  }

  public GraphQLInputObjectType getPutValuesObjectType() {
    return putValuesObjectType;
  }

  public GraphQLObjectType getPutPayloadObjectType() {
    return putPayloadObjectType;
  }

  public GraphQLFieldDefinition getMutationDeleteField() {
    return mutationDeleteField;
  }

  public GraphQLInputObjectType getDeleteInputObjectType() {
    return deleteInputObjectType;
  }

  public GraphQLObjectType getDeletePayloadObjectType() {
    return deletePayloadObjectType;
  }
}
