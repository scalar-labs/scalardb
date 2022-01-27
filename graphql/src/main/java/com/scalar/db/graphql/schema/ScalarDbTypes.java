package com.scalar.db.graphql.schema;

import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLDirective.newDirective;
import static graphql.schema.GraphQLEnumType.newEnum;
import static graphql.schema.GraphQLInputObjectField.newInputObjectField;
import static graphql.schema.GraphQLInputObjectType.newInputObject;
import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.schema.GraphQLScalarType.newScalar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.DataType;
import graphql.Scalars;
import graphql.introspection.Introspection.DirectiveLocation;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import java.util.List;

public final class ScalarDbTypes {
  public static final GraphQLDirective TRANSACTION_DIRECTIVE =
      newDirective()
          .name("transaction")
          .argument(newArgument().name("txId").type(Scalars.GraphQLString))
          .argument(newArgument().name("commit").type(Scalars.GraphQLBoolean))
          .validLocations(DirectiveLocation.MUTATION, DirectiveLocation.QUERY)
          .build();

  public static final GraphQLScalarType BIG_INT_SCALAR =
      newScalar().name("BigInt").coercing(BigIntCoercing.INSTANCE).build();

  public static final GraphQLScalarType FLOAT_32_SCALAR =
      newScalar().name("Float32").coercing(FloatCoercing.INSTANCE).build();

  private static final GraphQLInputObjectType CONDITIONAL_EXPRESSION_INPUT_OBJECT =
      newInputObject()
          .name("ConditionalExpression")
          .field(newInputObjectField().name("name").type(nonNull(Scalars.GraphQLString)))
          .fields(getScalarValueInputObjectFields())
          .field(
              newInputObjectField()
                  .name("operator")
                  .type(
                      nonNull(
                          newEnum()
                              .name("ConditionalExpressionOperator")
                              .value(Operator.EQ.name())
                              .value(Operator.NE.name())
                              .value(Operator.GT.name())
                              .value(Operator.GTE.name())
                              .value(Operator.LT.name())
                              .value(Operator.LTE.name())
                              .build())))
          .build();

  private static final GraphQLInputObjectType PUT_CONDITION_INPUT_OBJECT =
      newInputObject()
          .name("PutCondition")
          .field(
              newInputObjectField()
                  .name("type")
                  .type(
                      nonNull(
                          newEnum()
                              .name("PutConditionType")
                              .value(PutConditionType.PutIf.name())
                              .value(PutConditionType.PutIfExists.name())
                              .value(PutConditionType.PutIfNotExists.name())
                              .build())))
          .field(
              newInputObjectField()
                  .name("expressions")
                  .type(list(nonNull(CONDITIONAL_EXPRESSION_INPUT_OBJECT))))
          .build();

  private static final GraphQLInputObjectType DELETE_CONDITION_INPUT_OBJECT =
      newInputObject()
          .name("DeleteCondition")
          .field(
              newInputObjectField()
                  .name("type")
                  .type(
                      nonNull(
                          newEnum()
                              .name("DeleteConditionType")
                              .value(DeleteConditionType.DeleteIf.name())
                              .value(DeleteConditionType.DeleteIfExists.name())
                              .build())))
          .field(
              newInputObjectField()
                  .name("expressions")
                  .type(list(nonNull(CONDITIONAL_EXPRESSION_INPUT_OBJECT))))
          .build();

  private static final GraphQLEnumType CONSISTENCY_ENUM =
      newEnum()
          .name("Consistency")
          .value(Consistency.SEQUENTIAL.name())
          .value(Consistency.EVENTUAL.name())
          .value(Consistency.LINEARIZABLE.name())
          .build();

  private static final GraphQLEnumType ORDER_ENUM =
      newEnum().name("Order").value(Order.ASC.name()).value(Order.DESC.name()).build();

  public static final ImmutableSet<GraphQLType> SCALAR_DB_GRAPHQL_TYPES =
      ImmutableSet.of(
          BIG_INT_SCALAR,
          FLOAT_32_SCALAR,
          PUT_CONDITION_INPUT_OBJECT,
          DELETE_CONDITION_INPUT_OBJECT,
          CONSISTENCY_ENUM,
          ORDER_ENUM);

  private ScalarDbTypes() {}

  static GraphQLScalarType dataTypeToGraphQLScalarType(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return Scalars.GraphQLBoolean;
      case INT:
        return Scalars.GraphQLInt;
      case BIGINT:
        return BIG_INT_SCALAR;
      case FLOAT:
        return FLOAT_32_SCALAR;
      case DOUBLE:
        return Scalars.GraphQLFloat;
      case TEXT:
        return Scalars.GraphQLString;
      case BLOB:
      default:
        throw new IllegalArgumentException(dataType.name() + " type is not supported");
    }
  }

  static List<GraphQLInputObjectField> getScalarValueInputObjectFields() {
    return ImmutableList.of(
        newInputObjectField().name("intValue").type(Scalars.GraphQLInt).build(),
        newInputObjectField().name("bigIntValue").type(BIG_INT_SCALAR).build(),
        newInputObjectField().name("floatValue").type(FLOAT_32_SCALAR).build(),
        newInputObjectField().name("doubleValue").type(Scalars.GraphQLFloat).build(),
        newInputObjectField().name("textValue").type(Scalars.GraphQLString).build(),
        newInputObjectField().name("booleanValue").type(Scalars.GraphQLBoolean).build());
  }

  public enum PutConditionType {
    PutIf,
    PutIfExists,
    PutIfNotExists
  }

  public enum DeleteConditionType {
    DeleteIf,
    DeleteIfExists
  }
}
