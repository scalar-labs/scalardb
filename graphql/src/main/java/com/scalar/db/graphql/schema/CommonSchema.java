package com.scalar.db.graphql.schema;

import static graphql.schema.GraphQLEnumType.newEnum;
import static graphql.schema.GraphQLInputObjectField.newInputObjectField;
import static graphql.schema.GraphQLInputObjectType.newInputObject;
import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.schema.GraphQLScalarType.newScalar;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Scan;
import graphql.Scalars;
import graphql.introspection.Introspection.DirectiveLocation;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLNamedInputType;
import graphql.schema.GraphQLScalarType;
import java.util.Set;

public final class CommonSchema {
  public static final GraphQLScalarType FLOAT_32_SCALAR =
      newScalar().name("Float32").coercing(FloatCoercing.INSTANCE).build();

  public static final GraphQLScalarType BIG_INT_SCALAR =
      newScalar().name("BigInt").coercing(BigIntCoercing.INSTANCE).build();

  public static Set<GraphQLNamedInputType> createCommonGraphQLTypes() {
    GraphQLInputObjectType conditionalExpressionInputObject =
        newInputObject()
            .name("ConditionalExpression")
            .field(newInputObjectField().name("name").type(nonNull(Scalars.GraphQLString)))
            .field(newInputObjectField().name("intValue").type(Scalars.GraphQLInt))
            .field(newInputObjectField().name("bigIntValue").type(BIG_INT_SCALAR))
            .field(newInputObjectField().name("floatValue").type(FLOAT_32_SCALAR))
            .field(newInputObjectField().name("doubleValue").type(Scalars.GraphQLFloat))
            .field(newInputObjectField().name("textValue").type(Scalars.GraphQLString))
            .field(newInputObjectField().name("booleanValue").type(Scalars.GraphQLBoolean))
            .field(
                newInputObjectField()
                    .name("operator")
                    .type(
                        nonNull(
                            newEnum()
                                .name("ConditionalExpressionOperator")
                                .value(ConditionalExpression.Operator.EQ.name())
                                .value(ConditionalExpression.Operator.NE.name())
                                .value(ConditionalExpression.Operator.GT.name())
                                .value(ConditionalExpression.Operator.GTE.name())
                                .value(ConditionalExpression.Operator.LT.name())
                                .value(ConditionalExpression.Operator.LTE.name())
                                .build())))
            .build();

    return ImmutableSet.<GraphQLNamedInputType>builder()
        .add(BIG_INT_SCALAR)
        .add(FLOAT_32_SCALAR)
        .add(
            newEnum()
                .name("Order")
                .value(Scan.Ordering.Order.ASC.name())
                .value(Scan.Ordering.Order.DESC.name())
                .build())
        .add(
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
                        .type(list(nonNull(conditionalExpressionInputObject))))
                .build())
        .add(
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
                        .type(list(nonNull(conditionalExpressionInputObject))))
                .build())
        .add(
            newEnum()
                .name("Consistency")
                .value(Consistency.SEQUENTIAL.name())
                .value(Consistency.EVENTUAL.name())
                .value(Consistency.LINEARIZABLE.name())
                .build())
        .build();
  }

  public static GraphQLDirective createTransactionDirective() {
    return GraphQLDirective.newDirective()
        .name(Constants.TRANSACTION_DIRECTIVE_NAME)
        .argument(
            GraphQLArgument.newArgument()
                .name(Constants.TRANSACTION_DIRECTIVE_TX_ID_ARGUMENT_NAME)
                .type(Scalars.GraphQLString))
        .argument(
            GraphQLArgument.newArgument()
                .name(Constants.TRANSACTION_DIRECTIVE_COMMIT_ARGUMENT_NAME)
                .type(Scalars.GraphQLBoolean))
        .validLocations(DirectiveLocation.MUTATION, DirectiveLocation.QUERY)
        .build();
  }
}
