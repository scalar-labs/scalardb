package com.scalar.db.graphql.schema;

import static graphql.schema.GraphQLEnumType.newEnum;
import static graphql.schema.GraphQLInputObjectField.newInputObjectField;
import static graphql.schema.GraphQLInputObjectType.newInputObject;
import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLNonNull.nonNull;

import com.google.common.collect.ImmutableSet;
import graphql.Scalars;
import graphql.introspection.Introspection.DirectiveLocation;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLNamedInputType;
import java.util.Set;

public class CommonSchema {
  public static Set<GraphQLNamedInputType> createCommonGraphQLTypes() {
    GraphQLInputObjectType conditionalExpressionInputObject =
        newInputObject()
            .name("ConditionalExpression")
            .field(newInputObjectField().name("name").type(nonNull(Scalars.GraphQLString)))
            .field(newInputObjectField().name("intValue").type(Scalars.GraphQLInt))
            .field(newInputObjectField().name("floatValue").type(Scalars.GraphQLFloat))
            .field(newInputObjectField().name("stringValue").type(Scalars.GraphQLString))
            .field(newInputObjectField().name("booleanValue").type(Scalars.GraphQLBoolean))
            .field(
                newInputObjectField()
                    .name("operator")
                    .type(
                        nonNull(
                            newEnum()
                                .name("ConditionalExpressionOperator")
                                .value(ConditionalExpressionOperator.EQ.name())
                                .value(ConditionalExpressionOperator.NE.name())
                                .value(ConditionalExpressionOperator.GT.name())
                                .value(ConditionalExpressionOperator.GTE.name())
                                .value(ConditionalExpressionOperator.LT.name())
                                .value(ConditionalExpressionOperator.LTE.name())
                                .build())))
            .build();

    return ImmutableSet.<GraphQLNamedInputType>builder()
        .add(
            newEnum()
                .name("ScanOrderingOrder")
                .value(ScanOrderingOrder.ASC.name())
                .value(ScanOrderingOrder.DESC.name())
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
        .build();
  }

  public static GraphQLDirective createTransactionDirective() {
    return GraphQLDirective.newDirective()
        .name(Constants.TRANSACTION_DIRECTIVE_NAME)
        .argument(
            GraphQLArgument.newArgument()
                .name(Constants.TRANSACTION_DIRECTIVE_TX_ID_ARGUMENT_NAME)
                .type(Scalars.GraphQLString)
                .build())
        .validLocation(DirectiveLocation.MUTATION)
        .build();
  }
}
