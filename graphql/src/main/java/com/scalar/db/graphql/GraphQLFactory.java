package com.scalar.db.graphql;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.graphql.schema.CommonSchema;
import com.scalar.db.graphql.schema.TableGraphQLModel;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import graphql.GraphQL;
import graphql.Scalars;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GraphQLFactory {

  private final DistributedStorage storage;
  private final DistributedTransactionManager transactionManager;
  private final List<TableGraphQLModel> tableModels;

  private GraphQLFactory(
      DistributedStorage storage,
      DistributedTransactionManager transactionManager,
      List<TableGraphQLModel> tableModels) {
    this.storage = Objects.requireNonNull(storage);
    this.transactionManager = transactionManager;
    this.tableModels = tableModels;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private GraphQLObjectType createQueryObjectType() {
    GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name("Query");
    for (TableGraphQLModel tableModel : tableModels) {
      builder.field(tableModel.getQueryGetField());
      GraphQLFieldDefinition queryScanField = tableModel.getQueryScanField();
      if (queryScanField != null) {
        builder.field(queryScanField);
      }
    }
    return builder.build();
  }

  private GraphQLObjectType createMutationObjectType() {
    GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name("Mutation");
    for (TableGraphQLModel tableModel : tableModels) {
      builder.field(tableModel.getMutationPutField());
      builder.field(tableModel.getMutationDeleteField());
    }
    builder.field(
        GraphQLFieldDefinition.newFieldDefinition().name("commit").type(Scalars.GraphQLBoolean));
    builder.field(
        GraphQLFieldDefinition.newFieldDefinition().name("abort").type(Scalars.GraphQLBoolean));

    return builder.build();
  }

  private GraphQLCodeRegistry createGraphQLCodeRegistry(
      GraphQLObjectType queryObjectType, GraphQLObjectType mutationObjectType) {
    GraphQLCodeRegistry.Builder builder = GraphQLCodeRegistry.newCodeRegistry();
    // TODO: Add data fetchers
    return builder.build();
  }

  public GraphQL createGraphQL() {
    GraphQLObjectType queryObjectType = createQueryObjectType();
    GraphQLObjectType mutationObjectType = createMutationObjectType();
    GraphQLSchema.Builder schema =
        GraphQLSchema.newSchema()
            .query(queryObjectType)
            .mutation(mutationObjectType)
            .codeRegistry(createGraphQLCodeRegistry(queryObjectType, mutationObjectType));
    CommonSchema.createCommonGraphQLTypes().forEach(schema::additionalType);
    if (transactionManager != null) {
      schema.additionalDirective(CommonSchema.createTransactionDirective());
    }
    return GraphQL.newGraphQL(schema.build())
        // TODO: .instrumentation(new TransactionInstrumentation(transactionManager))
        .build();
  }

  public static final class Builder {
    private final List<String> namespaces = new ArrayList<>();
    private final List<String> tables = new ArrayList<>();
    private StorageFactory storageFactory;
    private TransactionFactory transactionFactory;

    private Builder() {}

    public Builder storageFactory(StorageFactory storageFactory) {
      this.storageFactory = storageFactory;
      return this;
    }

    public Builder transactionFactory(TransactionFactory transactionFactory) {
      this.transactionFactory = transactionFactory;
      return this;
    }

    public Builder table(String namespace, String table) {
      namespaces.add(namespace);
      tables.add(table);
      return this;
    }

    public GraphQLFactory build() throws ExecutionException {
      if (storageFactory == null) {
        throw new IllegalArgumentException("Need to specify storageFactory");
      }
      if (tables.isEmpty()) {
        throw new IllegalArgumentException("Need to specify at least one table");
      }

      DistributedStorageAdmin storageAdmin = storageFactory.getAdmin();
      ImmutableList.Builder<TableGraphQLModel> tableModelListBuilder = ImmutableList.builder();
      for (int i = 0; i < tables.size(); i++) {
        String namespace = namespaces.get(i);
        String table = tables.get(i);
        tableModelListBuilder.add(
            new TableGraphQLModel(
                namespace, table, storageAdmin.getTableMetadata(namespace, table)));
      }

      return new GraphQLFactory(
          storageFactory.getStorage(),
          transactionFactory != null ? transactionFactory.getTransactionManager() : null,
          tableModelListBuilder.build());
    }
  }
}
