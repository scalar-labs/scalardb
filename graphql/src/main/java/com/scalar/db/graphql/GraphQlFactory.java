package com.scalar.db.graphql;

import static graphql.schema.FieldCoordinates.coordinates;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.graphql.datafetcher.DataFetcherHelper;
import com.scalar.db.graphql.datafetcher.MutationBulkDeleteDataFetcher;
import com.scalar.db.graphql.datafetcher.MutationBulkPutDataFetcher;
import com.scalar.db.graphql.datafetcher.MutationDeleteDataFetcher;
import com.scalar.db.graphql.datafetcher.MutationMutateDataFetcher;
import com.scalar.db.graphql.datafetcher.MutationPutDataFetcher;
import com.scalar.db.graphql.datafetcher.QueryGetDataFetcher;
import com.scalar.db.graphql.datafetcher.QueryScanDataFetcher;
import com.scalar.db.graphql.schema.CommonSchema;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import graphql.GraphQL;
import graphql.Scalars;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class GraphQlFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphQlFactory.class);
  private final DistributedStorage storage;
  private final DistributedTransactionManager transactionManager;
  private final List<TableGraphQlModel> tableModels;

  private GraphQlFactory(
      DistributedStorage storage,
      DistributedTransactionManager transactionManager,
      List<TableGraphQlModel> tableModels) {
    this.storage = Objects.requireNonNull(storage);
    this.transactionManager = transactionManager;
    this.tableModels = tableModels;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private GraphQLObjectType createQueryObjectType() {
    GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name("Query");
    for (TableGraphQlModel tableModel : tableModels) {
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
    for (TableGraphQlModel tableModel : tableModels) {
      builder
          .field(tableModel.getMutationPutField())
          .field(tableModel.getMutationBulkPutField())
          .field(tableModel.getMutationDeleteField())
          .field(tableModel.getMutationBulkDeleteField())
          .field(tableModel.getMutationMutateField());
    }
    builder
        .field(
            GraphQLFieldDefinition.newFieldDefinition()
                .name("commit")
                .type(GraphQLNonNull.nonNull(Scalars.GraphQLBoolean)))
        .field(
            GraphQLFieldDefinition.newFieldDefinition()
                .name("abort")
                .type(GraphQLNonNull.nonNull(Scalars.GraphQLBoolean)));

    return builder.build();
  }

  private GraphQLCodeRegistry createGraphQLCodeRegistry(
      GraphQLObjectType queryObjectType, GraphQLObjectType mutationObjectType) {
    GraphQLCodeRegistry.Builder builder = GraphQLCodeRegistry.newCodeRegistry();
    for (TableGraphQlModel tableModel : tableModels) {
      DataFetcherHelper helper = new DataFetcherHelper(tableModel);
      builder
          .dataFetcher(
              coordinates(queryObjectType, tableModel.getQueryGetField()),
              new QueryGetDataFetcher(storage, helper))
          .dataFetcher(
              coordinates(mutationObjectType, tableModel.getMutationPutField()),
              new MutationPutDataFetcher(storage, helper))
          .dataFetcher(
              coordinates(mutationObjectType, tableModel.getMutationBulkPutField()),
              new MutationBulkPutDataFetcher(storage, helper))
          .dataFetcher(
              coordinates(mutationObjectType, tableModel.getMutationDeleteField()),
              new MutationDeleteDataFetcher(storage, helper))
          .dataFetcher(
              coordinates(mutationObjectType, tableModel.getMutationBulkDeleteField()),
              new MutationBulkDeleteDataFetcher(storage, helper))
          .dataFetcher(
              coordinates(mutationObjectType, tableModel.getMutationMutateField()),
              new MutationMutateDataFetcher(storage, helper));
      if (tableModel.getQueryScanField() != null) {
        builder.dataFetcher(
            coordinates(queryObjectType, tableModel.getQueryScanField()),
            new QueryScanDataFetcher(storage, helper));
      }
    }
    return builder.build();
  }

  public GraphQL createGraphQL() {
    GraphQLObjectType queryObjectType = createQueryObjectType();
    GraphQLObjectType mutationObjectType = createMutationObjectType();
    GraphQLSchema.Builder schemaBuilder =
        GraphQLSchema.newSchema()
            .query(queryObjectType)
            .mutation(mutationObjectType)
            .codeRegistry(createGraphQLCodeRegistry(queryObjectType, mutationObjectType));
    CommonSchema.createCommonGraphQLTypes().forEach(schemaBuilder::additionalType);
    if (transactionManager != null) {
      schemaBuilder.additionalDirective(CommonSchema.createTransactionDirective());
    } else {
      LOGGER.info("@transaction directive is disabled since transactionManager is not given");
    }

    GraphQLSchema schema = schemaBuilder.build();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("GraphQL schema generated: {}", new SchemaPrinter().print(schema));
    }

    return GraphQL.newGraphQL(schema)
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

    public GraphQlFactory build() throws ExecutionException {
      if (storageFactory == null) {
        throw new IllegalStateException("Need to specify storageFactory");
      }
      if (tables.isEmpty()) {
        throw new IllegalStateException("Need to specify at least one table");
      }

      DistributedStorageAdmin storageAdmin = storageFactory.getAdmin();
      ImmutableList.Builder<TableGraphQlModel> tableModelListBuilder = ImmutableList.builder();
      for (int i = 0; i < tables.size(); i++) {
        String namespace = namespaces.get(i);
        String table = tables.get(i);
        TableGraphQlModel tableGraphQlModel =
            new TableGraphQlModel(
                namespace, table, storageAdmin.getTableMetadata(namespace, table));
        tableModelListBuilder.add(tableGraphQlModel);
        LOGGER.debug("table added: {}.{}", namespace, table);
      }

      return new GraphQlFactory(
          storageFactory.getStorage(),
          transactionFactory != null ? transactionFactory.getTransactionManager() : null,
          tableModelListBuilder.build());
    }
  }
}
