package com.scalar.db.graphql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.graphql.schema.CommonSchema;
import com.scalar.db.graphql.schema.Constants;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import graphql.GraphQL;
import graphql.schema.GraphQLNamedInputType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class GraphQLFactoryTest {
  private static final String NAMESPACE_NAME = "namespace_1";
  private static final String TABLE_NAME_1 = "table_1";
  private static final String TABLE_NAME_2 = "table_2";
  private static final String COLUMN_NAME_1 = "column_1";
  private static final String COLUMN_NAME_2 = "column_2";
  private static final String COLUMN_NAME_3 = "column_3";

  @Mock private StorageFactory storageFactory;
  @Mock private TransactionFactory transactionFactory;
  @Mock private DistributedStorage storage;
  @Mock private DistributedStorageAdmin storageAdmin;
  @Mock private DistributedTransactionManager transactionManager;

  @Before
  public void setUp() {
    initMocks(this);

    // Arrange
    when(storageFactory.getStorage()).thenReturn(storage);
    when(storageFactory.getAdmin()).thenReturn(storageAdmin);
    when(transactionFactory.getTransactionManager()).thenReturn(transactionManager);
  }

  @Test
  public void build_AllParametersGiven_ShouldReturnFactory() throws Exception {
    // Arrange
    when(storageAdmin.getTableMetadata(NAMESPACE_NAME, TABLE_NAME_1))
        .thenReturn(mock(TableMetadata.class));

    // Act
    GraphQLFactory factory =
        GraphQLFactory.newBuilder()
            .storageFactory(storageFactory)
            .transactionFactory(transactionFactory)
            .table(NAMESPACE_NAME, TABLE_NAME_1)
            .build();

    // Assert
    assertThat(factory).isNotNull();
    verify(storageFactory).getAdmin();
    verify(storageFactory).getStorage();
    verify(transactionFactory).getTransactionManager();
  }

  @Test
  public void build_StorageFactoryNotGiven_ShouldThrowIllegalStateException() {
    // Act Assert
    assertThatThrownBy(
            () -> GraphQLFactory.newBuilder().table(NAMESPACE_NAME, TABLE_NAME_1).build())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void build_NoTableGiven_ShouldThrowIllegalStateException() {
    // Act Assert
    assertThatThrownBy(() -> GraphQLFactory.newBuilder().storageFactory(storageFactory).build())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void build_FetchingTableMetadataFailed_ShouldThrowTheSameExecutionException()
      throws Exception {
    // Arrange
    Exception exception = new ExecutionException("Error");
    when(storageAdmin.getTableMetadata(NAMESPACE_NAME, TABLE_NAME_1)).thenThrow(exception);

    // Act Assert
    assertThatThrownBy(
            () ->
                GraphQLFactory.newBuilder()
                    .storageFactory(storageFactory)
                    .table(NAMESPACE_NAME, TABLE_NAME_1)
                    .build())
        .isSameAs(exception);
  }

  private void configureTableMetadata() throws Exception {
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_NAME_1, DataType.INT)
            .addColumn(COLUMN_NAME_2, DataType.TEXT)
            .addColumn(COLUMN_NAME_3, DataType.FLOAT)
            .addPartitionKey(COLUMN_NAME_1)
            .addClusteringKey(COLUMN_NAME_2)
            .build();
    when(storageAdmin.getTableMetadata(NAMESPACE_NAME, TABLE_NAME_1)).thenReturn(tableMetadata);
  }

  @Test
  public void createGraphQL_AllParametersGiven_ShouldReturnGraphQL() throws Exception {
    // Arrange
    configureTableMetadata();

    // Act
    GraphQLFactory factory =
        GraphQLFactory.newBuilder()
            .storageFactory(storageFactory)
            .transactionFactory(transactionFactory)
            .table(NAMESPACE_NAME, TABLE_NAME_1)
            .build();
    GraphQL graphql = factory.createGraphQL();

    // Assert
    GraphQLSchema schema = graphql.getGraphQLSchema();
    assertThat(schema.containsType(TABLE_NAME_1)).isEqualTo(true);
    assertThat(schema.containsType("Query")).isEqualTo(true);
    assertThat(schema.containsType("Mutation")).isEqualTo(true);
    assertThat(schema.getDirective(Constants.TRANSACTION_DIRECTIVE_NAME)).isNotNull();
    for (GraphQLNamedInputType type : CommonSchema.createCommonGraphQLTypes()) {
      schema.containsType(type.getName());
    }
  }

  @Test
  public void
      createGraphQL_NoTransactionFactoryGiven_ShouldReturnGraphQLWithoutTransactionDirective()
          throws Exception {
    // Arrange
    configureTableMetadata();

    // Act
    GraphQLFactory factory =
        GraphQLFactory.newBuilder()
            .storageFactory(storageFactory)
            .table(NAMESPACE_NAME, TABLE_NAME_1)
            .build();
    GraphQL graphql = factory.createGraphQL();

    // Assert
    GraphQLSchema schema = graphql.getGraphQLSchema();
    assertThat(schema.getDirective(Constants.TRANSACTION_DIRECTIVE_NAME)).isNull();
  }

  @Test
  public void createGraphQL_MultipleTablesGiven_ShouldReturnGraphQLWithTableTypes()
      throws Exception {
    // Arrange
    configureTableMetadata();
    TableMetadata secondTableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_NAME_1, DataType.INT)
            .addColumn(COLUMN_NAME_2, DataType.INT)
            .addPartitionKey(COLUMN_NAME_1)
            .build();
    when(storageAdmin.getTableMetadata(NAMESPACE_NAME, TABLE_NAME_2))
        .thenReturn(secondTableMetadata);

    // Act
    GraphQLFactory factory =
        GraphQLFactory.newBuilder()
            .storageFactory(storageFactory)
            .table(NAMESPACE_NAME, TABLE_NAME_1)
            .table(NAMESPACE_NAME, TABLE_NAME_2)
            .build();
    GraphQL graphql = factory.createGraphQL();

    // Assert
    GraphQLSchema schema = graphql.getGraphQLSchema();
    assertThat(schema.containsType(TABLE_NAME_1)).isEqualTo(true);
    assertThat(schema.containsType(TABLE_NAME_2)).isEqualTo(true);
  }

  @Test
  public void
      createGraphQL_TableWithoutClusteringKeysGiven_ShouldReturnGraphQLWithoutScanOperation()
          throws Exception {
    // Arrange
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_NAME_1, DataType.INT)
            .addColumn(COLUMN_NAME_2, DataType.INT)
            .addPartitionKey(COLUMN_NAME_1)
            .addPartitionKey(COLUMN_NAME_2)
            .build();
    when(storageAdmin.getTableMetadata(NAMESPACE_NAME, TABLE_NAME_1)).thenReturn(tableMetadata);

    // Act
    GraphQLFactory factory =
        GraphQLFactory.newBuilder()
            .storageFactory(storageFactory)
            .table(NAMESPACE_NAME, TABLE_NAME_1)
            .build();
    GraphQL graphql = factory.createGraphQL();

    // Assert
    GraphQLSchema schema = graphql.getGraphQLSchema();
    assertThat(schema.containsType(TABLE_NAME_1)).isEqualTo(true);
    GraphQLObjectType query = (GraphQLObjectType) schema.getType("Query");
    assertThat(query).isNotNull();
    assertThat(query.getFieldDefinition(TABLE_NAME_1 + "_get")).isNotNull();
    assertThat(query.getFieldDefinition(TABLE_NAME_1 + "_scan")).isNull();
  }
}
