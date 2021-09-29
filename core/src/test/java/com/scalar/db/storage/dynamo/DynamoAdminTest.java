package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.applicationautoscaling.ApplicationAutoScalingClient;
import software.amazon.awssdk.services.applicationautoscaling.model.PutScalingPolicyRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.PutScalingPolicyResponse;
import software.amazon.awssdk.services.applicationautoscaling.model.RegisterScalableTargetRequest;
import software.amazon.awssdk.services.applicationautoscaling.model.RegisterScalableTargetResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

public class DynamoAdminTest {

  private static final String NAMESPACE = "namespace";
  private static final String TABLE = "table";
  private static final String FULL_TABLE_NAME = NAMESPACE + "." + TABLE;

  @Mock private DynamoDbClient client;
  @Mock private ApplicationAutoScalingClient applicationAutoScalingClient;
  private DynamoAdmin admin;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    admin = new DynamoAdmin(client, applicationAutoScalingClient, Optional.empty());
  }

  @Test
  public void getTableMetadata_ClientShouldBeCalledProperly() throws ExecutionException {
    // Arrange
    Map<String, AttributeValue> expectedKey = new HashMap<>();
    expectedKey.put("table", AttributeValue.builder().s(FULL_TABLE_NAME).build());

    GetItemResponse response = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item()).thenReturn(Collections.emptyMap());

    DynamoAdmin admin = new DynamoAdmin(client, applicationAutoScalingClient, Optional.empty());

    // Act
    admin.getTableMetadata(NAMESPACE, TABLE);

    // Assert
    ArgumentCaptor<GetItemRequest> captor = ArgumentCaptor.forClass(GetItemRequest.class);
    verify(client).getItem(captor.capture());
    GetItemRequest actualRequest = captor.getValue();
    assertThat(actualRequest.tableName()).isEqualTo("scalardb.metadata");
    assertThat(actualRequest.key()).isEqualTo(expectedKey);
  }

  @Test
  public void dropNamespace_ShouldDropAllTablesInNamespace() throws ExecutionException {
    // Arrange
    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(client.listTables()).thenReturn(listTablesResponse);
    when(listTablesResponse.tableNames())
        .thenReturn(
            ImmutableList.<String>builder()
                .add(NAMESPACE + ".tb1")
                .add(NAMESPACE + ".tb2")
                .add(NAMESPACE + ".tb3")
                .build());

    GetItemResponse response = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item()).thenReturn(Collections.emptyMap());

    DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(describeTableResponse);
    TableDescription tableDescription = mock(TableDescription.class);
    when(tableDescription.itemCount()).thenReturn((long) 1);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    when(tableDescription.tableStatus()).thenReturn(TableStatus.ACTIVE);

    // Act
    admin.dropNamespace(NAMESPACE);

    // Assert
    verify(client, times(3)).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  public void
      createTable_WithCorrectParamsAndMetadataTableNotExist_ShouldCreateTableAndMetadataTable()
          throws ExecutionException {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder().addPartitionKey("c1").addColumn("c1", DataType.INT).build();

    when(applicationAutoScalingClient.registerScalableTarget(
            any(RegisterScalableTargetRequest.class)))
        .thenReturn(RegisterScalableTargetResponse.builder().build());
    when(applicationAutoScalingClient.putScalingPolicy(any(PutScalingPolicyRequest.class)))
        .thenReturn(PutScalingPolicyResponse.builder().build());

    DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(describeTableResponse);
    TableDescription tableDescription = mock(TableDescription.class);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    when(tableDescription.tableStatus()).thenReturn(TableStatus.ACTIVE);

    // for the table metadata table
    describeTableResponse = mock(DescribeTableResponse.class);
    tableDescription = mock(TableDescription.class);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    when(tableDescription.tableStatus()).thenReturn(TableStatus.ACTIVE);
    when(client.describeTable(any(DescribeTableRequest.class)))
        .thenThrow(ResourceNotFoundException.class)
        .thenReturn(describeTableResponse);

    // Act
    admin.createTable(NAMESPACE, TABLE, metadata, Collections.emptyMap());

    // Assert
    verify(client, times(2)).createTable(any(CreateTableRequest.class));
    verify(client).putItem(any(PutItemRequest.class));
  }

  @Test
  public void createTable_WithCorrectParamsAndMetadataTableExists_ShouldCreateTable()
      throws ExecutionException {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder().addPartitionKey("c1").addColumn("c1", DataType.INT).build();

    when(applicationAutoScalingClient.registerScalableTarget(
            any(RegisterScalableTargetRequest.class)))
        .thenReturn(RegisterScalableTargetResponse.builder().build());
    when(applicationAutoScalingClient.putScalingPolicy(any(PutScalingPolicyRequest.class)))
        .thenReturn(PutScalingPolicyResponse.builder().build());

    DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(describeTableResponse);
    TableDescription tableDescription = mock(TableDescription.class);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    when(tableDescription.tableStatus()).thenReturn(TableStatus.ACTIVE);

    // for the table metadata table
    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(describeTableResponse);

    // Act
    admin.createTable(NAMESPACE, TABLE, metadata, Collections.emptyMap());

    // Assert
    verify(client).createTable(any(CreateTableRequest.class));
    verify(client).putItem(any(PutItemRequest.class));
  }

  @Test
  public void dropTable_WithExistingTable_ShouldDropTableAndDeleteMetadata()
      throws ExecutionException {
    // Arrange
    GetItemResponse response = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item()).thenReturn(Collections.emptyMap());

    // for the table metadata table
    DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
    TableDescription tableDescription = mock(TableDescription.class);
    when(tableDescription.itemCount()).thenReturn((long) 1);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(describeTableResponse);

    // Act
    admin.dropTable(NAMESPACE, TABLE);

    // Assert
    verify(client).deleteTable(any(DeleteTableRequest.class));
    verify(client).deleteItem(any(DeleteItemRequest.class));
  }

  @Test
  public void dropTable_WithEmptyMetadataTableAfterDeletion_ShouldDropTableAndDropMetadataTable()
      throws ExecutionException {
    // Arrange
    GetItemResponse response = mock(GetItemResponse.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(response);
    when(response.item()).thenReturn(Collections.emptyMap());

    // for the table metadata table
    DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
    TableDescription tableDescription = mock(TableDescription.class);
    when(tableDescription.itemCount()).thenReturn((long) 0);
    when(describeTableResponse.table()).thenReturn(tableDescription);
    when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(describeTableResponse);

    // Act
    admin.dropTable(NAMESPACE, TABLE);

    // Assert
    verify(client, times(2)).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  public void truncateTable_WithExistingRecords_ShouldDeleteAllRecords() throws ExecutionException {
    // Arrange
    ScanResponse scanResponse = mock(ScanResponse.class);
    when(scanResponse.items())
        .thenReturn(
            ImmutableList.<Map<String, AttributeValue>>builder()
                .add(
                    ImmutableMap.<String, AttributeValue>builder()
                        .put("pKey1", AttributeValue.builder().s("pKey1Val").build())
                        .build())
                .add(
                    ImmutableMap.<String, AttributeValue>builder()
                        .put("pKey2", AttributeValue.builder().s("pKey2Val").build())
                        .build())
                .add(
                    ImmutableMap.<String, AttributeValue>builder()
                        .put("pKey3", AttributeValue.builder().s("pKey3Val").build())
                        .build())
                .build());
    when(scanResponse.lastEvaluatedKey()).thenReturn(Collections.emptyMap());
    when(client.scan(any(ScanRequest.class))).thenReturn(scanResponse);

    // Act
    admin.truncateTable(NAMESPACE, TABLE);

    // Assert
    verify(client, times(3)).deleteItem(any(DeleteItemRequest.class));
  }

  @Test
  public void getNamespaceTableNames_ShouldReturnTableNamesCorrectly() throws ExecutionException {
    // Arrange
    Set<String> tableNames = ImmutableSet.of("t1", "t2");

    ListTablesResponse listTablesResponse = mock(ListTablesResponse.class);
    when(client.listTables()).thenReturn(listTablesResponse);
    when(listTablesResponse.tableNames())
        .thenReturn(ImmutableList.of(NAMESPACE + ".t1", NAMESPACE + ".t2"));

    // Act
    Set<String> actualNames = admin.getNamespaceTableNames(NAMESPACE);

    // Assert
    assertThat(actualNames).isEqualTo(tableNames);
  }
}
