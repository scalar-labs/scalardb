package com.scalar.db.storage.dynamo;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
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
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

public class DynamoAdminTest {
  private static final String NAMESPACE_PREFIX = "ns_pfx";
  @Mock DynamoTableMetadataManager metadataManager;
  @Mock DynamoDbClient client;
  @Mock ApplicationAutoScalingClient applicationAutoScalingClient;

  DynamoAdmin admin;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    admin =
        new DynamoAdmin(
            client, applicationAutoScalingClient, metadataManager, Optional.of(NAMESPACE_PREFIX));
  }

  @Test
  public void
      getTableMetadata_ConstructedWithoutNamespacePrefix_ShouldBeCalledWithoutNamespacePrefix()
          throws ExecutionException {
    // Arrange
    Optional<String> namespacePrefix = Optional.empty();
    String namespace = "ns";
    String table = "table";

    DynamoAdmin admin = new DynamoAdmin(metadataManager, namespacePrefix);

    // Act
    admin.getTableMetadata(namespace, table);

    // Assert
    verify(metadataManager).getTableMetadata(namespace, table);
  }

  @Test
  public void getTableMetadata_ConstructedWithNamespacePrefix_ShouldBeCalledWithNamespacePrefix()
      throws ExecutionException {
    // Arrange
    Optional<String> namespacePrefix = Optional.of("prefix_");
    String namespace = "ns";
    String table = "table";

    DynamoAdmin admin = new DynamoAdmin(metadataManager, namespacePrefix);

    // Act
    admin.getTableMetadata(namespace, table);

    // Assert
    verify(metadataManager).getTableMetadata(namespacePrefix.get() + "_" + namespace, table);
  }

  @Test
  public void dropNameSpace_ShouldExecuteDropTable() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    when(metadataManager.getTableNames(namespace))
        .thenReturn(ImmutableSet.<String>builder().add("tb1").add("tb2").add("tb3").build());

    // Act
    admin.dropNamespace(namespace);

    // Assert
    verify(client, times(3)).deleteTable(any(DeleteTableRequest.class));
    verify(metadataManager, times(3)).deleteTableMetadata(anyString(), anyString());
  }

  @Test
  public void createTable_WithCorrectParams_ShouldCreateTable() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";
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

    // Act
    admin.createTable(namespace, table, metadata, Collections.emptyMap());

    // Assert
    verify(client).createTable(any(CreateTableRequest.class));
    verify(metadataManager).addTableMetadata(namespace, table, metadata);
  }

  @Test
  public void dropTable_WithExistingTable_ShouldDropTable() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";

    // Act
    admin.dropTable(namespace, table);

    // Assert
    verify(client).deleteTable(any(DeleteTableRequest.class));
    verify(metadataManager).deleteTableMetadata(namespace, table);
  }

  @Test
  public void truncateTable_WithExistingRecords_ShouldDeleteAllRecords() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";
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
    when(scanResponse.lastEvaluatedKey()).thenReturn(null);
    when(client.scan(any(ScanRequest.class))).thenReturn(scanResponse);

    // Act
    admin.truncateTable(namespace, table);

    // Assert
    verify(client, times(3)).deleteItem(any(DeleteItemRequest.class));
  }

  @Test
  public void getNamespaceTableNames_ShouldExecuteManagerMethod() throws ExecutionException {
    // Arrange
    String namespace = "ns";
    Set<String> tableNames = ImmutableSet.of("t1", "t2");
    when(metadataManager.getTableNames(anyString())).thenReturn(tableNames);

    // Act
    Set<String> actualNames = admin.getNamespaceTableNames(namespace);

    // Assert
    verify(metadataManager).getTableNames(namespace);
    Assertions.assertThat(actualNames).isEqualTo(tableNames);
  }
}
