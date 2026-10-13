package com.scalar.db.storage.dynamo;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class DynamoConditionalMutationIntegrationTest
    extends DistributedStorageConditionalMutationIntegrationTestBase {
  private Properties properties;

  @Override
  protected void initialize(String testName) {
    properties = getProperties(testName);
  }

  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Override
  protected List<OperatorAndDataType> getOperatorAndDataTypeListForTest() {
    return super.getOperatorAndDataTypeListForTest().stream()
        .filter(
            operatorAndDataType -> {
              // DynamoDB only supports the 'equal' and 'not equal' and 'is null' and 'is not null'
              // conditions for BOOLEAN type
              if (operatorAndDataType.getDataType() == DataType.BOOLEAN) {
                return operatorAndDataType.getOperator() == Operator.EQ
                    || operatorAndDataType.getOperator() == Operator.NE
                    || operatorAndDataType.getOperator() == Operator.IS_NULL
                    || operatorAndDataType.getOperator() == Operator.IS_NOT_NULL;
              }
              return true;
            })
        .collect(Collectors.toList());
  }

  @Override
  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    if (dataType == DataType.DOUBLE) {
      return DynamoTestUtils.getRandomDynamoDoubleColumn(random, columnName);
    }
    return super.getColumnWithRandomValue(random, columnName, dataType);
  }

  @Test
  public void
      put_withPutIfNotEqualWhenColumnIsStoredAsNullTypeAttribute_shouldThrowNoMutationException()
          throws Exception {
    // Arrange
    Key partitionKey = Key.ofText("pkey", "legacy");
    storage.put(
        Put.newBuilder()
            .namespace(getNamespace())
            .table(TABLE)
            .partitionKey(partitionKey)
            .intValue("c2", 1)
            .build());
    setToNullTypeAttribute("c2");
    Get get =
        Get.newBuilder().namespace(getNamespace()).table(TABLE).partitionKey(partitionKey).build();
    Optional<Result> before = storage.get(get);
    assertThat(before).isPresent();
    assertThat(before.get().isNull("c2")).isTrue();

    Put put =
        Put.newBuilder()
            .namespace(getNamespace())
            .table(TABLE)
            .partitionKey(partitionKey)
            .bigIntValue("c3", 2L)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column("c2").isNotEqualToInt(2)).build())
            .build();

    // Act Assert
    assertThatThrownBy(() -> storage.put(put)).isInstanceOf(NoMutationException.class);
  }

  /** Rewrites a column of the only item in the table the way versions before #3326 stored NULL. */
  private void setToNullTypeAttribute(String columnName) {
    DynamoConfig config = new DynamoConfig(new DatabaseConfig(properties));
    DynamoDbClientBuilder builder = DynamoDbClient.builder();
    config.getEndpointOverride().ifPresent(e -> builder.endpointOverride(URI.create(e)));
    try (DynamoDbClient client =
        builder
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        config.getAccessKeyId(), config.getSecretAccessKey())))
            .region(Region.of(config.getRegion()))
            .build()) {
      String tableName =
          getFullTableName(
              Namespace.of(config.getNamespacePrefix().orElse(""), getNamespace()).prefixed(),
              TABLE);
      List<Map<String, AttributeValue>> items =
          client
              .scan(ScanRequest.builder().tableName(tableName).consistentRead(true).build())
              .items();
      assertThat(items).hasSize(1);

      Map<String, AttributeValue> key = new HashMap<>();
      key.put(DynamoOperation.PARTITION_KEY, items.get(0).get(DynamoOperation.PARTITION_KEY));
      Map<String, String> names = new HashMap<>();
      names.put("#c", columnName);
      Map<String, AttributeValue> values = new HashMap<>();
      values.put(":n", AttributeValue.builder().nul(true).build());
      client.updateItem(
          UpdateItemRequest.builder()
              .tableName(tableName)
              .key(key)
              .updateExpression("SET #c = :n")
              .expressionAttributeNames(names)
              .expressionAttributeValues(values)
              .build());
    }
  }
}
