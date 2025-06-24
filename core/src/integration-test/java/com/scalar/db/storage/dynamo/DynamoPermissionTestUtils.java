package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.PermissionTestUtils;
import java.util.Properties;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.policybuilder.iam.IamEffect;
import software.amazon.awssdk.policybuilder.iam.IamPolicy;
import software.amazon.awssdk.policybuilder.iam.IamResource;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.AttachUserPolicyRequest;
import software.amazon.awssdk.services.iam.model.CreatePolicyVersionRequest;
import software.amazon.awssdk.services.iam.model.User;

public class DynamoPermissionTestUtils implements PermissionTestUtils {
  private static final String IAM_POLICY_NAME = "test-dynamodb-permissions";
  private static final IamPolicy POLICY =
      IamPolicy.builder()
          .addStatement(
              s ->
                  s.effect(IamEffect.ALLOW)
                      .addAction("dynamodb:ConditionCheckItem")
                      .addAction("dynamodb:PutItem")
                      .addAction("dynamodb:ListTables")
                      .addAction("dynamodb:DeleteItem")
                      .addAction("dynamodb:Scan")
                      .addAction("dynamodb:Query")
                      .addAction("dynamodb:UpdateItem")
                      .addAction("dynamodb:DeleteTable")
                      .addAction("dynamodb:UpdateContinuousBackups")
                      .addAction("dynamodb:CreateTable")
                      .addAction("dynamodb:DescribeTable")
                      .addAction("dynamodb:GetItem")
                      .addAction("dynamodb:DescribeContinuousBackups")
                      .addAction("dynamodb:UpdateTable")
                      .addAction("application-autoscaling:RegisterScalableTarget")
                      .addAction("application-autoscaling:DeleteScalingPolicy")
                      .addAction("application-autoscaling:PutScalingPolicy")
                      .addAction("application-autoscaling:DeregisterScalableTarget")
                      .addAction("application-autoscaling:TagResource")
                      .addResource(IamResource.ALL))
          .build();
  private final IamClient client;

  public DynamoPermissionTestUtils(Properties properties) {
    DynamoConfig config = new DynamoConfig(new DatabaseConfig(properties));
    this.client =
        IamClient.builder()
            .region(Region.of(config.getRegion()))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        config.getAccessKeyId(), config.getSecretAccessKey())))
            .build();
  }

  @Override
  public void createNormalUser(String userName, String password) {
    // Do nothing for DynamoDB.
  }

  @Override
  public void dropNormalUser(String userName) {
    // Do nothing for DynamoDB.
  }

  @Override
  public void grantRequiredPermission(String userName) {
    try {
      // Get the account ID to construct the ARN\
      User user = client.getUser().user();
      String accountId = user.arn().split(":")[4];
      String policyArn = String.format("arn:aws:iam::%s:policy/%s", accountId, IAM_POLICY_NAME);

      // Create a new version of the existing policy
      client.createPolicyVersion(
          CreatePolicyVersionRequest.builder()
              .policyArn(policyArn)
              .policyDocument(POLICY.toJson())
              .setAsDefault(true)
              .build());

      // Attach the policy to the user
      client.attachUserPolicy(
          AttachUserPolicyRequest.builder().userName(user.userName()).policyArn(policyArn).build());
    } catch (Exception e) {
      throw new RuntimeException("Failed to grant required permissions", e);
    }
  }

  @Override
  public void close() {
    client.close();
  }
}
