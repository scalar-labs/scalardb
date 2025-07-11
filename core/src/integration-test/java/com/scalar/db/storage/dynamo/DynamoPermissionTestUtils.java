package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.PermissionTestUtils;
import java.util.Optional;
import java.util.Properties;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.policybuilder.iam.IamEffect;
import software.amazon.awssdk.policybuilder.iam.IamPolicy;
import software.amazon.awssdk.policybuilder.iam.IamResource;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.AttachUserPolicyRequest;
import software.amazon.awssdk.services.iam.model.AttachedPolicy;
import software.amazon.awssdk.services.iam.model.CreatePolicyRequest;
import software.amazon.awssdk.services.iam.model.CreatePolicyVersionRequest;
import software.amazon.awssdk.services.iam.model.DeletePolicyVersionRequest;
import software.amazon.awssdk.services.iam.model.ListAttachedUserPoliciesRequest;
import software.amazon.awssdk.services.iam.model.ListPolicyVersionsRequest;
import software.amazon.awssdk.services.iam.model.User;

public class DynamoPermissionTestUtils implements PermissionTestUtils {
  public static final int SLEEP_BETWEEN_TESTS_SECONDS = 10;
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
      User user = client.getUser().user();
      Optional<String> attachedPolicyArn = getAttachedPolicyArn(user.userName());
      if (attachedPolicyArn.isPresent()) {
        String policyArn = attachedPolicyArn.get();
        try {
          deleteStalePolicyVersions(policyArn);
          createNewPolicyVersion(policyArn);
        } catch (SdkException e) {
          throw new RuntimeException(
              String.format(
                  "Failed to update policy for user: %s, policyArn: %s", userName, policyArn),
              e);
        }
      } else {
        String policyArn = createNewPolicy();
        try {
          client.attachUserPolicy(
              AttachUserPolicyRequest.builder()
                  .userName(user.userName())
                  .policyArn(policyArn)
                  .build());
        } catch (SdkException e) {
          throw new RuntimeException(
              String.format(
                  "Failed to attach new policy for user: %s, policyArn: %s", userName, policyArn),
              e);
        }
      }
    } catch (SdkException e) {
      throw new RuntimeException(
          String.format("Failed to grant required permissions for user: %s", userName), e);
    }
  }

  @Override
  public void close() {
    client.close();
  }

  private Optional<String> getAttachedPolicyArn(String userName) {
    AttachedPolicy attachedPolicy =
        client
            .listAttachedUserPolicies(
                ListAttachedUserPoliciesRequest.builder().userName(userName).build())
            .attachedPolicies().stream()
            .filter(policy -> policy.policyName().equals(DynamoPermissionTestUtils.IAM_POLICY_NAME))
            .findFirst()
            .orElse(null);
    return Optional.ofNullable(attachedPolicy).map(AttachedPolicy::policyArn);
  }

  private String createNewPolicy() {
    return client
        .createPolicy(
            CreatePolicyRequest.builder()
                .policyName(IAM_POLICY_NAME)
                .policyDocument(POLICY.toJson())
                .build())
        .policy()
        .arn();
  }

  private void deleteStalePolicyVersions(String policyArn) {
    client.listPolicyVersions(ListPolicyVersionsRequest.builder().policyArn(policyArn).build())
        .versions().stream()
        .filter(version -> !version.isDefaultVersion())
        .forEach(
            version ->
                client.deletePolicyVersion(
                    DeletePolicyVersionRequest.builder()
                        .policyArn(policyArn)
                        .versionId(version.versionId())
                        .build()));
  }

  private void createNewPolicyVersion(String policyArn) {
    client.createPolicyVersion(
        CreatePolicyVersionRequest.builder()
            .policyArn(policyArn)
            .policyDocument(POLICY.toJson())
            .setAsDefault(true)
            .build());
  }
}
