#!/usr/bin/env bash

set -e -o pipefail; [[ -n "$DEBUG" ]] && set -x

# This script sets up the required IAM policy for DynamoDB permission integration tests.
# It creates or updates a policy with the minimum permissions needed for ScalarDB operations.
#
# Required environment variables:
#   AWS_ACCESS_KEY_ID
#   AWS_SECRET_ACCESS_KEY
#   AWS_DEFAULT_REGION (or AWS_REGION)
#
# Usage:
#   ./ci/dynamo-permission-setup.sh

POLICY_NAME="test-dynamodb-permissions"

# Get current user name from caller identity
USER_NAME=$(aws sts get-caller-identity --query 'Arn' --output text | cut -d'/' -f2)
echo "Setting up DynamoDB test permissions for user: $USER_NAME"

# Check if the policy is already attached to the user
POLICY_ARN=$(aws iam list-attached-user-policies \
  --user-name "$USER_NAME" \
  --query "AttachedPolicies[?PolicyName=='${POLICY_NAME}'].PolicyArn" \
  --output text 2>/dev/null || echo "")

# Policy document with minimum required permissions for ScalarDB DynamoDB operations
# TODO Remove dynamodb:TagResource
POLICY_DOCUMENT='{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "dynamodb:ConditionCheckItem",
      "dynamodb:PutItem",
      "dynamodb:ListTables",
      "dynamodb:DeleteItem",
      "dynamodb:Scan",
      "dynamodb:Query",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteTable",
      "dynamodb:UpdateContinuousBackups",
      "dynamodb:CreateTable",
      "dynamodb:DescribeTable",
      "dynamodb:GetItem",
      "dynamodb:DescribeContinuousBackups",
      "dynamodb:UpdateTable",
      "application-autoscaling:RegisterScalableTarget",
      "application-autoscaling:DeleteScalingPolicy",
      "application-autoscaling:PutScalingPolicy",
      "application-autoscaling:DeregisterScalableTarget",
      "application-autoscaling:TagResource",
      "dynamodb:TagResource",
    ],
    "Resource": "*"
  }]
}'

if [[ -n "$POLICY_ARN" ]]; then
  echo "Policy already attached, updating to latest version..."

  # Delete non-default policy versions (AWS limits to 5 versions max)
  NON_DEFAULT_VERSIONS=$(aws iam list-policy-versions \
    --policy-arn "$POLICY_ARN" \
    --query 'Versions[?!IsDefaultVersion].VersionId' \
    --output text)

  for VERSION_ID in $NON_DEFAULT_VERSIONS; do
    echo "Deleting old policy version: $VERSION_ID"
    aws iam delete-policy-version \
      --policy-arn "$POLICY_ARN" \
      --version-id "$VERSION_ID"
  done

  # Create new version and set as default
  aws iam create-policy-version \
    --policy-arn "$POLICY_ARN" \
    --policy-document "$POLICY_DOCUMENT" \
    --set-as-default > /dev/null

  echo "Policy updated successfully"
else
  echo "Creating and attaching new policy..."

  # Create the policy
  POLICY_ARN=$(aws iam create-policy \
    --policy-name "$POLICY_NAME" \
    --policy-document "$POLICY_DOCUMENT" \
    --query 'Policy.Arn' \
    --output text)

  # Attach it to the user
  aws iam attach-user-policy \
    --user-name "$USER_NAME" \
    --policy-arn "$POLICY_ARN"

  echo "Policy created and attached successfully"
fi

echo "Policy ARN: $POLICY_ARN"
echo "DynamoDB permission setup complete"