package com.scalar.db.api;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;

/** A utility class to manipulate the operation attributes for attribute-based access control. */
public final class AbacOperationAttributes {

  private static final String OPERATION_ATTRIBUTE_PREFIX = "abac-";

  /** The prefix of the operation attribute key for the read tag. */
  public static final String READ_TAG_PREFIX = OPERATION_ATTRIBUTE_PREFIX + "read-tag-";

  /** The prefix of the operation attribute key for the write tag. */
  public static final String WRITE_TAG_PREFIX = OPERATION_ATTRIBUTE_PREFIX + "write-tag-";

  private AbacOperationAttributes() {}

  /**
   * Sets the specified read tag for the specified policy in the operation attributes.
   *
   * @param attributes the operation attributes
   * @param policyName the policy name
   * @param readTag the read tag
   */
  public static void setReadTag(Map<String, String> attributes, String policyName, String readTag) {
    attributes.put(READ_TAG_PREFIX + policyName, readTag);
  }

  /**
   * Sets the specified read tag for the specified policy in the operation attributes builder.
   *
   * @param attributesBuilder the operation attributes builder
   * @param policyName the policy name
   * @param readTag the read tag
   */
  public static void setReadTag(
      ImmutableMap.Builder<String, String> attributesBuilder, String policyName, String readTag) {
    attributesBuilder.put(READ_TAG_PREFIX + policyName, readTag);
  }

  /**
   * Clears the read tag for the specified policy from the operation attributes.
   *
   * @param attributes the operation attributes
   * @param policyName the policy name
   */
  public static void clearReadTag(Map<String, String> attributes, String policyName) {
    attributes.remove(READ_TAG_PREFIX + policyName);
  }

  /**
   * Clears all the read tags from the operation attributes.
   *
   * @param attributes the operation attributes
   */
  public static void clearReadTags(Map<String, String> attributes) {
    attributes.entrySet().removeIf(e -> e.getKey().startsWith(READ_TAG_PREFIX));
  }

  /**
   * Sets the specified write tag for the specified policy in the operation attributes.
   *
   * @param attributes the operation attributes
   * @param policyName the policy name
   * @param writeTag the write tag
   */
  public static void setWriteTag(
      Map<String, String> attributes, String policyName, String writeTag) {
    attributes.put(WRITE_TAG_PREFIX + policyName, writeTag);
  }

  /**
   * Sets the specified write tag for the specified policy in the operation attributes builder.
   *
   * @param attributesBuilder the operation attributes builder
   * @param policyName the policy name
   * @param writeTag the write tag
   */
  public static void setWriteTag(
      ImmutableMap.Builder<String, String> attributesBuilder, String policyName, String writeTag) {
    attributesBuilder.put(WRITE_TAG_PREFIX + policyName, writeTag);
  }

  /**
   * Clears the write tag for the specified policy from the operation attributes.
   *
   * @param attributes the operation attributes
   * @param policyName the policy name
   */
  public static void clearWriteTag(Map<String, String> attributes, String policyName) {
    attributes.remove(WRITE_TAG_PREFIX + policyName);
  }

  /**
   * Clears all the write tags from the operation attributes.
   *
   * @param attributes the operation attributes
   */
  public static void clearWriteTags(Map<String, String> attributes) {
    attributes.entrySet().removeIf(e -> e.getKey().startsWith(WRITE_TAG_PREFIX));
  }

  /**
   * Returns the read tag for the specified policy from the operation.
   *
   * @param operation the operation
   * @param policyName the policy name
   * @return an {@code Optional} containing the read tag, or an empty {@code Optional} if not set
   */
  public static Optional<String> getReadTag(Operation operation, String policyName) {
    return operation.getAttribute(READ_TAG_PREFIX + policyName);
  }

  /**
   * Returns the write tag for the specified policy from the operation.
   *
   * @param operation the operation
   * @param policyName the policy name
   * @return an {@code Optional} containing the write tag, or an empty {@code Optional} if not set
   */
  public static Optional<String> getWriteTag(Operation operation, String policyName) {
    return operation.getAttribute(WRITE_TAG_PREFIX + policyName);
  }
}
