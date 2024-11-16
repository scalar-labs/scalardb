package com.scalar.db.api;

import java.util.Map;
import java.util.Optional;

/** A utility class to manipulate the operation attributes for Attribute-Based Access Control. */
public final class AbacOperationAttributes {

  private static final String OPERATION_ATTRIBUTE_PREFIX = "abac-";
  public static final String READ_TAG_PREFIX = OPERATION_ATTRIBUTE_PREFIX + "read-tag-";
  public static final String WRITE_TAG_PREFIX = OPERATION_ATTRIBUTE_PREFIX + "write-tag";

  private AbacOperationAttributes() {}

  public static void setReadTag(Map<String, String> attributes, String policyName, String readTag) {
    attributes.put(READ_TAG_PREFIX + policyName, readTag);
  }

  public static void clearReadTag(Map<String, String> attributes, String policyName) {
    attributes.remove(READ_TAG_PREFIX + policyName);
  }

  public static void clearReadTags(Map<String, String> attributes) {
    attributes.entrySet().removeIf(e -> e.getKey().startsWith(READ_TAG_PREFIX));
  }

  public static void setWriteTag(
      Map<String, String> attributes, String policyName, String writeTag) {
    attributes.put(WRITE_TAG_PREFIX + policyName, writeTag);
  }

  public static void clearWriteTag(Map<String, String> attributes, String policyName) {
    attributes.remove(WRITE_TAG_PREFIX + policyName);
  }

  public static void clearWriteTags(Map<String, String> attributes) {
    attributes.entrySet().removeIf(e -> e.getKey().startsWith(WRITE_TAG_PREFIX));
  }

  public static Optional<String> getReadTag(Operation operation, String policyName) {
    return operation.getAttribute(READ_TAG_PREFIX + policyName);
  }

  public static Optional<String> getWriteTag(Operation operation, String policyName) {
    return operation.getAttribute(WRITE_TAG_PREFIX + policyName);
  }
}
