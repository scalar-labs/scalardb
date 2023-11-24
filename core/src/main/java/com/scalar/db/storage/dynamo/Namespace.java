package com.scalar.db.storage.dynamo;

public class Namespace {
  private final String prefix;
  private final String name;

  private Namespace(String prefix, String name) {
    if (prefix == null) {
      throw new IllegalArgumentException("The prefix cannot be null");
    }
    if (name == null) {
      throw new IllegalArgumentException("The namespace name cannot be null");
    }
    this.prefix = prefix;
    this.name = name;
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public static Namespace of(String prefix, String name) {
    return new Namespace(prefix, name);
  }

  public static Namespace of(String name) {
    return new Namespace("", name);
  }

  public String prefixed() {
    return prefix + name;
  }

  public String nonPrefixed() {
    return name;
  }

  @Override
  public String toString() {
    return prefixed();
  }
}
