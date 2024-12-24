package com.scalar.db.storage.dynamo;

import java.util.Objects;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Namespace)) {
      return false;
    }
    Namespace namespace = (Namespace) o;
    return Objects.equals(prefix, namespace.prefix) && Objects.equals(name, namespace.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(prefix, name);
  }
}
