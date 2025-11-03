package com.scalar.db.storage.objectstorage;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ObjectStorageNamespaceMetadata {
  public static final Integer DEFAULT_VERSION = 1;
  private final String name;
  private final Integer version;

  // The default constructor is required by Jackson to deserialize JSON object
  @SuppressWarnings("unused")
  public ObjectStorageNamespaceMetadata() {
    this(null, null);
  }

  public ObjectStorageNamespaceMetadata(@Nullable String name, @Nullable Integer version) {
    this.name = name != null ? name : "";
    this.version = version != null ? version : DEFAULT_VERSION;
  }

  public ObjectStorageNamespaceMetadata(@Nullable String name) {
    this(name, DEFAULT_VERSION);
  }

  public String getName() {
    return name;
  }

  public Integer getVersion() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ObjectStorageNamespaceMetadata)) {
      return false;
    }
    ObjectStorageNamespaceMetadata that = (ObjectStorageNamespaceMetadata) o;

    return name.equals(that.name) && version.equals(that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, version);
  }
}
