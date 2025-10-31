package com.scalar.db.storage.objectstorage;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ObjectStorageNamespaceMetadata {
  private final String name;

  // The default constructor is required by Jackson to deserialize JSON object
  @SuppressWarnings("unused")
  public ObjectStorageNamespaceMetadata() {
    this(null);
  }

  public ObjectStorageNamespaceMetadata(@Nullable String name) {
    this.name = name != null ? name : "";
  }

  public String getName() {
    return name;
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

    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
