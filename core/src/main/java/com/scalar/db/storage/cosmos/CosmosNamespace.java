package com.scalar.db.storage.cosmos;

import com.google.common.base.MoreObjects;
import java.util.Objects;

/** An entity class to store namespace metadata in a CosmosDB container */
public class CosmosNamespace {
  private String id;

  public CosmosNamespace() {}

  public CosmosNamespace(String id) {
    this.id = id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CosmosNamespace)) {
      return false;
    }
    CosmosNamespace that = (CosmosNamespace) o;

    return Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", id).toString();
  }
}
