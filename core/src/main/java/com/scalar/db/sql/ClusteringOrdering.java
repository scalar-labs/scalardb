package com.scalar.db.sql;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ClusteringOrdering {

  public final String columnName;
  public final ClusteringOrder clusteringOrder;

  private ClusteringOrdering(String columnName, ClusteringOrder clusteringOrder) {
    this.columnName = columnName;
    this.clusteringOrder = clusteringOrder;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("columnName", columnName)
        .add("clusteringOrder", clusteringOrder)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClusteringOrdering)) {
      return false;
    }
    ClusteringOrdering that = (ClusteringOrdering) o;
    return Objects.equals(columnName, that.columnName) && clusteringOrder == that.clusteringOrder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, clusteringOrder);
  }

  public static Builder column(String columnName) {
    return new Builder(columnName);
  }

  public static class Builder {
    private final String columnName;

    public Builder(String columnName) {
      this.columnName = columnName;
    }

    public ClusteringOrdering asc() {
      return new ClusteringOrdering(columnName, ClusteringOrder.ASC);
    }

    public ClusteringOrdering desc() {
      return new ClusteringOrdering(columnName, ClusteringOrder.DESC);
    }
  }
}
