package com.scalar.db.sql;

public class ClusteringOrdering {

  public final String columnName;
  public final ClusteringOrder clusteringOrder;

  private ClusteringOrdering(String columnName, ClusteringOrder clusteringOrder) {
    this.columnName = columnName;
    this.clusteringOrder = clusteringOrder;
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
