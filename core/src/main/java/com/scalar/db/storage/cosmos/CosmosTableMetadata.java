package com.scalar.db.storage.cosmos;

import com.google.common.base.MoreObjects;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A metadata class for a table of ScalarDB to know the type of each column
 *
 * @author Yuji Ito
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
@ThreadSafe
public class CosmosTableMetadata {
  private final String id;
  private final LinkedHashSet<String> partitionKeyNames;
  private final LinkedHashSet<String> clusteringKeyNames;
  private final Map<String, String> clusteringOrders;
  private final Set<String> secondaryIndexNames;
  private final Map<String, String> columns;

  public CosmosTableMetadata() {
    this(null, null, null, null, null, null);
  }

  public CosmosTableMetadata(
      @Nullable String id,
      @Nullable LinkedHashSet<String> partitionKeyNames,
      @Nullable LinkedHashSet<String> clusteringKeyNames,
      @Nullable Map<String, String> clusteringOrders,
      @Nullable Set<String> secondaryIndexNames,
      @Nullable Map<String, String> columns) {
    this.id = id != null ? id : "";
    this.partitionKeyNames = partitionKeyNames != null ? partitionKeyNames : new LinkedHashSet<>();
    this.clusteringKeyNames =
        clusteringKeyNames != null ? clusteringKeyNames : new LinkedHashSet<>();
    this.clusteringOrders = clusteringOrders != null ? clusteringOrders : Collections.emptyMap();
    this.secondaryIndexNames =
        secondaryIndexNames != null ? secondaryIndexNames : Collections.emptySet();
    this.columns = columns != null ? columns : Collections.emptyMap();
  }

  private CosmosTableMetadata(Builder builder) {
    this(
        builder.id,
        builder.partitionKeyNames,
        builder.clusteringKeyNames,
        builder.clusteringOrders,
        builder.secondaryIndexNames,
        builder.columns);
  }

  public String getId() {
    return id;
  }

  public LinkedHashSet<String> getPartitionKeyNames() {
    return partitionKeyNames;
  }

  public LinkedHashSet<String> getClusteringKeyNames() {
    return clusteringKeyNames;
  }

  public Map<String, String> getClusteringOrders() {
    return clusteringOrders;
  }

  public Set<String> getSecondaryIndexNames() {
    return secondaryIndexNames;
  }

  public Map<String, String> getColumns() {
    return columns;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CosmosTableMetadata)) {
      return false;
    }
    CosmosTableMetadata that = (CosmosTableMetadata) o;
    return Objects.equals(id, that.id)
        && Objects.equals(partitionKeyNames, that.partitionKeyNames)
        && Objects.equals(clusteringKeyNames, that.clusteringKeyNames)
        && Objects.equals(clusteringOrders, that.clusteringOrders)
        && Objects.equals(secondaryIndexNames, that.secondaryIndexNames)
        && Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id, partitionKeyNames, clusteringKeyNames, clusteringOrders, secondaryIndexNames, columns);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("partitionKeyNames", partitionKeyNames)
        .add("clusteringKeyNames", clusteringKeyNames)
        .add("clusteringOrders", clusteringOrders)
        .add("secondaryIndexNames", secondaryIndexNames)
        .add("columns", columns)
        .toString();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {

    private String id;
    private LinkedHashSet<String> partitionKeyNames;
    private LinkedHashSet<String> clusteringKeyNames;
    private Map<String, String> clusteringOrders;
    private Set<String> secondaryIndexNames;
    private Map<String, String> columns;

    private Builder() {}

    public Builder id(String val) {
      id = val;
      return this;
    }

    public Builder partitionKeyNames(LinkedHashSet<String> val) {
      partitionKeyNames = val;
      return this;
    }

    public Builder clusteringKeyNames(LinkedHashSet<String> val) {
      clusteringKeyNames = val;
      return this;
    }

    public Builder clusteringOrders(Map<String, String> val) {
      clusteringOrders = val;
      return this;
    }

    public Builder secondaryIndexNames(Set<String> val) {
      secondaryIndexNames = val;
      return this;
    }

    public Builder columns(Map<String, String> val) {
      columns = val;
      return this;
    }

    public CosmosTableMetadata build() {
      return new CosmosTableMetadata(this);
    }
  }
}
