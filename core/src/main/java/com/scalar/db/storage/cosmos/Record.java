package com.scalar.db.storage.cosmos;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * A record class that Cosmos DB uses for storing a document based on ScalarDB data model.
 *
 * @author Yuji Ito
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
@Immutable
public class Record {
  private final String id;
  private final String concatenatedPartitionKey;
  private final Map<String, Object> partitionKey;
  private final Map<String, Object> clusteringKey;
  private final Map<String, Object> values;

  public Record() {
    this(null, null, null, null, null);
  }

  public Record(
      @Nullable String id,
      @Nullable String concatenatedPartitionKey,
      @Nullable Map<String, Object> partitionKey,
      @Nullable Map<String, Object> clusteringKey,
      @Nullable Map<String, Object> values) {
    this.id = id != null ? id : "";
    this.concatenatedPartitionKey =
        concatenatedPartitionKey != null ? concatenatedPartitionKey : "";
    this.partitionKey = partitionKey != null ? partitionKey : Collections.emptyMap();
    this.clusteringKey = clusteringKey != null ? clusteringKey : Collections.emptyMap();
    this.values = values != null ? values : Collections.emptyMap();
  }

  public String getId() {
    return id;
  }

  public String getConcatenatedPartitionKey() {
    return concatenatedPartitionKey;
  }

  public Map<String, Object> getPartitionKey() {
    return partitionKey;
  }

  public Map<String, Object> getClusteringKey() {
    return clusteringKey;
  }

  public Map<String, Object> getValues() {
    return values;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Record)) {
      return false;
    }

    Record other = (Record) o;
    if (!other.getConcatenatedPartitionKey().equals(concatenatedPartitionKey)) {
      return false;
    }
    if (!other.getId().equals(id)) {
      return false;
    }
    if (!other.getPartitionKey().equals(partitionKey)) {
      return false;
    }
    if (!other.getClusteringKey().equals(clusteringKey)) {
      return false;
    }
    return other.getValues().equals(values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, concatenatedPartitionKey, partitionKey, clusteringKey, values);
  }
}
