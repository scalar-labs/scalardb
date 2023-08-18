package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.scalar.db.transaction.consensuscommit.replication.semisync.columns.Key;
import javax.annotation.Nullable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = InsertedTuple.class, name = "insert"),
  @JsonSubTypes.Type(value = UpdatedTuple.class, name = "update"),
  @JsonSubTypes.Type(value = DeletedTuple.class, name = "delete"),
})
public class WrittenTuple {
  //  @JsonProperty public final String type;
  public final String namespace;
  public final String table;
  public final Key partitionKey;
  public final Key clusteringKey;

  public WrittenTuple(
      //    String type,
      String namespace, String table, Key partitionKey, @Nullable Key clusteringKey) {
    //    this.type = type;
    this.namespace = namespace;
    this.table = table;
    this.partitionKey = partitionKey;
    this.clusteringKey = clusteringKey;
  }
}
