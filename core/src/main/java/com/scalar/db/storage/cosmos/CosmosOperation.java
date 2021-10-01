package com.scalar.db.storage.cosmos;

import com.azure.cosmos.models.PartitionKey;
import com.google.common.base.Joiner;
import com.google.common.collect.Streams;
import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Value;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/** A class to treating utilities for an operation */
@Immutable
public class CosmosOperation {
  private final Operation operation;
  private final TableMetadata metadata;

  public CosmosOperation(Operation operation, TableMetadata metadata) {
    this.operation = operation;
    this.metadata = metadata;
  }

  public boolean isPrimaryKeySpecified() {
    if (metadata.getClusteringKeyNames().isEmpty()) {
      return true;
    }

    if (operation.getClusteringKey().isPresent()) {
      return operation.getClusteringKey().get().get().stream()
          .map(Value::getName)
          .collect(Collectors.toList())
          .containsAll(metadata.getClusteringKeyNames());
    } else {
      return false;
    }
  }

  @Nonnull
  public Operation getOperation() {
    return operation;
  }

  @Nonnull
  public String getConcatenatedPartitionKey() {
    Map<String, Value<?>> keyMap = new HashMap<>();
    operation.getPartitionKey().get().forEach(v -> keyMap.put(v.getName(), v));

    ConcatenationVisitor visitor = new ConcatenationVisitor();
    metadata.getPartitionKeyNames().forEach(name -> keyMap.get(name).accept(visitor));

    return visitor.build();
  }

  @Nonnull
  public PartitionKey getCosmosPartitionKey() {
    return new PartitionKey(getConcatenatedPartitionKey());
  }

  @Nonnull
  public String getId() {
    Map<String, Value<?>> keyMap = new HashMap<>();
    operation.getPartitionKey().get().forEach(v -> keyMap.put(v.getName(), v));
    operation.getClusteringKey().ifPresent(k -> k.get().forEach(v -> keyMap.put(v.getName(), v)));

    ConcatenationVisitor visitor = new ConcatenationVisitor();
    Streams.concat(
            metadata.getPartitionKeyNames().stream(), metadata.getClusteringKeyNames().stream())
        .forEach(name -> keyMap.get(name).accept(visitor));

    return visitor.build();
  }

  @SafeVarargs
  public final void checkArgument(Class<? extends Operation>... expected) {
    for (Class<? extends Operation> e : expected) {
      if (e.isInstance(operation)) {
        return;
      }
    }
    throw new IllegalArgumentException(
        Joiner.on(" ")
            .join(
                new String[] {
                  operation.getClass().toString(), "is passed where something like",
                  expected[0].toString(), "is expected."
                }));
  }
}
