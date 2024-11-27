package com.scalar.db.storage.cosmos;

import com.azure.cosmos.models.PartitionKey;
import com.google.common.base.Joiner;
import com.google.common.collect.Streams;
import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
      Set<String> set =
          operation.getClusteringKey().get().getColumns().stream()
              .map(Column::getName)
              .collect(Collectors.toSet());
      return set.containsAll(metadata.getClusteringKeyNames());
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
    Map<String, Column<?>> keyMap = new HashMap<>();
    operation.getPartitionKey().getColumns().forEach(c -> keyMap.put(c.getName(), c));

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
    Map<String, Column<?>> keyMap = new HashMap<>();
    operation.getPartitionKey().getColumns().forEach(c -> keyMap.put(c.getName(), c));
    operation
        .getClusteringKey()
        .ifPresent(k -> k.getColumns().forEach(c -> keyMap.put(c.getName(), c)));

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
                  expected[0].toString(), "is expected"
                }));
  }
}
