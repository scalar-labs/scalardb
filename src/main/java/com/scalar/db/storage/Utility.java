package com.scalar.db.storage;

import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.io.Key;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Utilities for {@link com.scalar.db.api.DistributedStorage}
 *
 * @author Yuji Ito
 */
public final class Utility {

  public static void setTargetToIfNot(
      List<? extends Operation> operations,
      Optional<String> namespacePrefix,
      Optional<String> namespace,
      Optional<String> tableName) {
    operations.forEach(o -> setTargetToIfNot(o, namespacePrefix, namespace, tableName));
  }

  public static void setTargetToIfNot(
      Operation operation,
      Optional<String> namespacePrefix,
      Optional<String> namespace,
      Optional<String> tableName) {
    if (!operation.forNamespace().isPresent()) {
      if (namespace.isPresent()) {
        String name = namespace.get();
        operation.forNamespace(namespacePrefix.isPresent() ? namespacePrefix.get() + name : name);
      } else {
        operation.forNamespace(null);
      }
    } else if (namespacePrefix.isPresent()) {
      operation.forNamespace(namespacePrefix.get() + operation.forNamespace().get());
    }
    if (!operation.forTable().isPresent()) {
      operation.forTable(tableName.orElse(null));
    }
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }
  }

  public static void checkIfPrimaryKeyExists(Mutation mutation, TableMetadata metadata) {
    throwIfNotMatched(Optional.of(mutation.getPartitionKey()), metadata.getPartitionKeyNames());
    throwIfNotMatched(mutation.getClusteringKey(), metadata.getClusteringKeyNames());
  }

  private static void throwIfNotMatched(Optional<Key> key, Set<String> names) {
    String message = "The primary key is not properly specified.";
    if ((!key.isPresent() && names.size() > 0)
        || (key.isPresent() && (key.get().size() != names.size()))) {
      throw new IllegalArgumentException(message);
    }
    key.ifPresent(
        k ->
            k.forEach(
                v -> {
                  if (!names.contains(v.getName())) {
                    throw new IllegalArgumentException(message);
                  }
                }));
  }
}
