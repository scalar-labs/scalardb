package com.scalar.db.storage;

import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
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
    if (!operation.forNamespacePrefix().isPresent()) {
      operation.forNamespacePrefix(namespacePrefix.orElse(null));
    }
    if (!operation.forNamespace().isPresent()) {
      operation.forNamespace(namespace.orElse(null));
    }
    if (!operation.forTable().isPresent()) {
      operation.forTable(tableName.orElse(null));
    }
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }
  }

  public static void checkIfPrimaryKeyExists(Operation operation, TableMetadata metadata) {
    throwIfNotMatched(Optional.of(operation.getPartitionKey()), metadata.getPartitionKeyNames());
    throwIfNotMatched(operation.getClusteringKey(), metadata.getClusteringKeyNames());
  }

  public static void checkGetOperation(Get get, TableMetadata metadata) {
    if (isSecondaryIndexSpecified(get, metadata)) {
      if (get.getClusteringKey().isPresent()) {
        throw new IllegalArgumentException(
            "Clustering keys cannot be specified when using an index");
      }
      return;
    }
    checkIfPrimaryKeyExists(get, metadata);
  }

  public static void checkScanOperation(Scan scan, TableMetadata metadata) {
    if (isSecondaryIndexSpecified(scan, metadata)) {
      if (scan.getStartClusteringKey().isPresent() || scan.getEndClusteringKey().isPresent()) {
        throw new IllegalArgumentException(
            "The clusteringKey should not be specified when using an index");
      }
      if (!scan.getOrderings().isEmpty()) {
        throw new IllegalArgumentException(
            "The ordering should not be specified when using an index");
      }
      return;
    }

    throwIfNotMatched(Optional.of(scan.getPartitionKey()), metadata.getPartitionKeyNames());
  }

  private static void throwIfNotMatched(Optional<Key> key, Set<String> names) {
    String message = "The required key (primary or partition) is not properly specified.";
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

  public static boolean isSecondaryIndexSpecified(Operation operation, TableMetadata metadata) {
    List<Value> keyValues = operation.getPartitionKey().get();
    if (keyValues.size() == 1) {
      String name = keyValues.get(0).getName();
      return metadata.getSecondaryIndexNames().contains(name);
    }

    return false;
  }
}
