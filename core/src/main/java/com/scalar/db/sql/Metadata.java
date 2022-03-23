package com.scalar.db.sql;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.sql.exception.SqlException;
import java.util.Optional;

public class Metadata {

  private final DistributedTransactionAdmin admin;

  public Metadata(DistributedTransactionAdmin admin) {
    this.admin = admin;
  }

  public Optional<NamespaceMetadata> getNamespace(String namespaceName) {
    try {
      if (admin.namespaceExists(namespaceName)) {
        return Optional.of(new NamespaceMetadata(namespaceName, admin));
      }
    } catch (ExecutionException e) {
      throw new SqlException("Failed to check the namespace existence", e);
    }
    return Optional.empty();
  }
}
