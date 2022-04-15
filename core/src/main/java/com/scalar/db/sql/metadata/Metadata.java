package com.scalar.db.sql.metadata;

import java.util.Optional;

public interface Metadata {

  Optional<NamespaceMetadata> getNamespace(String namespaceName);
}
