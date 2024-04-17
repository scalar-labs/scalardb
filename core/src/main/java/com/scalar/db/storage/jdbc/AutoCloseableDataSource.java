package com.scalar.db.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import javax.sql.DataSource;

public interface AutoCloseableDataSource extends DataSource, AutoCloseable {
  @VisibleForTesting
  DataSource underlyingDataSource();
}
