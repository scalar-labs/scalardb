package com.scalar.db.storage.jdbc;

import com.google.common.collect.ImmutableMap;
import java.sql.Driver;
import javax.annotation.Nullable;

class RdbEngineYugabyte extends RdbEnginePostgresql {
  @Override
  public Driver getDriver() {
    throw new UnsupportedOperationException("`getDriver` isn't supported");
  }

  @Nullable
  @Override
  public UnderlyingDataSourceConfig getDataSourceConfig(JdbcConfig config) {
    return new UnderlyingDataSourceConfig(
        "com.yugabyte.ysql.YBClusterAwareDataSource",
        ImmutableMap.<String, String>builder()
            .put("portNumber", "5433")
            // TODO: Add other parameters to address the warnings
            .build());
  }
}
