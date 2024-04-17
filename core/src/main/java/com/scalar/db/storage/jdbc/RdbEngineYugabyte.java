package com.scalar.db.storage.jdbc;

import com.google.common.base.Splitter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Driver;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nullable;

class RdbEngineYugabyte extends RdbEnginePostgresql {
  @Override
  public Driver getDriver() {
    throw new UnsupportedOperationException("`getDriver` isn't supported");
  }

  HikariConfig buildHikariConfig(JdbcConfig config) {
    HikariConfig hikariConfig = new HikariConfig();
    if (!config.getJdbcUrl().startsWith("jdbc:")) {
      throw new AssertionError("Invalid JDBC URL. Jdbc Url: " + config.getJdbcUrl());
    }

    URI uri;
    try {
      uri = new URI(config.getJdbcUrl().substring("jdbc:".length()));
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Failed to parse JDBC URL. Url: " + config.getJdbcUrl());
    }

    String database = uri.getPath();
    Map<String, String> ybParams;
    if (database.startsWith("/")) {
      database = database.substring(1);
    }
    if (database.contains("?")) {
      int paramDelimiterPos = database.indexOf('?');
      String params = database.substring(paramDelimiterPos + 1);
      ybParams = Splitter.on('&').withKeyValueSeparator('=').split(params);
      database = database.substring(0, paramDelimiterPos);
    } else {
      ybParams = Collections.emptyMap();
    }

    hikariConfig.setDataSourceClassName("com.yugabyte.ysql.YBClusterAwareDataSource");
    hikariConfig.setMaximumPoolSize(config.getConnectionPoolMaxTotal());
    if (uri.getHost() != null) {
      hikariConfig.addDataSourceProperty("serverName", uri.getHost());
    }
    if (uri.getPort() > 0) {
      hikariConfig.addDataSourceProperty("portNumber", String.valueOf(uri.getPort()));
    }
    if (uri.getHost() != null) {
      hikariConfig.addDataSourceProperty("serverName", uri.getHost());
    }
    if (!database.isEmpty()) {
      hikariConfig.addDataSourceProperty("databaseName", database);
    }
    config.getUsername().ifPresent(value -> hikariConfig.addDataSourceProperty("user", value));
    config.getPassword().ifPresent(value -> hikariConfig.addDataSourceProperty("password", value));
    for (Entry<String, String> kv : ybParams.entrySet()) {
      hikariConfig.addDataSourceProperty(kv.getKey(), kv.getValue());
    }

    // TODO: Take care of other parameters

    return hikariConfig;
  }

  @Nullable
  @Override
  public AutoCloseableDataSource getDataSource(JdbcConfig config) {
    HikariConfig hikariConfig = buildHikariConfig(config);
    hikariConfig.validate();
    return new HikariCpDataSource(new HikariDataSource(hikariConfig));
  }
}
