package com.scalar.db.storage.redis;

import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import redis.clients.jedis.Jedis;

@ThreadSafe
public class RedisAdmin implements DistributedStorageAdmin {
  private static final String KEY_SEPARATOR = "$";

  private final JedisClient client;

  @Inject
  public RedisAdmin(DatabaseConfig config) {
    client = new JedisClient(config);
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    // In Redis storage, namespace is a key prefix like: `<namespace>$<table>$<column>`.
    // So, do nothing here.
  }

  // Drop all keys in the namespace.
  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    try (Jedis jedis = client.getJedis()) {
      Set<String> keys = jedis.keys(namespace + KEY_SEPARATOR);
      for (String key : keys) {
        jedis.del(key);
      }
    }
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {}

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {}

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {}

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {}

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {}

  @Nullable
  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    return null;
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    return null;
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    return false;
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {}

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {}

  @Override
  public void close() {}
}
