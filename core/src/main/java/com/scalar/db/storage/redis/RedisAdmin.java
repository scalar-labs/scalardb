package com.scalar.db.storage.redis;

import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

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

  /** Drop all keys in the namespace. */
  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    try (Jedis jedis = client.getJedis()) {
      Set<String> keys = jedis.keys(namespace + KEY_SEPARATOR);
      for (String key : keys) {
        jedis.del(key);
      }
    }
  }

  /** Try to find any key with the namespace prefix. */
  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    boolean ret = false;

    try (Jedis jedis = client.getJedis()) {
      // note: `jedis.keys()` is slow, so use `jedis.scan()` instead.
      // see:
      // <https://www.javadoc.io/static/redis.clients/jedis/5.0.0-alpha1/redis/clients/jedis/Jedis.html#keys-byte:A->
      ScanParams params = new ScanParams().match(namespace + KEY_SEPARATOR + "*");
      ScanResult<String> result;
      String cursor = "0";
      do {
        result = jedis.scan(cursor, params);
        List<String> keys = result.getResult();

        if (!keys.isEmpty()) {
          ret = true;
          break;
        }

        cursor = result.getCursor();
      } while (!cursor.equals("0"));
    }

    return ret;
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    throw new RuntimeException("Not implemented");
  }

  @Nullable
  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void close() {
    throw new RuntimeException("Not implemented");
  }
}
