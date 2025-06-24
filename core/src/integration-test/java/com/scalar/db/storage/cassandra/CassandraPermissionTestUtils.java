package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.Session;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.PermissionTestUtils;
import java.util.Properties;

public class CassandraPermissionTestUtils implements PermissionTestUtils {
  private final ClusterManager clusterManager;

  public CassandraPermissionTestUtils(Properties properties) {
    DatabaseConfig databaseConfig = new DatabaseConfig(properties);
    clusterManager = new ClusterManager(databaseConfig);
  }

  @Override
  public void createNormalUser(String userName, String password) {
    clusterManager
        .getSession()
        .execute(
            String.format(
                "CREATE ROLE IF NOT EXISTS %s WITH PASSWORD = '%s' AND LOGIN = true",
                userName, password));
  }

  @Override
  public void dropNormalUser(String userName) {
    clusterManager.getSession().execute(String.format("DROP ROLE IF EXISTS %s", userName));
  }

  @Override
  public void grantRequiredPermission(String userName) {
    Session session = clusterManager.getSession();
    for (String grantStatement : getGrantPermissionStatements(userName)) {
      session.execute(grantStatement);
    }
  }

  private String[] getGrantPermissionStatements(String userName) {
    return new String[] {
      String.format("GRANT CREATE ON ALL KEYSPACES TO %s", userName),
      String.format("GRANT DROP ON ALL KEYSPACES TO %s", userName),
      String.format("GRANT ALTER ON ALL KEYSPACES TO %s", userName),
      String.format("GRANT SELECT ON ALL KEYSPACES TO %s", userName),
      String.format("GRANT MODIFY ON ALL KEYSPACES TO %s", userName)
    };
  }

  @Override
  public void close() {
    clusterManager.close();
  }
}
