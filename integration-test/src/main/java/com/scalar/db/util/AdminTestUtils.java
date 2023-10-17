package com.scalar.db.util;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Properties;

/** Utility class used to delete or truncate the metadata table. */
public abstract class AdminTestUtils {

  private final Properties coordinatorStorageProperties;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public AdminTestUtils(Properties coordinatorStorageProperties) {
    this.coordinatorStorageProperties = coordinatorStorageProperties;
  }

  /**
   * Deletes the namespaces table.
   *
   * @throws Exception if an error occurs
   */
  public abstract void dropNamespacesTable() throws Exception;

  /**
   * Deletes the metadata table.
   *
   * @throws Exception if an error occurs
   */
  public abstract void dropMetadataTable() throws Exception;

  /**
   * Truncates the namespaces table.
   *
   * @throws Exception if an error occurs
   */
  public abstract void truncateNamespacesTable() throws Exception;

  /**
   * Truncates the metadata table.
   *
   * @throws Exception if an error occurs
   */
  public abstract void truncateMetadataTable() throws Exception;

  /**
   * Modifies the metadata the table in a way that it will fail if the metadata are parsed.
   *
   * @param namespace a namespace
   * @param table a table
   * @throws Exception if an error occurs
   */
  public abstract void corruptMetadata(String namespace, String table) throws Exception;

  /**
   * Returns whether the table and the table metadata for the coordinator tables are present or not.
   *
   * @return whether the table and the table metadata for the coordinator tables are present or not
   * @throws Exception if an error occurs
   */
  public boolean areTableAndMetadataForCoordinatorTablesPresent() throws Exception {
    String coordinatorNamespace =
        new ConsensusCommitConfig(new DatabaseConfig(coordinatorStorageProperties))
            .getCoordinatorNamespace()
            .orElse(Coordinator.NAMESPACE);
    String coordinatorTable = Coordinator.TABLE;
    if (!tableExists(coordinatorNamespace, coordinatorTable)) {
      return false;
    }
    // Use the DistributedStorageAdmin instead of the DistributedTransactionAdmin because the latter
    // expects the table to hold transaction table metadata columns which is not the case for the
    // coordinator table
    DistributedStorageAdmin storageAdmin =
        StorageFactory.create(coordinatorStorageProperties).getStorageAdmin();
    TableMetadata tableMetadata =
        storageAdmin.getTableMetadata(coordinatorNamespace, coordinatorTable);
    storageAdmin.close();

    if (tableMetadata == null) {
      return false;
    }
    return tableMetadata.equals(Coordinator.TABLE_METADATA);
  }

  /**
   * Drop the namespace but do not delete its metadata
   *
   * @param namespace a namespace
   * @throws Exception if an error occurs
   */
  public abstract void dropNamespace(String namespace) throws Exception;

  /**
   * Verify if the namespace exists in the underlying storage. It does not check the ScalarDB
   * metadata.
   *
   * @param namespace a namespace
   * @return true if the namespace exists or if the storage does not have the concept of namespace,
   *     false otherwise
   * @throws Exception if an error occurs
   */
  public abstract boolean namespaceExists(String namespace) throws Exception;

  /**
   * Check if the table exists in the underlying storage.
   *
   * @param namespace a namespace
   * @param table a table
   * @return true if the table exists, false otherwise
   * @throws Exception if an error occurs
   */
  public abstract boolean tableExists(String namespace, String table) throws Exception;

  /**
   * Drops the table in the underlying storage.
   *
   * @param namespace a namespace
   * @param table a table
   * @throws Exception if an errors occurs
   */
  public abstract void dropTable(String namespace, String table) throws Exception;

  /**
   * Closes connections to the storage
   *
   * @throws Exception if an error occurs
   */
  public abstract void close() throws Exception;
}
