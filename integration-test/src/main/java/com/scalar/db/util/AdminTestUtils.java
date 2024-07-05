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
   * Deletes the metadata table.
   *
   * @throws Exception if an error occurs
   */
  public abstract void dropMetadataTable() throws Exception;

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
   * Returns whether the table metadata for the coordinator tables are present or not.
   *
   * @return whether the table metadata for the coordinator tables are present or not
   * @throws Exception if an error occurs
   */
  public boolean areTableMetadataForCoordinatorTablesPresent() throws Exception {
    ConsensusCommitConfig config =
        new ConsensusCommitConfig(new DatabaseConfig(coordinatorStorageProperties));
    String coordinatorNamespace = config.getCoordinatorNamespace().orElse(Coordinator.NAMESPACE);
    String coordinatorTable = Coordinator.TABLE;
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
    TableMetadata expectedMetadata;
    if (config.isCoordinatorGroupCommitEnabled()) {
      expectedMetadata = Coordinator.TABLE_METADATA_WITH_GROUP_COMMIT_ENABLED;
    } else {
      expectedMetadata = Coordinator.TABLE_METADATA_WITH_GROUP_COMMIT_DISABLED;
    }
    return tableMetadata.equals(expectedMetadata);
  }
}
