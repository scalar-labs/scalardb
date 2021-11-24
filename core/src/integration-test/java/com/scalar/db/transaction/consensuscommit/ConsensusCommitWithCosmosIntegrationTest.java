package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.cosmos.Cosmos;
import com.scalar.db.storage.cosmos.CosmosAdmin;
import com.scalar.db.storage.cosmos.CosmosConfig;
import com.scalar.db.storage.cosmos.CosmosEnv;
import java.util.Map;
import java.util.Optional;
import org.junit.AfterClass;

public class ConsensusCommitWithCosmosIntegrationTest extends ConsensusCommitIntegrationTestBase {

  @Override
  protected void tearUp() throws Exception {
    CosmosConfig cosmosConfig = CosmosEnv.getCosmosConfig();
    Cosmos cosmos = new Cosmos(cosmosConfig);
    originalStorage = cosmos;
    admin = new CosmosAdmin(cosmos.getCosmosClient(), cosmosConfig);
    consensusCommitConfig = new ConsensusCommitConfig(cosmosConfig.getProperties());
    consensusCommitAdmin = new ConsensusCommitAdmin(admin, consensusCommitConfig);
    namespace1 = getNamespace1();
    namespace2 = getNamespace2();
    createTables();
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    deleteTables();
    originalStorage.close();
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CosmosEnv.getCosmosConfig();
  }

  @Override
  protected String getNamespace1() {
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + NAMESPACE_1).orElse(NAMESPACE_1);
  }

  @Override
  protected String getNamespace2() {
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + NAMESPACE_2).orElse(NAMESPACE_2);
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return CosmosEnv.getCreateOptions();
  }
}
