package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Result;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitImportTableWithMetadataDecouplingIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import org.junit.jupiter.api.condition.DisabledIf;

@DisabledIf("com.scalar.db.storage.jdbc.JdbcEnv#isSqlite")
public class ConsensusCommitImportTableWithMetadataDecouplingIntegrationTestWithJdbcDatabase
    extends ConsensusCommitImportTableWithMetadataDecouplingIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);

    // Set the isolation level for consistency reads for virtual tables
    RdbEngineStrategy rdbEngine =
        RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    properties.setProperty(
        JdbcConfig.ISOLATION_LEVEL,
        JdbcTestUtils.getIsolationLevel(
                rdbEngine.getMinimumIsolationLevelForConsistentVirtualTableRead())
            .name());

    return properties;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
  }

  @Override
  protected Key createPartitionKey(int id) {
    if (JdbcEnv.isOracle()) {
      return Key.ofBigInt(ACCOUNT_ID, id);
    }
    return super.createPartitionKey(id);
  }

  @Override
  protected Column<?> createBalanceColumn(int balance) {
    if (JdbcEnv.isOracle()) {
      return BigIntColumn.of(BALANCE, balance);
    }
    return super.createBalanceColumn(balance);
  }

  @Override
  protected int getBalance(Result result) {
    if (JdbcEnv.isOracle()) {
      assertThat(result.getColumns()).containsKey(BALANCE);
      assertThat(result.getColumns().get(BALANCE).hasNullValue()).isFalse();
      return (int) result.getColumns().get(BALANCE).getBigIntValue();
    }
    return super.getBalance(result);
  }
}
