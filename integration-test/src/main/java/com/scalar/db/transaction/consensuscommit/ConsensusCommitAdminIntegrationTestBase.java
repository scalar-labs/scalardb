package com.scalar.db.transaction.consensuscommit;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdminIntegrationTestBase;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public abstract class ConsensusCommitAdminIntegrationTestBase
    extends DistributedTransactionAdminIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_admin_cc";
  }

  @Override
  protected final Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps(testName));

    // Add testName as a coordinator namespace suffix
    ConsensusCommitTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return properties;
  }

  protected abstract Properties getProps(String testName);

  @Override
  protected void transactionalInsert(Insert insert) throws TransactionException {
    // Wait for cache expiry
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

    try (DistributedTransactionManager manager = transactionFactory.getTransactionManager()) {
      DistributedTransaction transaction = manager.start();
      transaction.insert(insert);
      transaction.commit();
    }
  }

  @Override
  protected List<Result> transactionalScan(Scan scan) throws TransactionException {
    // Wait for cache expiry
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

    try (DistributedTransactionManager manager = transactionFactory.getTransactionManager()) {
      DistributedTransaction transaction = manager.start();
      List<Result> results = transaction.scan(scan);
      transaction.commit();
      return results;
    }
  }
}
