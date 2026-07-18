package com.scalar.db.common;

import com.scalar.db.api.BranchTransaction;
import com.scalar.db.api.GlobalTransaction;
import com.scalar.db.api.GlobalTransactionManager;
import com.scalar.db.exception.transaction.TransactionException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;

/**
 * A {@link GlobalTransactionManager} that forwards each abstract method to a wrapped manager.
 *
 * <p>Base class for global-transaction-manager decorators: subclasses override only the methods
 * they need and inherit plain delegation for the rest. Methods that the interface supplies as
 * default implementations are left inherited rather than delegated, since they are defined in terms
 * of the abstract methods this class already forwards.
 */
public abstract class DecoratedGlobalTransactionManager implements GlobalTransactionManager {

  private final GlobalTransactionManager globalTransactionManager;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  protected DecoratedGlobalTransactionManager(GlobalTransactionManager globalTransactionManager) {
    this.globalTransactionManager = globalTransactionManager;
  }

  @Override
  public GlobalTransaction beginGlobal(Map<String, String> attributes) throws TransactionException {
    return globalTransactionManager.beginGlobal(attributes);
  }

  @Override
  public GlobalTransaction beginGlobalReadOnly(Map<String, String> attributes)
      throws TransactionException {
    return globalTransactionManager.beginGlobalReadOnly(attributes);
  }

  @Override
  public BranchTransaction beginBranch(String transactionId, Map<String, String> attributes)
      throws TransactionException {
    return globalTransactionManager.beginBranch(transactionId, attributes);
  }

  @Override
  public void close() {
    globalTransactionManager.close();
  }
}
