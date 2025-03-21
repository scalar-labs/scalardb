package com.scalar.db.common;

import com.scalar.db.api.TransactionManagerCrudOperable;
import com.scalar.db.exception.transaction.TransactionException;

public abstract class AbstractTransactionManagerCrudOperableScanner
    extends AbstractCrudOperableScanner<TransactionException>
    implements TransactionManagerCrudOperable.Scanner {}
