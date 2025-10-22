package com.scalar.db.common;

import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.exception.transaction.CrudException;

public abstract class AbstractTransactionCrudOperableScanner
    extends AbstractCrudOperableScanner<CrudException> implements TransactionCrudOperable.Scanner {}
