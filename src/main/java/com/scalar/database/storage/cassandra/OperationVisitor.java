package com.scalar.database.storage.cassandra;

import com.scalar.database.api.Delete;
import com.scalar.database.api.Get;
import com.scalar.database.api.Operation;
import com.scalar.database.api.Put;
import com.scalar.database.api.Scan;

/**
 * A visitor class for {@link Operation}s
 *
 * @author Hiroyuki Yamada
 */
public interface OperationVisitor {

  void visit(Get get);

  void visit(Scan scan);

  void visit(Put put);

  void visit(Delete delete);
}
