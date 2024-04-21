package com.scalar.db.api;

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

  void visit(Insert insert);

  void visit(Upsert upsert);

  void visit(Update update);
}
