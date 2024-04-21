package com.scalar.db.api;

/**
 * A visitor class for {@code MutationCondition}
 *
 * @author Hiroyuki Yamada
 */
public interface MutationConditionVisitor {

  void visit(PutIf condition);

  void visit(PutIfExists condition);

  void visit(PutIfNotExists condition);

  void visit(DeleteIf condition);

  void visit(DeleteIfExists condition);

  void visit(UpdateIf condition);
}
