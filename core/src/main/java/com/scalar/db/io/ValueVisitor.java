package com.scalar.db.io;

/**
 * A visitor interface to traverse multiple {@code Value}s
 *
 * @author Hiroyuki Yamada
 */
public interface ValueVisitor {

  void visit(BooleanValue value);

  void visit(IntValue value);

  void visit(BigIntValue value);

  void visit(FloatValue value);

  void visit(DoubleValue value);

  void visit(TextValue value);

  void visit(BlobValue value);
}
