package com.scalar.db.io;

/** A visitor interface to traverse multiple {@code Column}s */
public interface ColumnVisitor {
  void visit(BooleanColumn column);

  void visit(IntColumn column);

  void visit(BigIntColumn column);

  void visit(FloatColumn column);

  void visit(DoubleColumn column);

  void visit(TextColumn column);

  void visit(BlobColumn column);

  void visit(DateColumn column);

  void visit(TimeColumn column);

  void visit(TimestampColumn column);

  void visit(TimestampTZColumn column);
}
