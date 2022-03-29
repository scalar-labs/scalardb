package com.scalar.db.sql.statement;

public interface StatementVisitor<R, C>
    extends DdlStatementVisitor<R, C>, DmlStatementVisitor<R, C> {}
