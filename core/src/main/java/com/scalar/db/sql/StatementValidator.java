package com.scalar.db.sql;

import com.scalar.db.sql.statement.CreateCoordinatorTableStatement;
import com.scalar.db.sql.statement.CreateIndexStatement;
import com.scalar.db.sql.statement.CreateNamespaceStatement;
import com.scalar.db.sql.statement.CreateTableStatement;
import com.scalar.db.sql.statement.DeleteStatement;
import com.scalar.db.sql.statement.DropCoordinatorTableStatement;
import com.scalar.db.sql.statement.DropIndexStatement;
import com.scalar.db.sql.statement.DropNamespaceStatement;
import com.scalar.db.sql.statement.DropTableStatement;
import com.scalar.db.sql.statement.InsertStatement;
import com.scalar.db.sql.statement.SelectStatement;
import com.scalar.db.sql.statement.Statement;
import com.scalar.db.sql.statement.StatementVisitor;
import com.scalar.db.sql.statement.TruncateCoordinatorTableStatement;
import com.scalar.db.sql.statement.TruncateTableStatement;
import com.scalar.db.sql.statement.UpdateStatement;
import com.scalar.db.util.TableMetadataManager;

// TODO
public class StatementValidator implements StatementVisitor {

  public StatementValidator(TableMetadataManager tableMetadataManager, Statement statement) {}

  public void validate() {}

  @Override
  public void visit(CreateNamespaceStatement statement) {}

  @Override
  public void visit(CreateTableStatement statement) {}

  @Override
  public void visit(DropNamespaceStatement statement) {}

  @Override
  public void visit(DropTableStatement statement) {}

  @Override
  public void visit(TruncateTableStatement statement) {}

  @Override
  public void visit(CreateCoordinatorTableStatement statement) {}

  @Override
  public void visit(DropCoordinatorTableStatement statement) {}

  @Override
  public void visit(TruncateCoordinatorTableStatement statement) {}

  @Override
  public void visit(CreateIndexStatement statement) {}

  @Override
  public void visit(DropIndexStatement statement) {}

  @Override
  public void visit(SelectStatement statement) {}

  @Override
  public void visit(InsertStatement statement) {}

  @Override
  public void visit(UpdateStatement statement) {}

  @Override
  public void visit(DeleteStatement statement) {}
}
