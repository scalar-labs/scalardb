package com.scalar.db.storage.cassandra;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.Scan;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A manager for all the statements
 *
 * @author Hiroyui Yamada
 */
@ThreadSafe
public class StatementHandlerManager {
  private final SelectStatementHandler select;
  private final InsertStatementHandler insert;
  private final UpdateStatementHandler update;
  private final DeleteStatementHandler delete;

  private StatementHandlerManager(Builder builder) {
    this.select = builder.select;
    this.insert = builder.insert;
    this.update = builder.update;
    this.delete = builder.delete;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  @Nonnull
  public SelectStatementHandler select() {
    return select;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  @Nonnull
  public InsertStatementHandler insert() {
    return insert;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  @Nonnull
  public UpdateStatementHandler update() {
    return update;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  @Nonnull
  public DeleteStatementHandler delete() {
    return delete;
  }

  @Nonnull
  public StatementHandler get(Operation operation) {
    if (operation instanceof Get || operation instanceof Scan) {
      return select();
    } else if (operation instanceof Put) {
      MutationCondition condition = ((Put) operation).getCondition().orElse(null);
      if (condition instanceof PutIf || condition instanceof PutIfExists) {
        return update();
      } else {
        return insert();
      }
    } else if (operation instanceof Delete) {
      return delete();
    }
    // never comes here usually
    throw new AssertionError("Unexpected operation was given");
  }

  @Nonnull
  public static StatementHandlerManager.Builder builder() {
    return new StatementHandlerManager.Builder();
  }

  public static class Builder {
    private SelectStatementHandler select;
    private InsertStatementHandler insert;
    private UpdateStatementHandler update;
    private DeleteStatementHandler delete;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public Builder select(SelectStatementHandler select) {
      this.select = select;
      return this;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public Builder insert(InsertStatementHandler insert) {
      this.insert = insert;
      return this;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public Builder update(UpdateStatementHandler update) {
      this.update = update;
      return this;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public Builder delete(DeleteStatementHandler delete) {
      this.delete = delete;
      return this;
    }

    public StatementHandlerManager build() {
      if (select == null || insert == null || update == null || delete == null) {
        throw new IllegalStateException("Please set all the statement handlers");
      }
      return new StatementHandlerManager(this);
    }
  }
}
