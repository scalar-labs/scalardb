package com.scalar.db.storage.cosmos;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.Scan;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * A manager for all the statements
 *
 * @author Yuji Ito
 */
@Immutable
public class StatementHandlerManager {
  private final GetStatementHandler get;
  //private final ScanStatementHandler scan;
  private final InsertStatementHandler insert;
  private final UpdateStatementHandler update;
  //private final DeleteStatementHandler delete;

  private StatementHandlerManager(Builder builder) {
    this.get = builder.get;
    //this.scan = builder.scan;
    this.insert = builder.insert;
    this.update = builder.update;
    //this.delete = builder.delete;
  }

  @Nonnull
  public GetStatementHandler get() {
    return get;
  }

  //@Nonnull
  //public ScanStatementHandler scan() {
  //  return scan;
  //}

  @Nonnull
  public InsertStatementHandler insert() {
    return insert;
  }

  @Nonnull
  public UpdateStatementHandler update() {
    return update;
  }

  //@Nonnull
  //public DeleteStatementHandler delete() {
  //  return delete;
  //}

  @Nonnull
  public StatementHandler get(Operation operation) {
    if (operation instanceof Get) {
      return get();
    //} else if (operation instanceof Scan) {
    //  return scan();
    } else if (operation instanceof Put) {
      MutationCondition condition = ((Put) operation).getCondition().orElse(null);
      if (condition != null && (condition instanceof PutIf || condition instanceof PutIfExists)) {
        return update();
      } else {
        return insert();
      }
    //} else if (operation instanceof Delete) {
    //  return delete();
    }
    // never comes here usually
    throw new IllegalArgumentException("unexpected operation was given.");
  }

  @Nonnull
  public static StatementHandlerManager.Builder builder() {
    return new StatementHandlerManager.Builder();
  }

  public static class Builder {
    private GetStatementHandler get;
    //private ScanStatementHandler scan;
    private InsertStatementHandler insert;
    private UpdateStatementHandler update;
    //private DeleteStatementHandler delete;

    public Builder get(GetStatementHandler get) {
      this.get = get;
      return this;
    }

    //public Builder scan(ScanStatementHandler scan) {
    //  this.scan = scan;
    //  return this;
    //}

    public Builder insert(InsertStatementHandler insert) {
      this.insert = insert;
      return this;
    }

    public Builder update(UpdateStatementHandler update) {
      this.update = update;
      return this;
    }

    //public Builder delete(DeleteStatementHandler delete) {
    //  this.delete = delete;
    //  return this;
    //}

    public StatementHandlerManager build() {
      //if (get == null || scan == null || insert == null || update == null || delete == null) {
      if (get == null) {
        throw new IllegalArgumentException("please set all the statement handlers.");
      }
      return new StatementHandlerManager(this);
    }
  }
}
