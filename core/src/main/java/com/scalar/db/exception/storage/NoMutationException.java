package com.scalar.db.exception.storage;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Mutation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;

public class NoMutationException extends ExecutionException {

  private final List<? extends Mutation> mutations;

  public NoMutationException(
      String message, @SuppressFBWarnings("EI_EXPOSE_REP2") List<? extends Mutation> mutations) {
    super(addTransactionIdToMessage(message, mutations));
    this.mutations = mutations;
  }

  public NoMutationException(
      String message,
      @SuppressFBWarnings("EI_EXPOSE_REP2") List<? extends Mutation> mutations,
      Throwable cause) {
    super(addTransactionIdToMessage(message, mutations), cause);
    this.mutations = mutations;
  }

  public List<? extends Mutation> getMutations() {
    return ImmutableList.copyOf(mutations);
  }

  private static String addTransactionIdToMessage(
      String message, List<? extends Mutation> mutations) {
    StringBuilder builder = new StringBuilder(message).append(". Mutations: [");

    boolean first = true;
    for (Mutation mutation : mutations) {
      assert mutation.forFullTableName().isPresent();

      if (!first) {
        builder.append(", ");
      } else {
        first = false;
      }
      builder
          .append("{Type: ")
          .append(mutation.getClass().getSimpleName())
          .append(", Table: ")
          .append(mutation.forFullTableName().get())
          .append(", Partition Key: ")
          .append(mutation.getPartitionKey())
          .append(", Clustering Key: ")
          .append(mutation.getClusteringKey())
          .append("}");
    }

    return builder.append("]").toString();
  }
}
