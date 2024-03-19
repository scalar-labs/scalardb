package com.scalar.db.transaction.consensuscommit;

import static org.mockito.Mockito.spy;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;

@Nested
class CommitHandlerWithoutGroupCommitTest extends CommitHandlerTestBase {
  private static final String ANY_ID = "id";

  @BeforeEach
  void setUp() throws Exception {
    handler =
        spy(
            new CommitHandler(
                storage, coordinator, tableMetadataManager, new ParallelExecutor(config)));
  }

  @Override
  Optional<CoordinatorGroupCommitter> groupCommitter() {
    return Optional.empty();
  }

  @Override
  String anyId() {
    return ANY_ID;
  }

  @Override
  String groupCommitParentId() {
    throw new AssertionError("Shouldn't reach here");
  }

  @Override
  String groupCommitChildId() {
    throw new AssertionError("Shouldn't reach here");
  }
}
