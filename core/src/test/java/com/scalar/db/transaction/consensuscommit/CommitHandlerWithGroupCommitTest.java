package com.scalar.db.transaction.consensuscommit;

import static org.mockito.Mockito.spy;

import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.util.groupcommit.GroupCommitConfig;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;

@Nested
class CommitHandlerWithGroupCommitTest extends CommitHandlerTestBase {
  private final CoordinatorGroupCommitKeyManipulator keyManipulator =
      new CoordinatorGroupCommitKeyManipulator();
  private String parentKey;
  private String childKey;
  private CoordinatorGroupCommitter groupCommitter;

  @BeforeEach
  void setUp() throws Exception {
    groupCommitter = new CoordinatorGroupCommitter(new GroupCommitConfig(4, 100, 500, 10));
    handler =
        spy(
            new CommitHandler(
                storage,
                coordinator,
                tableMetadataManager,
                new ParallelExecutor(config),
                groupCommitter));
    childKey = UUID.randomUUID().toString();
    String fullKey = groupCommitter.reserve(childKey);
    parentKey = keyManipulator.keysFromFullKey(fullKey).parentKey;
  }

  @AfterEach
  void tearDown() {
    groupCommitter.close();
  }

  @Override
  Optional<CoordinatorGroupCommitter> groupCommitter() {
    return Optional.of(groupCommitter);
  }

  @Override
  String anyId() {
    return keyManipulator.fullKey(parentKey, childKey);
  }

  @Override
  String groupCommitParentId() {
    return parentKey;
  }

  @Override
  String groupCommitChildId() {
    return childKey;
  }
}
