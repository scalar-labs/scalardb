package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Result;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SingleRecordScannerTest {
  @Mock ResultInterpreter resultInterpreter;
  @Mock Record record1;

  @Mock Result result1;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    when(resultInterpreter.interpret(record1)).thenReturn(result1);
  }

  @Test
  public void one_ShouldContainOnlyOneResult() {
    // Arrange
    SingleRecordScanner scanner = buildScanner(record1);

    // Act
    Optional<Result> actualResult = scanner.one();
    Optional<Result> emptyResultForOne = scanner.one();
    List<Result> emptyResultForAll = scanner.all();

    // Assert
    assertThat(actualResult).contains(result1);
    assertThat(emptyResultForOne).isEmpty();
    assertThat(emptyResultForAll).isEmpty();
  }

  @Test
  public void all_ShouldContainOnlyOneResult() {
    // Arrange
    SingleRecordScanner scanner = buildScanner(record1);

    // Act
    List<Result> actualResult = scanner.all();
    Optional<Result> emptyResultForOne = scanner.one();
    List<Result> emptyResultForAll = scanner.all();

    // Assert
    assertThat(actualResult).containsExactly(result1);
    assertThat(emptyResultForOne).isEmpty();
    assertThat(emptyResultForAll).isEmpty();
  }

  private SingleRecordScanner buildScanner(Record record) {
    return new SingleRecordScanner(record, resultInterpreter);
  }
}
