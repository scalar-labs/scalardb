package com.scalar.db.common.error;

import com.scalar.db.api.Put;
import com.scalar.db.io.Key;
import java.util.Arrays;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class CoreErrorTest {

  @Test
  public void checkDuplicateErrorCode() {
    Assertions.assertThat(Arrays.stream(CoreError.values()).map(CoreError::buildCode))
        .doesNotHaveDuplicates();
  }

  @Test
  public void buildCode_ShouldBuildCorrectCode() {
    // Arrange
    CoreError error = CoreError.OPERATION_CHECK_ERROR_INDEX1;

    // Act
    String code = error.buildCode();

    // Assert
    Assertions.assertThat(code).isEqualTo("CORE-40001");
  }

  @Test
  public void buildMessage_ShouldBuildCorrectMessage() {
    // Arrange
    CoreError error = CoreError.OPERATION_CHECK_ERROR_INDEX1;
    Put put =
        Put.newBuilder()
            .namespace("namespace")
            .table("table")
            .partitionKey(Key.ofInt("id", 0))
            .intValue("col", 0)
            .build();

    // Act
    String message = error.buildMessage(put);

    // Assert
    Assertions.assertThat(message)
        .isEqualTo("CORE-40001: Only a single column index is supported. Operation: " + put);
  }
}
