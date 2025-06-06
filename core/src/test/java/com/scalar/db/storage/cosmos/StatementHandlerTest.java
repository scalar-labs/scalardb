package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.common.TableMetadataManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class StatementHandlerTest {

  private static final int FETCH_SIZE = 10;

  @Mock private TableMetadataManager metadataManager;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new SelectStatementHandler(null, metadataManager, FETCH_SIZE))
        .isInstanceOf(NullPointerException.class);
  }
}
