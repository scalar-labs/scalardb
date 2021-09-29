package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.storage.common.TableMetadataManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class StatementHandlerTest {
  @Mock private TableMetadataManager metadataManager;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new SelectStatementHandler(null, metadataManager))
        .isInstanceOf(NullPointerException.class);
  }
}
