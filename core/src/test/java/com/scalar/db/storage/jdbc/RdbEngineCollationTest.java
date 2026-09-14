package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.common.CoreError;
import com.scalar.db.io.Collation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Pins the ICU verdict of every RDB engine the test-only {@link RdbEngine} enum covers, inherited
 * verdicts included. TiDB is not in that enum; see {@link RdbEngineTidbTest}.
 */
class RdbEngineCollationTest {

  private static final ImmutableSet<RdbEngine> ENGINES_REJECTING_ICU =
      ImmutableSet.of(RdbEngine.SQLITE, RdbEngine.SPANNER);

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  void throwIfCollationNotSupported_GivenBinary_ShouldNotThrowAnyException(RdbEngine engine) {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(engine);
    assertThatCode(() -> rdbEngine.throwIfCollationNotSupported(Collation.BINARY))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  void throwIfCollationNotSupported_GivenIcu_ShouldThrowOnlyForEnginesWithoutUcaCollation(
      RdbEngine engine) {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(engine);
    if (ENGINES_REJECTING_ICU.contains(engine)) {
      assertThatThrownBy(() -> rdbEngine.throwIfCollationNotSupported(Collation.ICU))
          .isInstanceOf(IllegalArgumentException.class);
    } else {
      assertThatCode(() -> rdbEngine.throwIfCollationNotSupported(Collation.ICU))
          .doesNotThrowAnyException();
    }
  }

  @Test
  void
      throwIfCollationNotSupported_GivenIcuAndEngineNotOverridingIt_ShouldThrowIllegalArgumentException() {
    RdbEngineStrategy rdbEngine = mock(RdbEngineStrategy.class, CALLS_REAL_METHODS);
    assertThatThrownBy(() -> rdbEngine.throwIfCollationNotSupported(Collation.ICU))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(CoreError.COLLATION_ICU_NOT_SUPPORTED_BY_STORAGE.buildCode());
  }
}
