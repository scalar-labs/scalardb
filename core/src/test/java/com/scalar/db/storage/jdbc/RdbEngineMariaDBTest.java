package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RdbEngineMariaDBTest {

  private final RdbEngineMariaDB rdbEngine = new RdbEngineMariaDB();

  @Test
  void renameColumnSql_ShouldRenameWithoutRestatingTheType() {
    // Arrange Act
    String sql = rdbEngine.renameColumnSql("ns", "tbl", "c1", "c2", "LONGTEXT");

    // Assert
    assertThat(sql).isEqualTo("ALTER TABLE `ns`.`tbl` RENAME COLUMN `c1` TO `c2`");
    assertThat(rdbEngine.renameColumnRestatesType()).isFalse();
  }

  @Test
  void getCollationClause_ShouldReturnCharacterSetAndCollate() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("utf8mb4_nopad_bin", "utf8mb4");

    // Act
    String clause = rdbEngine.getCollationClause(collation);

    // Assert
    assertThat(clause).isEqualTo("CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin");
  }

  @Test
  void getTextColumnCollations_ShouldReadTheInformationSchema() throws SQLException {
    // Arrange
    Connection connection = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(connection.prepareStatement(anyString())).thenReturn(ps);
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true, false);
    when(rs.getString("COLUMN_NAME")).thenReturn("c1");
    when(rs.getString("COLLATION_NAME")).thenReturn("utf8mb4_nopad_bin");
    when(rs.getString("CHARACTER_SET_NAME")).thenReturn("utf8mb4");

    // Act
    Map<String, TextColumnCollation> collations =
        rdbEngine.getTextColumnCollations(connection, "ns", "tbl");

    // Assert
    assertThat(collations)
        .containsOnly(entry("c1", new TextColumnCollation("utf8mb4_nopad_bin", "utf8mb4")));
  }

  @Test
  void isBinaryCollation_Utf8mb4NopadBin_ShouldReturnTrue() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("utf8mb4_nopad_bin", "utf8mb4");

    // Act
    boolean actual = rdbEngine.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  void isBinaryCollation_Utf8mb4Bin_ShouldReturnFalse() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("utf8mb4_bin", "utf8mb4");

    // Act
    boolean actual = rdbEngine.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {"utf16", "utf16le", "utf32"})
  void isBinaryCollation_OtherUnicodeEncodingNopadBin_ShouldReturnTrue(String charset) {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation(charset + "_nopad_bin", charset);

    // Act
    boolean actual = rdbEngine.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  void isBinaryCollation_Utf16Bin_ShouldReturnFalse() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("utf16_bin", "utf16");

    // Act
    boolean actual = rdbEngine.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isFalse();
  }
}
