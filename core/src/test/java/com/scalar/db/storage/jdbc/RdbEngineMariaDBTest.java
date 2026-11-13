package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

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
}
