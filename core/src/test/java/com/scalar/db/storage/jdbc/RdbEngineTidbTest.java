package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RdbEngineTidbTest {

  // TiDB is MySQL compatible and uses the same connection string, cf. RdbEngineFactory#create()
  private static final String ANY_JDBC_URL = "jdbc:mysql://localhost:4000/";

  private RdbEngineTidb rdbEngineTidb;

  @BeforeEach
  void setUp() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");
    rdbEngineTidb = new RdbEngineTidb(new JdbcConfig(new DatabaseConfig(props)));
  }

  @Test
  void renameColumnSql_ShouldRenameWithoutRestatingTheType() {
    // Arrange Act
    String sql = rdbEngineTidb.renameColumnSql("ns", "tbl", "c1", "c2", "LONGTEXT");

    // Assert
    assertThat(sql).isEqualTo("ALTER TABLE `ns`.`tbl` RENAME COLUMN `c1` TO `c2`");
    assertThat(rdbEngineTidb.renameColumnRestatesType()).isFalse();
  }

  @Test
  void getCollationClause_ShouldReturnCharacterSetAndCollate() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("utf8mb4_bin", "utf8mb4");

    // Act
    String clause = rdbEngineTidb.getCollationClause(collation);

    // Assert
    assertThat(clause).isEqualTo("CHARACTER SET utf8mb4 COLLATE utf8mb4_bin");
  }
}
