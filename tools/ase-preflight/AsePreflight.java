import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;
import java.util.List;

/**
 * Checks, against a live SAP ASE, every assumption that RdbEngineSybase makes and that could not be
 * verified without a server. Each check runs the SQL the engine actually emits.
 *
 * <p>Run it with the jConnect driver on the classpath:
 *
 * <pre>
 * java -cp jconn4.jar tools/ase-preflight/AsePreflight.java \
 *     "jdbc:sybase:Tds:localhost:5000/scalardb" sa myPassword
 * </pre>
 *
 * <p>It creates objects named {@code preflight_*} owned by the connecting user and drops them
 * again, so point it at a scratch database rather than at anything that matters.
 */
public class AsePreflight {

  private static final String TABLE = "preflight_t";
  private static final String INDEX = "preflight_idx";

  private final Connection connection;
  private final List<String> results = new ArrayList<>();
  private int failures;

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("usage: AsePreflight <jdbcUrl> <user> <password>");
      System.exit(2);
    }
    // The same driver class name that RdbEngineSybase#getDriverClassName() returns. The driver is
    // instantiated directly rather than gone through DriverManager, which refuses a driver loaded
    // by a different classloader than the caller's -- as happens when this file is run straight
    // from source with "java -cp jconn4.jar AsePreflight.java".
    Driver driver =
        (Driver)
            Class.forName("com.sybase.jdbc4.jdbc.SybDriver").getDeclaredConstructor().newInstance();
    Properties properties = new Properties();
    properties.setProperty("user", args[1]);
    properties.setProperty("password", args[2]);
    try (Connection connection = driver.connect(args[0], properties)) {
      if (connection == null) {
        throw new SQLException("The driver did not accept the URL: " + args[0]);
      }
      AsePreflight preflight = new AsePreflight(connection);
      preflight.run();
      System.exit(preflight.failures == 0 ? 0 : 1);
    }
  }

  private AsePreflight(Connection connection) {
    this.connection = connection;
  }

  private void run() {
    describeServer();

    // The engine turns quoted identifiers on for every connection, and everything below depends on
    // it, so it runs first.
    check(
        "SET QUOTED_IDENTIFIER ON",
        () -> {
          execute("SET QUOTED_IDENTIFIER ON");
          query("SELECT 1 AS \"quoted alias\"");
          return "accepted";
        });

    dropLeftovers();

    check("CREATE TABLE with LOCK DATAROWS and explicit nullability", this::createTable);
    check("Nullable column accepts null", this::insertNull);
    check("Multi-statement prepared upsert", this::multiStatementUpsert);
    check("MERGE statement", this::mergeStatement);
    check("SELECT TOP n", () -> query("SELECT TOP 1 * FROM \"" + TABLE + "\""));
    check("Date and time text literals round trip", this::timeRoundTrip);
    check("Pre-1582 date round trips", this::earlyDateRoundTrip);
    check("RAISERROR guard for a missing owner", this::raiserrorGuard);
    check("Catalog lookup by owner", this::catalogLookup);
    check("CREATE INDEX and DROP INDEX table.index", this::indexLifecycle);
    check("sp_rename of a column", this::renameColumn);
    check("ALTER TABLE MODIFY", this::alterColumnType);
    check("ALTER TABLE DROP without the COLUMN keyword", this::dropColumn);

    check("Type catalogue table", this::typeCatalogue);

    captureErrorCode("Duplicate key error code", this::provokeDuplicateKey, 2601, 2615, 2627);
    captureErrorCode("Duplicate object error code", this::provokeDuplicateObject, 2714);
    captureErrorCode("Missing index error code", this::provokeMissingIndex, 2727, 3701);

    reportImportTypeNames();
    dropLeftovers();

    System.out.println();
    results.forEach(System.out::println);
    System.out.println();
    System.out.println(failures == 0 ? "All checks passed" : failures + " check(s) need attention");
  }

  private void describeServer() {
    System.out.println("== server ==");
    print("version", () -> scalar("SELECT @@version"));
    // Decides the VARCHAR(1900) and VARCHAR(128) constants in the engine
    print("page size", () -> scalar("SELECT @@maxpagesize"));
    // Decides whether creating a table for another owner needs create any table or sa_role
    print("granular permissions", () -> scalar("SELECT value FROM master..sysconfigures WHERE name = 'enable granular permissions'"));
    // The engine assumes it has to ask for datarows locking explicitly
    print("default lock scheme", () -> scalar("SELECT value FROM master..sysconfigures WHERE name = 'lock scheme'"));
    // ScalarDB expects a column with no nullability clause to be nullable; ASE usually does not
    print("allow nulls by default", () -> scalar("SELECT 1 FROM master..sysdatabases WHERE name = db_name() AND status2 & 16384 = 16384"));
    System.out.println();
  }

  private String createTable() throws SQLException {
    // The DDL that JdbcAdmin builds for this engine: NOT NULL keys, NULL everywhere else, and the
    // primary key clause that carries LOCK DATAROWS
    execute(
        "CREATE TABLE \""
            + TABLE
            + "\"("
            + "\"p1\" VARCHAR(128) NOT NULL,"
            + "\"c1\" VARCHAR(128) NOT NULL,"
            + "\"v1\" VARCHAR(1900) NULL,"
            + "\"v2\" TINYINT NULL,"
            + "\"v3\" BIGDATETIME NULL,"
            + "\"v4\" BIGTIME NULL,"
            + "\"v5\" DATE NULL,"
            + "\"v6\" VARBINARY(1900) NULL,"
            + " PRIMARY KEY (\"p1\",\"c1\")) LOCK DATAROWS");
    return "created";
  }

  private String insertNull() throws SQLException {
    execute("INSERT INTO \"" + TABLE + "\" (\"p1\",\"c1\") VALUES ('a','a')");
    return "a row with only its keys set was accepted";
  }

  private String multiStatementUpsert() throws SQLException {
    // Character for character what UpdateThenInsertQuery emits
    String sql =
        "UPDATE \""
            + TABLE
            + "\" SET \"v1\"=? WHERE \"p1\"=? AND \"c1\"=?\n"
            + "IF @@rowcount = 0\n"
            + "INSERT INTO \""
            + TABLE
            + "\" (\"p1\",\"c1\",\"v1\") VALUES (?,?,?)";

    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      // The row is absent, so the INSERT branch has to run
      bindUpsert(statement, "inserted", "b", "b");
      statement.executeUpdate();
    }
    if (!"inserted".equals(scalar("SELECT \"v1\" FROM \"" + TABLE + "\" WHERE \"p1\" = 'b'"))) {
      throw new SQLException("the insert branch did not run");
    }
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      // The row is present now, so the UPDATE branch has to run instead
      bindUpsert(statement, "updated", "b", "b");
      statement.executeUpdate();
    }
    if (!"updated".equals(scalar("SELECT \"v1\" FROM \"" + TABLE + "\" WHERE \"p1\" = 'b'"))) {
      throw new SQLException("the update branch did not run");
    }
    if (!"1".equals(scalar("SELECT count(*) FROM \"" + TABLE + "\" WHERE \"p1\" = 'b'"))) {
      throw new SQLException("the upsert left more than one row");
    }
    return "both branches behave";
  }

  private void bindUpsert(PreparedStatement statement, String value, String p1, String c1)
      throws SQLException {
    statement.setString(1, value);
    statement.setString(2, p1);
    statement.setString(3, c1);
    statement.setString(4, p1);
    statement.setString(5, c1);
    statement.setString(6, value);
  }

  private String mergeStatement() throws SQLException {
    // Character for character the shape MergeQuery emits, parameters and all. If this passes,
    // UpdateThenInsertQuery can be dropped in favour of the shared MergeQuery.
    String sql =
        "MERGE INTO \""
            + TABLE
            + "\" t1 USING (SELECT ? \"p1\",? \"c1\") t2"
            + " ON (t1.\"p1\"=t2.\"p1\" AND t1.\"c1\"=t2.\"c1\")"
            + " WHEN MATCHED THEN UPDATE SET \"v1\"=?"
            + " WHEN NOT MATCHED THEN INSERT (\"p1\",\"c1\",\"v1\") VALUES (?,?,?)";

    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      bindMerge(statement, "merge-inserted");
      statement.executeUpdate();
    }
    if (!"merge-inserted".equals(scalar("SELECT \"v1\" FROM \"" + TABLE + "\" WHERE \"p1\" = 'g'"))) {
      throw new SQLException("the not-matched branch did not insert");
    }
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      bindMerge(statement, "merge-updated");
      statement.executeUpdate();
    }
    if (!"merge-updated".equals(scalar("SELECT \"v1\" FROM \"" + TABLE + "\" WHERE \"p1\" = 'g'"))) {
      throw new SQLException("the matched branch did not update");
    }
    if (!"1".equals(scalar("SELECT count(*) FROM \"" + TABLE + "\" WHERE \"p1\" = 'g'"))) {
      throw new SQLException("the merge left more than one row");
    }
    // Both branches work, but the engine still does not use MERGE: ASE compiles the WHEN NOT
    // MATCHED THEN INSERT branch even for a row that is there, so it fails on any table with a NOT
    // NULL column the upsert is not writing. See UpdateThenInsertQuery.
    return "both branches behave, though the engine still uses the two-statement upsert";
  }

  private void bindMerge(PreparedStatement statement, String value) throws SQLException {
    statement.setString(1, "g");
    statement.setString(2, "g");
    statement.setString(3, value);
    statement.setString(4, "g");
    statement.setString(5, "g");
    statement.setString(6, value);
  }

  private String timeRoundTrip() throws SQLException {
    // The text forms RdbEngineTimeTypeSybase produces
    execute(
        "INSERT INTO \""
            + TABLE
            + "\" (\"p1\",\"c1\",\"v3\",\"v4\",\"v5\") VALUES "
            + "('t','t','20200102 03:04:05.123456','01:02:03.123456','20200102')");
    Timestamp timestamp =
        (Timestamp) object("SELECT \"v3\" FROM \"" + TABLE + "\" WHERE \"p1\" = 't'", "v3");
    String time = String.valueOf(object("SELECT \"v4\" FROM \"" + TABLE + "\" WHERE \"p1\" = 't'", "v4"));
    String date = String.valueOf(object("SELECT \"v5\" FROM \"" + TABLE + "\" WHERE \"p1\" = 't'", "v5"));
    boolean micros = timestamp != null && timestamp.getNanos() == 123456000;
    return "timestamp="
        + timestamp
        + (micros ? " (microseconds kept)" : " (MICROSECONDS LOST)")
        + ", time="
        + time
        + ", date="
        + date;
  }

  private String earlyDateRoundTrip() throws SQLException {
    // Writing text avoids the Julian to Gregorian shift on the way in; this shows what comes back
    execute(
        "INSERT INTO \"" + TABLE + "\" (\"p1\",\"c1\",\"v5\") VALUES ('e','e','10000102')");
    Object date = object("SELECT \"v5\" FROM \"" + TABLE + "\" WHERE \"p1\" = 'e'", "v5");
    return "1000-01-02 came back as " + date + (String.valueOf(date).startsWith("1000-01-02") ? "" : " (SHIFTED)");
  }

  private String raiserrorGuard() throws SQLException {
    // The statement createSchemaSqls() returns. It must raise for a user that does not exist.
    try {
      execute(
          "IF NOT EXISTS (SELECT 1 FROM sysusers WHERE name = 'no_such_owner_preflight')"
              + " RAISERROR 20000 'DB-CORE-10304: the namespace does not exist'");
      return "DID NOT RAISE, so a missing owner would go unreported";
    } catch (SQLException e) {
      // Raising is the expected outcome
      return "raised as expected: " + e.getErrorCode() + " " + firstLine(e.getMessage());
    }
  }

  private String catalogLookup() throws SQLException {
    String sql =
        "SELECT 1 FROM sysobjects o JOIN sysusers u ON o.uid = u.uid"
            + " WHERE u.name = ? AND o.name = ? AND o.type = 'U'";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, scalar("SELECT user_name()"));
      statement.setString(2, TABLE);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          throw new SQLException("the table was not found under its owner");
        }
      }
    }
    return "found the table under its owner";
  }

  private String indexLifecycle() throws SQLException {
    // ScalarDB alters a TEXT column to the key column size before indexing it, because ASE caps an
    // index row at 600 bytes on a 2K page server. Mirror that here.
    execute("ALTER TABLE \"" + TABLE + "\" MODIFY \"v1\" VARCHAR(128) NULL");
    // The index name is deliberately not quoted: ASE would keep the quotes in the name
    execute("CREATE INDEX " + INDEX + " ON \"" + TABLE + "\" (\"v1\")");
    String renameSql = renameAsOwner(TABLE + "." + INDEX, INDEX + "2", "index");
    execute(renameSql);
    if (!indexExists(INDEX + "2")) {
      // sp_rename reports a bad rename as a message with a non-zero status, not as an error, so
      // the outcome has to be checked rather than assumed
      throw new SQLException(
          "sp_rename reported success but the index was not renamed. Indexes now: "
              + allIndexNames()
              + ". SQL was: "
              + renameSql.replace("\n", " | "));
    }
    execute("DROP INDEX \"" + TABLE + "\"." + INDEX + "2");
    return "create, sp_rename and drop all work";
  }

  private String renameColumn() throws SQLException {
    execute(renameAsOwner(TABLE + ".v2", "v2renamed", "column"));
    if (!columnExists("v2renamed")) {
      throw new SQLException("sp_rename reported success but the column was not renamed");
    }
    execute(renameAsOwner(TABLE + ".v2renamed", "v2", "column"));
    return "renamed and renamed back, and the rename was verified in the catalog";
  }

  /** The shape RdbEngineSybase emits: sp_rename takes bare names, run as the object's owner. */
  private String renameAsOwner(String objectName, String newName, String objectType)
      throws SQLException {
    return "setuser '"
        + scalar("SELECT user_name()")
        + "'\nexec sp_rename '"
        + objectName
        + "', '"
        + newName
        + "', '"
        + objectType
        + "'\nsetuser";
  }

  private String allIndexNames() throws SQLException {
    StringBuilder names = new StringBuilder();
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT name FROM sysindexes WHERE id = object_id('" + TABLE + "')")) {
      while (resultSet.next()) {
        names.append('\'').append(resultSet.getString(1).trim()).append("' ");
      }
    }
    return names.toString();
  }

  private boolean indexExists(String indexName) throws SQLException {
    return !scalar(
            "SELECT name FROM sysindexes WHERE id = object_id('"
                + TABLE
                + "') AND name = '"
                + indexName
                + "'")
        .isEmpty();
  }

  private boolean columnExists(String columnName) throws SQLException {
    return !scalar(
            "SELECT name FROM syscolumns WHERE id = object_id('"
                + TABLE
                + "') AND name = '"
                + columnName
                + "'")
        .isEmpty();
  }

  private String alterColumnType() throws SQLException {
    execute("ALTER TABLE \"" + TABLE + "\" MODIFY \"v1\" VARCHAR(128) NULL");
    execute("ALTER TABLE \"" + TABLE + "\" MODIFY \"v1\" VARCHAR(1900) NULL");
    return "MODIFY accepted in both directions";
  }

  private String dropColumn() throws SQLException {
    execute("ALTER TABLE \"" + TABLE + "\" ADD \"v7\" INT NULL");
    execute("ALTER TABLE \"" + TABLE + "\" DROP \"v7\"");
    return "added a nullable column and dropped it";
  }

  private void provokeDuplicateKey() throws SQLException {
    execute("INSERT INTO \"" + TABLE + "\" (\"p1\",\"c1\") VALUES ('dup','dup')");
    execute("INSERT INTO \"" + TABLE + "\" (\"p1\",\"c1\") VALUES ('dup','dup')");
  }

  private void provokeDuplicateObject() throws SQLException {
    execute("CREATE TABLE \"" + TABLE + "\"(\"x\" INT NULL)");
  }

  private void provokeMissingIndex() throws SQLException {
    execute("DROP INDEX \"" + TABLE + "\".\"no_such_index_preflight\"");
  }

  private String typeCatalogue() throws SQLException {
    // What the driver reports for each type decides which branch of the import mapping it takes
    try {
      execute("DROP TABLE \"" + TABLE + "_types\"");
    } catch (SQLException e) {
      // not there yet
    }
    execute(
        "CREATE TABLE \""
            + TABLE
            + "_types\"("
            + "\"c_datetime\" DATETIME NULL,"
            + "\"c_smalldatetime\" SMALLDATETIME NULL,"
            + "\"c_bigdatetime\" BIGDATETIME NULL,"
            + "\"c_time\" TIME NULL,"
            + "\"c_bigtime\" BIGTIME NULL,"
            + "\"c_date\" DATE NULL,"
            + "\"c_char\" CHAR(10) NULL,"
            + "\"c_unichar\" UNICHAR(10) NULL,"
            + "\"c_univarchar\" UNIVARCHAR(10) NULL,"
            + "\"c_text\" TEXT NULL,"
            + "\"c_image\" IMAGE NULL,"
            + "\"c_binary\" BINARY(8) NULL,"
            + "\"c_numeric\" NUMERIC(10,2) NULL,"
            + "\"c_money\" MONEY NULL,"
            + "\"c_float\" FLOAT NULL,"
            + "\"c_real\" REAL NULL,"
            + "\"c_bit\" BIT NOT NULL,"
            + "\"c_uint\" UNSIGNED INT NULL,"
            + "\"c_ubigint\" UNSIGNED BIGINT NULL)");
    return "created a table covering the ASE types import has to recognise";
  }

  private void reportImportTypeNames() {
    // What the driver reports decides which branch of the import mapping a column takes
    System.out.println();
    System.out.println("== type names reported for import ==");
    try {
      DatabaseMetaData metaData = connection.getMetaData();
      for (String table : new String[] {TABLE, TABLE + "_types"}) {
      try (ResultSet resultSet =
          metaData.getColumns(null, scalar("SELECT user_name()"), table, "%")) {
        while (resultSet.next()) {
          System.out.printf(
              "   %-12s jdbcType=%-6d typeName=%-16s size=%d%n",
              resultSet.getString("COLUMN_NAME"),
              resultSet.getInt("DATA_TYPE"),
              resultSet.getString("TYPE_NAME"),
              resultSet.getInt("COLUMN_SIZE"));
        }
      }
      }
    } catch (SQLException e) {
      System.out.println("   could not read column metadata: " + firstLine(e.getMessage()));
    }
  }

  private interface Check {
    String run() throws SQLException;
  }

  private void check(String name, Check check) {
    try {
      results.add(String.format("PASS  %-52s %s", name, check.run()));
    } catch (SQLException e) {
      failures++;
      results.add(
          String.format(
              "FAIL  %-52s error %d: %s", name, e.getErrorCode(), firstLine(e.getMessage())));
    }
  }

  /** Runs something expected to fail, and reports the error number the server actually used. */
  private void captureErrorCode(String name, ThrowingRunnable action, int... expected) {
    try {
      action.run();
      failures++;
      results.add(String.format("FAIL  %-52s expected an error, got none", name));
    } catch (SQLException e) {
      boolean known = false;
      for (int code : expected) {
        known |= code == e.getErrorCode();
      }
      if (!known) {
        failures++;
      }
      results.add(
          String.format(
              "%s  %-52s error %d %s",
              known ? "PASS" : "FAIL",
              name,
              e.getErrorCode(),
              known ? "(as assumed)" : "(NOT among the assumed " + java.util.Arrays.toString(expected) + ")"));
    }
  }

  private interface ThrowingRunnable {
    void run() throws SQLException;
  }

  private void print(String label, Check check) {
    try {
      System.out.printf("   %-24s %s%n", label, check.run());
    } catch (SQLException e) {
      System.out.printf("   %-24s unavailable: %s%n", label, firstLine(e.getMessage()));
    }
  }

  private void dropLeftovers() {
    for (String sql :
        new String[] {
          "DROP TABLE \"" + TABLE + "\"", "DROP TABLE \"" + TABLE + "_types\""
        }) {
      try {
        execute(sql);
      } catch (SQLException e) {
        // The object is not there, which is the normal case
      }
    }
  }

  private void execute(String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private String query(String sql) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      return resultSet.next() ? "returned rows" : "returned no rows";
    }
  }

  private String scalar(String sql) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      return resultSet.next() ? firstLine(resultSet.getString(1)) : "";
    }
  }

  private Object object(String sql, String column) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      return resultSet.next() ? resultSet.getObject(column) : null;
    }
  }

  private static String firstLine(String value) {
    if (value == null) {
      return "";
    }
    int newline = value.indexOf('\n');
    return newline < 0 ? value.trim() : value.substring(0, newline).trim();
  }
}
