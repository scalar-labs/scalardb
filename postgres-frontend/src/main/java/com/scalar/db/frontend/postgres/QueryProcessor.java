package com.scalar.db.frontend.postgres;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DataType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;

/**
 * Executes PostgreSQL-style SQL statements against ScalarDB, one session per instance. {@code
 * BEGIN}/{@code START TRANSACTION}, {@code COMMIT}/{@code END}, and {@code ROLLBACK} control an
 * explicit transaction; any other statement outside a transaction runs in its own auto-committed
 * transaction. DDL ({@code CREATE}/{@code DROP} {@code SCHEMA}, {@code TABLE}, {@code INDEX},
 * {@code ALTER TABLE}, {@code TRUNCATE}, {@code CREATE COORDINATOR TABLES}) runs through the admin
 * and is refused inside a transaction. {@code SET}, {@code RESET}, and {@code DISCARD} are accepted
 * and ignored. Not thread-safe.
 */
public class QueryProcessor {
  private final DistributedTransactionManager manager;
  private final QueryParser parser;
  @Nullable private DistributedTransaction transaction;
  private boolean aborted; // a statement failed inside the transaction: only ROLLBACK/COMMIT now
  private String lastCommandTag = "";
  private long ddlSettleMillis;

  /** DDL after which the engine's cached metadata of an existing table is stale. */
  private static final java.util.Set<String> CHANGES_EXISTING_TABLES =
      new java.util.HashSet<>(
          java.util.Arrays.asList(
              "DROP TABLE", "DROP SCHEMA", "DROP INDEX", "ALTER TABLE", "CREATE INDEX"));

  // The statement describe() planned last: the extended protocol describes a portal right before
  // executing it, and planning it once serves both
  // Plans by statement text, most recently used last: a reusable plan is planned once per session
  private final Map<String, QueryParser.Plan> planCache =
      new LinkedHashMap<String, QueryParser.Plan>(64, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, QueryParser.Plan> eldest) {
          return size() > 256;
        }
      };
  @Nullable private String describedSql;
  private List<Object> describedParameters = Collections.emptyList();
  @Nullable private QueryParser.Plan describedPlan;
  // Savepoints: accepted inside a transaction; ROLLBACK TO can only be honored while nothing was
  // written since the savepoint, as ScalarDB has no partial rollback
  private final java.util.Deque<String> savepoints = new java.util.ArrayDeque<>();
  private int writesSinceSavepoint;
  private static final java.util.Set<String> WRITES =
      new java.util.HashSet<>(java.util.Arrays.asList("INSERT", "UPDATE", "DELETE", "TRUNCATE"));
  private static final java.util.regex.Pattern SAVEPOINT_NAME =
      java.util.regex.Pattern.compile(
          "(?is)^\\s*(?:SAVEPOINT|RELEASE\\s+(?:SAVEPOINT\\s+)?|ROLLBACK\\s+(?:WORK\\s+|TRANSACTION\\s+)?TO\\s+(?:SAVEPOINT\\s+)?)\"?([\\w$]+)\"?\\s*;?\\s*$");
  // Session settings: what SET stored, over the server's defaults; SHOW reads them
  private final Map<String, String> settings = new LinkedHashMap<>();
  private final Map<String, String> defaults = new LinkedHashMap<>();

  private static final java.util.regex.Pattern READ_ONLY =
      java.util.regex.Pattern.compile("(?i)\\bREAD\\s+ONLY\\b");
  // The most rows a read-then-write statement may write; 0 means no limit
  private static final String MAX_ROWS_PER_WRITE = "scalardb.max_rows_per_write";

  public QueryProcessor(
      DistributedTransactionManager manager,
      DistributedTransactionAdmin admin,
      String defaultNamespace) {
    this(manager, new QueryParser(admin, defaultNamespace));
  }

  QueryProcessor(DistributedTransactionManager manager, QueryParser parser) {
    this.manager = manager;
    this.parser = parser;
    defaults.put("server_version", "16.0");
    defaults.put("server_encoding", "UTF8");
    defaults.put("client_encoding", "UTF8");
    defaults.put("DateStyle", "ISO, MDY");
    defaults.put("TimeZone", "UTC");
    defaults.put("integer_datetimes", "on");
    defaults.put("standard_conforming_strings", "on");
    defaults.put(MAX_ROWS_PER_WRITE, String.valueOf(QueryParser.DEFAULT_MAX_ROWS_PER_WRITE));
    defaults.put("is_superuser", "off");
    defaults.put("search_path", "\"$user\", public");
    defaults.put("transaction_isolation", "repeatable read");
    defaults.put("application_name", "");
    defaults.put("max_identifier_length", "63");
    defaults.put("client_min_messages", "notice");
  }

  /** Sets a session default, as the server does from its properties and the startup message. */
  public void setDefault(String name, String value) {
    String key = settingKey(name);
    defaults.put(key == null ? name : key, value);
  }

  private static final java.util.regex.Pattern SET_VALUE =
      java.util.regex.Pattern.compile(
          "(?is)^\\s*SET\\s+(?:SESSION\\s+|LOCAL\\s+)?(?:([\\w.]+)\\s*(?:=|TO)\\s*(.+?)|TIME\\s+ZONE\\s+(.+?))\\s*;?\\s*$");
  private static final java.util.regex.Pattern RESET_NAME =
      java.util.regex.Pattern.compile("(?is)^\\s*RESET\\s+([\\w.]+|TIME\\s+ZONE|ALL)\\s*;?\\s*$");
  private static final java.util.regex.Pattern SHOW_NAME =
      java.util.regex.Pattern.compile("(?is)^\\s*SHOW\\s+(.+?)\\s*;?\\s*$");

  /** The stored key of a setting named as in SHOW, matched without regard to case; null if none. */
  @Nullable
  private String settingKey(String name) {
    String n = name.trim().replaceAll("\\s+", " ");
    if (n.equalsIgnoreCase("transaction isolation level")) {
      n = "transaction_isolation";
    } else if (n.equalsIgnoreCase("time zone")) {
      n = "TimeZone";
    }
    for (String key : settings.keySet()) {
      if (key.equalsIgnoreCase(n)) {
        return key;
      }
    }
    for (String key : defaults.keySet()) {
      if (key.equalsIgnoreCase(n)) {
        return key;
      }
    }
    return null;
  }

  /** Records SET and RESET in the session settings. */
  private void setting(String sql) {
    java.util.regex.Matcher set = SET_VALUE.matcher(sql);
    if (set.matches()) {
      String name = set.group(1) != null ? set.group(1) : "TimeZone"; // SET TIME ZONE 'x'
      String value = (set.group(1) != null ? set.group(2) : set.group(3)).trim();
      if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
        value = value.substring(1, value.length() - 1).replace("''", "'");
      } else if (value.equalsIgnoreCase("DEFAULT")) {
        String key = settingKey(name);
        if (key != null) {
          settings.remove(key);
        }
        return;
      }
      String key = settingKey(name);
      if (MAX_ROWS_PER_WRITE.equals(key) && !value.matches("\\d{1,9}")) {
        throw new IllegalArgumentException(
            "invalid value for parameter \"" + MAX_ROWS_PER_WRITE + "\": \"" + value + "\"");
      }
      settings.put(key == null ? name : key, value);
      return;
    }
    java.util.regex.Matcher reset = RESET_NAME.matcher(sql);
    if (reset.matches()) {
      if (reset.group(1).equalsIgnoreCase("ALL")) {
        settings.clear();
      } else {
        String key = settingKey(reset.group(1));
        if (key != null) {
          settings.remove(key);
        }
      }
    } else if (firstWord(sql).equals("DISCARD")) {
      settings.clear();
    }
  }

  /** The session's cap on rows a read-then-write statement may write; 0 means no limit. */
  private int maxRowsPerWrite() {
    return Integer.parseInt(
        settings.getOrDefault(MAX_ROWS_PER_WRITE, defaults.get(MAX_ROWS_PER_WRITE)));
  }

  /** SHOW name, or SHOW ALL, as a constant result. */
  private QueryParser.Plan show(String sql) {
    java.util.regex.Matcher m = SHOW_NAME.matcher(sql);
    if (!m.matches()) {
      throw new IllegalArgumentException("Invalid SQL: " + sql);
    }
    Map<String, String> all = new LinkedHashMap<>(defaults);
    all.putAll(settings);
    if (m.group(1).equalsIgnoreCase("ALL")) {
      List<Map<String, Object>> rows = new ArrayList<>();
      for (Map.Entry<String, String> e : all.entrySet()) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("name", e.getKey());
        row.put("setting", e.getValue());
        row.put("description", null);
        rows.add(row);
      }
      return QueryParser.Plan.constant(
          java.util.Arrays.asList("name", "setting", "description"), rows, "SHOW");
    }
    String key = settingKey(m.group(1));
    if (key == null) {
      throw new IllegalArgumentException(
          "unrecognized configuration parameter \"" + m.group(1).trim() + "\"");
    }
    return QueryParser.Plan.constant(
        Collections.singletonList(key),
        Collections.singletonList(Collections.<String, Object>singletonMap(key, all.get(key))),
        "SHOW");
  }

  /** Thrown for any statement but COMMIT/ROLLBACK once a statement failed inside a transaction. */
  public static final class AbortedTransactionException extends IllegalStateException {
    AbortedTransactionException() {
      super("Current transaction is aborted, commands ignored until end of transaction block");
    }
  }

  /** The rows of one statement, pulled from ScalarDB as they are consumed; close it when done. */
  public final class Rows implements Iterator<Map<String, Object>>, AutoCloseable {
    @Nullable private final QueryParser.Plan plan;
    private final QueryParser.Cursor cursor;
    @Nullable private Map<String, Object> pending;
    private boolean exhausted;
    private int count;

    private Rows(@Nullable QueryParser.Plan plan, QueryParser.Cursor cursor) {
      this.plan = plan;
      this.cursor = cursor;
    }

    /** The output columns and their types where known (null for computed columns). */
    public Map<String, DataType> columns() {
      Map<String, DataType> columns = new LinkedHashMap<>();
      if (plan != null) {
        List<String> names = plan.getOutputColumns();
        List<DataType> types = plan.getOutputTypes();
        for (int i = 0; i < names.size(); i++) {
          columns.put(names.get(i), types.get(i));
        }
      }
      return columns;
    }

    /** The PostgreSQL type OIDs of the columns, inferred from the expressions; 0 when unknown. */
    public int[] oids() {
      return plan == null ? new int[0] : plan.outputOids();
    }

    /** The column names clients see, in the order of {@link #columns()}. */
    public List<String> labels() {
      return plan == null ? Collections.<String>emptyList() : plan.getColumnLabels();
    }

    /** True for a statement that produces a result set (even an empty one). */
    public boolean returnsRows() {
      return plan != null
          && (plan.commandTag(0).matches("(SELECT|EXPLAIN).*")
              || !plan.getOutputColumns().isEmpty()); // RETURNING
    }

    /**
     * @throws QueryParser.ReadFailure if a ScalarDB read fails; the cause is the original exception
     */
    @Override
    public boolean hasNext() {
      if (pending == null && !exhausted) {
        try {
          pending = cursor.next();
        } catch (RuntimeException e) {
          aborted |= transaction != null;
          throw e;
        }
        if (pending == null) {
          exhausted = true;
          if (plan != null) {
            lastCommandTag = plan.commandTag(count);
          }
        }
      }
      return pending != null;
    }

    @Override
    public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = pending;
      pending = null;
      count++;
      return row;
    }

    @Override
    public void close() {
      cursor.close();
    }
  }

  /**
   * Executes a SQL statement and returns all of its rows.
   *
   * @param sql a SQL statement
   * @return the rows for a {@code SELECT}, each a column-name-to-value map in select-list order; an
   *     empty list otherwise
   * @throws IllegalArgumentException if the SQL is invalid or unsupported
   * @throws IllegalStateException on {@code BEGIN} inside a transaction or {@code COMMIT}/{@code
   *     ROLLBACK} outside one
   * @throws TransactionException if the operation or the commit fails
   * @throws ExecutionException if the table metadata cannot be retrieved
   */
  public List<Map<String, Object>> execute(String sql)
      throws TransactionException, ExecutionException {
    try (Rows rows = open(sql)) {
      List<Map<String, Object>> out = new ArrayList<>();
      while (rows.hasNext()) {
        out.add(rows.next());
      }
      return out;
    } catch (QueryParser.ReadFailure e) {
      throw e.cause();
    }
  }

  /**
   * Executes a SQL statement, returning its rows as a stream to pull from. Control statements and
   * DML take effect before this method returns and yield no rows.
   */
  public Rows open(String sql) throws TransactionException, ExecutionException {
    return open(sql, Collections.emptyList());
  }

  /** As {@link #open(String)}, with values for the statement's {@code $n} placeholders. */
  public Rows open(String sql, List<Object> parameters)
      throws TransactionException, ExecutionException {
    String word = firstWord(sql);
    if (aborted && !ENDS_TRANSACTION.contains(word)) {
      throw new AbortedTransactionException();
    }
    switch (word) {
      case "BEGIN":
      case "START":
        if (transaction != null) {
          throw new IllegalStateException("A transaction is already active");
        }
        // BEGIN READ ONLY (what JDBC sends for a read-only connection) starts a read-only
        // ScalarDB transaction: no coordinator write and no validation at commit
        transaction = READ_ONLY.matcher(sql).find() ? manager.beginReadOnly() : manager.begin();
        lastCommandTag = "BEGIN";
        return new Rows(null, QueryParser.Cursor.EMPTY);
      case "COMMIT":
      case "END":
        if (transaction == null) {
          // As in PostgreSQL, only a warning: the error that ended the transaction stays visible
          lastCommandTag = "COMMIT";
          return new Rows(null, QueryParser.Cursor.EMPTY);
        }
        if (aborted) {
          // As in PostgreSQL, COMMIT of an aborted transaction rolls it back
          rollback();
          return new Rows(null, QueryParser.Cursor.EMPTY);
        }
        try {
          active().commit();
        } finally {
          transaction = null;
          savepoints.clear();
          writesSinceSavepoint = 0;
        }
        lastCommandTag = "COMMIT";
        return new Rows(null, QueryParser.Cursor.EMPTY);
      case "ROLLBACK":
      case "ABORT":
        if (SAVEPOINT_NAME.matcher(sql).matches()
            && sql.toUpperCase(Locale.ROOT).contains(" TO ")) {
          // ROLLBACK TO SAVEPOINT: fine while the savepoint saw only reads, which also clears an
          // error since the failed statement wrote nothing
          if (transaction == null) {
            throw new IllegalStateException(
                "ROLLBACK TO SAVEPOINT can only be used in transaction blocks");
          }
          if (writesSinceSavepoint > 0) {
            throw new IllegalArgumentException(
                "Cannot roll back to a savepoint after writes: ScalarDB has no partial rollback");
          }
          aborted = false;
          lastCommandTag = "ROLLBACK";
          return new Rows(null, QueryParser.Cursor.EMPTY);
        }
        rollback();
        return new Rows(null, QueryParser.Cursor.EMPTY);
      case "SAVEPOINT":
        if (transaction == null) {
          throw new IllegalStateException("SAVEPOINT can only be used in transaction blocks");
        }
        java.util.regex.Matcher sp = SAVEPOINT_NAME.matcher(sql);
        savepoints.push(sp.matches() ? sp.group(1) : "");
        writesSinceSavepoint = 0;
        lastCommandTag = "SAVEPOINT";
        return new Rows(null, QueryParser.Cursor.EMPTY);
      case "RELEASE":
        if (transaction == null) {
          throw new IllegalStateException(
              "RELEASE SAVEPOINT can only be used in transaction blocks");
        }
        if (!savepoints.isEmpty()) {
          savepoints.pop();
        }
        lastCommandTag = "RELEASE";
        return new Rows(null, QueryParser.Cursor.EMPTY);
      case "SET":
      case "RESET":
      case "DISCARD":
        // Session settings are recorded for SHOW; only the write cap changes behavior
        setting(sql);
        lastCommandTag = firstWord(sql).equals("DISCARD") ? "DISCARD ALL" : firstWord(sql);
        return new Rows(null, QueryParser.Cursor.EMPTY);
      case "SHOW":
        {
          QueryParser.Plan plan = show(sql);
          return new Rows(plan, plan.open(manager));
        }
      default:
        if (transaction != null && WRITES.contains(word)) {
          writesSinceSavepoint++;
        }
        try {
          return statement(sql, parameters);
        } catch (RuntimeException | TransactionException | ExecutionException e) {
          aborted |= transaction != null;
          throw e;
        }
    }
  }

  private static final java.util.Set<String> ENDS_TRANSACTION =
      new java.util.HashSet<>(java.util.Arrays.asList("COMMIT", "END", "ROLLBACK", "ABORT"));

  private void rollback() throws TransactionException {
    try {
      if (transaction != null) { // outside a transaction PostgreSQL only warns
        transaction.rollback();
      }
    } finally {
      transaction = null;
      aborted = false;
      savepoints.clear();
      writesSinceSavepoint = 0;
    }
    lastCommandTag = "ROLLBACK";
  }

  /** A statement other than transaction control and session settings. */
  private Rows statement(String sql, List<Object> parameters)
      throws TransactionException, ExecutionException {
    {
      QueryParser.Plan plan =
          sql.equals(describedSql) && parameters.equals(describedParameters)
              ? describedPlan
              : plan(sql, parameters);
      describedSql = null;
      describedPlan = null;
      if (plan.getDdl() != null) {
        // ScalarDB DDL is not transactional: inside BEGIN it takes effect at once and a later
        // ROLLBACK does not undo it. Migration tools wrap DDL in a transaction, so it is allowed.
        plan.getDdl().run();
        parser.invalidateMetadata();
        planCache.clear();
        lastCommandTag = plan.commandTag(0);
        settleMetadata(lastCommandTag);
        return new Rows(null, QueryParser.Cursor.EMPTY);
      }
      if (plan.getWrite() != null) {
        // The read and the writes must share one transaction
        int written;
        if (transaction != null) {
          written = plan.executeWrite(transaction, maxRowsPerWrite());
        } else {
          DistributedTransaction own = manager.begin();
          try {
            written = plan.executeWrite(own, maxRowsPerWrite());
            own.commit();
          } catch (RuntimeException | TransactionException e) {
            try {
              own.rollback();
            } catch (TransactionException rollback) {
              e.addSuppressed(rollback);
            }
            throw e;
          }
        }
        lastCommandTag = plan.commandTag(written);
        if (plan.getAnalyze() != null) {
          return new Rows(plan, plan.report());
        }
        return plan.getOutputColumns().isEmpty()
            ? new Rows(null, QueryParser.Cursor.EMPTY)
            : new Rows(plan, plan.returned());
      }
      QueryParser.Cursor cursor = transaction == null ? plan.open(manager) : plan.open(transaction);
      return new Rows(plan, cursor);
    }
  }

  /**
   * Plans a statement without executing it and returns its output columns.
   *
   * @param sql a SQL statement
   * @return the output column names mapped to their type where known from table metadata (null for
   *     computed columns); empty for statements that return no rows
   * @throws IllegalArgumentException if the SQL is invalid or unsupported
   * @throws ExecutionException if the table metadata cannot be retrieved
   */
  public Map<String, DataType> describe(String sql) throws ExecutionException {
    return describe(sql, Collections.emptyList());
  }

  /** As {@link #describe(String)}, with values for the statement's {@code $n} placeholders. */
  /** The PostgreSQL type OIDs of the columns {@link #describe} last returned; 0 when unknown. */
  public int[] describedOids() {
    return describedPlan == null ? new int[0] : describedPlan.outputOids();
  }

  /** The column names clients see for the statement {@link #describe} last returned columns for. */
  public List<String> describedLabels() {
    return describedPlan == null
        ? Collections.<String>emptyList()
        : describedPlan.getColumnLabels();
  }

  public Map<String, DataType> describe(String sql, List<Object> parameters)
      throws ExecutionException {
    Map<String, DataType> columns = new LinkedHashMap<>();
    String word = firstWord(sql);
    if (aborted && !ENDS_TRANSACTION.contains(word)) {
      throw new AbortedTransactionException();
    }
    switch (word) {
      case "BEGIN":
      case "START":
      case "COMMIT":
      case "END":
      case "ROLLBACK":
      case "ABORT":
      case "SET":
      case "RESET":
      case "DISCARD":
      case "SAVEPOINT":
      case "RELEASE":
        return columns;
      case "SHOW":
        for (String name : show(sql).getOutputColumns()) {
          columns.put(name, null);
        }
        return columns;
      default:
        QueryParser.Plan plan = plan(sql, parameters);
        describedSql = sql;
        describedParameters = new ArrayList<>(parameters);
        describedPlan = plan;
        List<String> names = plan.getOutputColumns();
        List<DataType> types = plan.getOutputTypes();
        for (int i = 0; i < names.size(); i++) {
          columns.put(names.get(i), types.get(i));
        }
        return columns;
    }
  }

  /**
   * The cached plan for the statement, rebound to the values, or a new one (cached if it can be).
   */
  private QueryParser.Plan plan(String sql, List<Object> parameters) throws ExecutionException {
    QueryParser.Plan plan = planCache.get(sql);
    if (plan != null) {
      plan.rebind(parameters);
      return plan;
    }
    plan = parser.parse(sql, parameters);
    if (plan.isCacheable()) {
      planCache.put(sql, plan);
    }
    return plan;
  }

  /**
   * ScalarDB caches table metadata for {@code scalar.db.metadata.cache_expiration_time_secs} and
   * its admin does not refresh that cache, so after DDL that changes an existing table the engine
   * would reject the new columns or miss a new index until the entry expires. Answering such DDL
   * only after this many milliseconds keeps the client's next statement correct.
   */
  public void setDdlSettleMillis(long millis) {
    ddlSettleMillis = millis;
  }

  private void settleMetadata(String commandTag) {
    if (ddlSettleMillis > 0 && CHANGES_EXISTING_TABLES.contains(commandTag)) {
      // ponytail: sleeping past the cache expiry is the only hook; a core API to invalidate the
      // table metadata cache would make this immediate
      try {
        Thread.sleep(ddlSettleMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Plans cached in this session. */
  int cachedPlans() {
    return planCache.size();
  }

  /** The PostgreSQL command tag of the last executed statement, such as {@code SELECT 3}. */
  public String lastCommandTag() {
    return lastCommandTag;
  }

  public boolean inTransaction() {
    return transaction != null;
  }

  /**
   * Records an error reported to the client outside {@link #open}, such as while describing a
   * statement: inside a transaction it aborts the transaction, which then accepts only {@code
   * ROLLBACK} or {@code COMMIT} (which rolls back).
   */
  public void fail() {
    aborted |= transaction != null;
  }

  /** True once a statement failed inside the transaction, until it is rolled back. */
  public boolean isAborted() {
    return aborted;
  }

  private DistributedTransaction active() {
    if (transaction == null) {
      throw new IllegalStateException("No active transaction");
    }
    return transaction;
  }

  private static String firstWord(String sql) {
    String[] words = sql.trim().replaceAll(";\\s*$", "").trim().split("\\s+", 2);
    return words[0].toUpperCase(Locale.ROOT);
  }
}
