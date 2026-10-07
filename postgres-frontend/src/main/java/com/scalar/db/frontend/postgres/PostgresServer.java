package com.scalar.db.frontend.postgres;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.DataType;
import com.scalar.db.service.TransactionFactory;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PostgreSQL wire-protocol (version 3) server in front of ScalarDB. It speaks the simple and the
 * extended query protocol with text-format parameters and results, without authentication or TLS.
 * The database name of a connection is used as the default ScalarDB namespace, and each connection
 * is a session with its own transaction state.
 */
public class PostgresServer implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(PostgresServer.class);
  private static final int SSL_REQUEST = 80877103;
  private static final int GSS_REQUEST = 80877104;
  private static final int CANCEL_REQUEST = 80877102;
  private static final int TEXT_OID = 25;

  private final DistributedTransactionManager manager;
  private final DistributedTransactionAdmin admin;
  private final ServerSocket serverSocket;

  public PostgresServer(
      DistributedTransactionManager manager, DistributedTransactionAdmin admin, int port)
      throws IOException {
    this.manager = manager;
    this.admin = admin;
    this.serverSocket = new ServerSocket(port);
  }

  /** Session defaults every connection starts with, such as the transaction isolation level. */
  final Map<String, String> defaults = new HashMap<>();

  /** Longest metadata cache expiry worth waiting out after DDL, see {@link #main}. */
  static final long MAX_SETTLE_SECS = 5;

  /** Milliseconds a session waits after DDL on an existing table, see {@link #main}. */
  long ddlSettleMillis;

  /** Usage: {@code PostgresServer <scalardb.properties> [port]}; the port defaults to 5432. */
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: PostgresServer <scalardb.properties> [port]");
      System.exit(1);
    }
    java.util.Properties properties = new java.util.Properties();
    try (java.io.FileInputStream in = new java.io.FileInputStream(args[0])) {
      properties.load(in);
    }
    // The engine caches table metadata (60 s by default) and its admin does not refresh the cache,
    // so DDL on an existing table would not be seen for that long: keep the window short and have
    // sessions wait it out after such DDL (QueryProcessor.setDdlSettleMillis)
    properties.putIfAbsent(DatabaseConfig.METADATA_CACHE_EXPIRATION_TIME_SECS, "1");
    long cacheSecs = DatabaseConfig.getMetadataCacheExpirationTimeSecs(properties);
    TransactionFactory factory = TransactionFactory.create(properties);
    int port = args.length > 1 ? Integer.parseInt(args[1]) : 5432;
    PostgresServer server =
        new PostgresServer(factory.getTransactionManager(), factory.getTransactionAdmin(), port);
    if (cacheSecs >= 0 && cacheSecs <= MAX_SETTLE_SECS) {
      server.ddlSettleMillis = cacheSecs * 1000 + 50;
    } else {
      logger.warn(
          "Table metadata is cached for {} s: DDL on an existing table takes effect for the engine"
              + " only after that",
          cacheSecs);
    }
    // SHOW TRANSACTION ISOLATION LEVEL: Consensus Commit's SNAPSHOT reads like REPEATABLE READ
    String isolation = properties.getProperty("scalar.db.consensus_commit.isolation_level", "");
    server.defaults.put(
        "transaction_isolation",
        isolation.equalsIgnoreCase("SERIALIZABLE") ? "serializable" : "repeatable read");
    logger.info("Listening on port {}", server.getPort());
    server.acceptLoop();
  }

  public int getPort() {
    return serverSocket.getLocalPort();
  }

  /** Accepts connections on a daemon thread. */
  public void start() {
    Thread thread = new Thread(this::acceptLoop, "postgres-frontend-acceptor");
    thread.setDaemon(true);
    thread.start();
  }

  @Override
  public void close() throws IOException {
    serverSocket.close();
  }

  private void acceptLoop() {
    while (!serverSocket.isClosed()) {
      try {
        Socket socket = serverSocket.accept();
        Thread thread = new Thread(new Connection(socket), "postgres-frontend-" + socket.getPort());
        thread.setDaemon(true);
        thread.start();
      } catch (IOException e) {
        if (!serverSocket.isClosed()) {
          logger.warn("Accept failed", e);
        }
      }
    }
  }

  /** One client connection: the startup handshake, then the message loop. */
  private final class Connection implements Runnable {
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final Map<String, String> statements = new HashMap<>(); // name -> SQL with $n
    private final Map<String, int[]> parameterTypes = new HashMap<>(); // name -> declared OIDs
    private final Map<String, Portal> portals = new HashMap<>();

    Connection(Socket socket) throws IOException {
      this.socket = socket;
      this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
      this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    @Override
    public void run() {
      QueryProcessor processor = null;
      try {
        processor = startup();
        if (processor != null) {
          serve(processor);
        }
      } catch (IOException e) {
        logger.debug("Connection ended: {}", e.toString());
      } catch (RuntimeException | Error e) {
        logger.warn("Connection failed", e);
      } finally {
        if (processor != null && processor.inTransaction()) {
          try {
            processor.execute("ROLLBACK");
          } catch (Exception e) {
            logger.warn("Rollback on disconnect failed", e);
          }
        }
        try {
          socket.close();
        } catch (IOException e) {
          logger.debug("Close failed: {}", e.toString());
        }
      }
    }

    @Nullable
    private QueryProcessor startup() throws IOException {
      while (true) {
        int length = in.readInt();
        int code = in.readInt();
        if (code == SSL_REQUEST || code == GSS_REQUEST) {
          out.writeByte('N'); // neither SSL nor GSS encryption is supported
          out.flush();
          continue;
        }
        if (code == CANCEL_REQUEST) {
          return null;
        }
        byte[] body = new byte[length - 8];
        in.readFully(body);
        ByteBuffer msg = ByteBuffer.wrap(body);
        Map<String, String> params = new HashMap<>();
        while (msg.hasRemaining()) {
          String key = readString(msg);
          if (key.isEmpty()) {
            break;
          }
          params.put(key, readString(msg));
        }
        String user = params.getOrDefault("user", "");
        String database = params.getOrDefault("database", user);
        send('R', new Msg().i32(0)); // AuthenticationOk
        String[][] status = {
          {"server_version", "16.0"},
          {"server_encoding", "UTF8"},
          {"client_encoding", "UTF8"},
          {"DateStyle", "ISO, MDY"},
          {"integer_datetimes", "on"},
          {"standard_conforming_strings", "on"},
          {"TimeZone", "UTC"},
          {"is_superuser", "off"},
          {"session_authorization", user},
        };
        for (String[] s : status) {
          send('S', new Msg().str(s[0]).str(s[1]));
        }
        send(
            'K',
            new Msg()
                .i32(ThreadLocalRandom.current().nextInt())
                .i32(ThreadLocalRandom.current().nextInt()));
        QueryProcessor processor = new QueryProcessor(manager, admin, database);
        processor.setDdlSettleMillis(ddlSettleMillis);
        for (Map.Entry<String, String> e : defaults.entrySet()) {
          processor.setDefault(e.getKey(), e.getValue());
        }
        for (Map.Entry<String, String> e : params.entrySet()) {
          if (!e.getKey().equals("user") && !e.getKey().equals("database")) {
            processor.setDefault(e.getKey(), e.getValue()); // application_name, DateStyle, ...
          }
        }
        readyForQuery(processor);
        return processor;
      }
    }

    private void serve(QueryProcessor processor) throws IOException {
      boolean failed = false; // extended protocol: after an error, skip messages until Sync
      while (true) {
        int type = in.read();
        if (type < 0 || type == 'X') {
          return;
        }
        int length = in.readInt();
        byte[] body = new byte[length - 4];
        in.readFully(body);
        ByteBuffer msg = ByteBuffer.wrap(body);
        if (type == 'S') {
          failed = false;
          readyForQuery(processor);
          continue;
        }
        if (type == 'H') {
          out.flush();
          continue;
        }
        if (failed) {
          continue;
        }
        try {
          switch (type) {
            case 'Q':
              simpleQuery(processor, readString(msg));
              break;
            case 'P':
              String name = readString(msg);
              statements.put(name, readString(msg));
              int[] oids = new int[msg.getShort()];
              for (int i = 0; i < oids.length; i++) {
                oids[i] = msg.getInt();
              }
              parameterTypes.put(name, oids);
              send('1', new Msg()); // ParseComplete
              break;
            case 'B':
              bind(msg);
              break;
            case 'D':
              describe(processor, msg);
              break;
            case 'E':
              execute(processor, msg);
              break;
            case 'C':
              char kind = (char) msg.get();
              String closing = readString(msg);
              (kind == 'S' ? statements : portals).remove(closing);
              if (kind == 'S') {
                parameterTypes.remove(closing);
              }
              send('3', new Msg()); // CloseComplete
              break;
            default:
              throw new IllegalArgumentException("Unsupported message type: " + (char) type);
          }
        } catch (Exception | Error e) {
          // an Error (a stack overflow on deeply nested SQL, a missing class) is reported like any
          // failure and the connection goes on, instead of dying with no message to the client
          sendError(e);
          processor.fail(); // any error inside a transaction block aborts it, as in PostgreSQL
          if (type == 'Q') {
            readyForQuery(processor);
          } else {
            failed = true;
          }
        }
        out.flush();
      }
    }

    private void simpleQuery(QueryProcessor processor, String sql) throws Exception {
      List<String> parts = split(sql);
      if (parts.isEmpty()) {
        send('I', new Msg()); // EmptyQueryResponse
      }
      for (String part : parts) {
        run(processor, part, Collections.emptyList(), true, new short[0]);
      }
      readyForQuery(processor);
    }

    /** Runs one statement, streaming its rows (with a RowDescription if asked) and command tag. */
    /**
     * Runs one statement, streaming its rows (with a RowDescription if asked) and command tag.
     * {@code formats} are the result-column format codes the client asked for in Bind (none for the
     * simple protocol): 0 text, 1 binary.
     */
    private void run(
        QueryProcessor processor,
        String sql,
        List<Object> parameters,
        boolean describe,
        short[] formats)
        throws Exception {
      try (QueryProcessor.Rows rows = processor.open(sql, parameters)) {
        Map<String, DataType> columns = rows.columns();
        if (!columns.isEmpty() || rows.returnsRows()) {
          // The first row is fetched before the RowDescription so computed columns get a type
          Map<String, Object> first = rows.hasNext() ? rows.next() : null;
          int[] oids =
              oids(columns, first == null ? null : Collections.singletonList(first), rows.oids());
          if (describe) {
            sendRowDescription(rows.labels(), oids, formats);
          }
          int sent = 0;
          for (Map<String, Object> row = first;
              row != null;
              row = rows.hasNext() ? rows.next() : null) {
            Msg m = new Msg().i16(columns.size());
            int i = 0;
            for (String column : columns.keySet()) {
              Object value = row.get(column);
              if (value == null) {
                m.i32(-1);
              } else {
                byte[] bytes =
                    format(formats, i) == 1
                        ? binary(oids[i], value)
                        : text(value).getBytes(StandardCharsets.UTF_8);
                m.i32(bytes.length).bytes(bytes);
              }
              i++;
            }
            send('D', m);
            if (++sent % 64 == 0) {
              out.flush();
            }
          }
        } else {
          while (rows.hasNext()) {
            rows.next();
          }
        }
      }
      send('C', new Msg().str(processor.lastCommandTag()));
    }

    private void bind(ByteBuffer msg) throws IOException {
      String portal = readString(msg);
      String statement = readString(msg);
      String sql = statements.get(statement);
      if (sql == null) {
        throw new IllegalArgumentException("Unknown prepared statement");
      }
      int[] oids = parameterTypes.getOrDefault(statement, new int[0]);
      short[] formats = new short[msg.getShort()];
      for (int i = 0; i < formats.length; i++) {
        formats[i] = msg.getShort();
      }
      int count = msg.getShort();
      List<String> params = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        int length = msg.getInt();
        if (length < 0) {
          params.add(null);
          continue;
        }
        byte[] value = new byte[length];
        msg.get(value);
        short format = formats.length == 0 ? 0 : formats.length == 1 ? formats[0] : formats[i];
        int oid = i < oids.length ? oids[i] : 0;
        params.add(format == 0 ? new String(value, StandardCharsets.UTF_8) : binary(oid, value));
      }
      short[] resultFormats = new short[msg.getShort()];
      for (int i = 0; i < resultFormats.length; i++) {
        resultFormats[i] = msg.getShort();
      }
      List<Object> values = new ArrayList<>();
      for (int i = 0; i < params.size(); i++) {
        values.add(typed(params.get(i), i < oids.length ? oids[i] : 0));
      }
      portals.put(portal, new Portal(sql, values, resultFormats));
      send('2', new Msg()); // BindComplete
    }

    private void describe(QueryProcessor processor, ByteBuffer msg) throws Exception {
      char kind = (char) msg.get();
      String name = readString(msg);
      Map<String, DataType> columns;
      short[] formats = new short[0];
      if (kind == 'S') {
        String sql = statements.get(name);
        if (sql == null) {
          throw new IllegalArgumentException("Unknown prepared statement");
        }
        int n = parameterCount(sql);
        int[] oids = parameterTypes.getOrDefault(name, new int[0]);
        Msg m = new Msg().i16(n);
        for (int i = 0; i < n; i++) {
          m.i32(i < oids.length ? oids[i] : 0); // 0: parameter type unspecified
        }
        send('t', m); // ParameterDescription
        try {
          columns = processor.describe(sql, Collections.nCopies(n, null));
        } catch (RuntimeException e) {
          columns = Collections.emptyMap(); // the row shape depends on the parameter values
        }
      } else {
        Portal portal = portals.get(name);
        if (portal == null) {
          throw new IllegalArgumentException("Unknown portal");
        }
        columns =
            portal.sql.trim().isEmpty()
                ? Collections.emptyMap()
                : processor.describe(portal.sql, portal.parameters);
        formats = portal.formats;
      }
      if (columns.isEmpty()) {
        send('n', new Msg()); // NoData
      } else {
        sendRowDescription(
            processor.describedLabels(), oids(columns, null, processor.describedOids()), formats);
      }
    }

    private void execute(QueryProcessor processor, ByteBuffer msg) throws Exception {
      Portal portal = portals.get(readString(msg));
      if (portal == null) {
        throw new IllegalArgumentException("Unknown portal");
      }
      // The maximum row count is ignored: all rows are always sent
      if (portal.sql.trim().isEmpty()) {
        send('I', new Msg());
      } else {
        run(processor, portal.sql, portal.parameters, false, portal.formats);
      }
    }

    private void sendRowDescription(List<String> labels, int[] oids, short[] formats)
        throws IOException {
      Msg m = new Msg().i16(labels.size());
      for (int i = 0; i < labels.size(); i++) {
        m.str(labels.get(i))
            .i32(0)
            .i16(0)
            .i32(oids[i])
            .i16(size(oids[i]))
            .i32(-1)
            .i16(format(formats, i));
      }
      send('T', m);
    }

    private void sendError(Throwable failure) throws IOException {
      Throwable e = failure instanceof QueryParser.ReadFailure ? failure.getCause() : failure;
      String code = sqlState(e);
      String message = e.getMessage() == null ? e.toString() : e.getMessage();
      if (e instanceof Error) {
        logger.warn("Statement failed with an error", e);
      } else {
        logger.info("Statement failed: {}", message);
      }
      send(
          'E',
          new Msg()
              .i8('S')
              .str("ERROR")
              .i8('V')
              .str("ERROR")
              .i8('C')
              .str(code)
              .i8('M')
              .str(message)
              .i8(0));
    }

    /**
     * The SQLSTATE for a failure. The frontend's own errors are told apart by their message
     * prefixes.
     */
    // ponytail: prefix matching on our own messages; a typed exception per error class if this
    // grows
    private String sqlState(Throwable e) {
      String m = e.getMessage() == null ? "" : e.getMessage();
      if (e instanceof Error) {
        return "XX000"; // internal_error
      }
      if (e instanceof QueryParser.UniqueViolationException) {
        return "23505"; // unique_violation
      }
      if (e instanceof QueryProcessor.AbortedTransactionException) {
        return "25P02"; // in_failed_sql_transaction
      }
      if (e instanceof CrudConflictException || e instanceof CommitConflictException) {
        return "40001"; // serialization_failure: the client may retry
      }
      if (e instanceof UnknownTransactionStatusException) {
        return "40003"; // statement_completion_unknown
      }
      if (e instanceof ArithmeticException) {
        return m.contains("by zero") ? "22012" : "22003"; // division_by_zero, out of range
      }
      if (e instanceof DateTimeException) {
        return "22007"; // invalid_datetime_format
      }
      if (e instanceof NumberFormatException || m.startsWith("Cannot cast")) {
        return "22P02"; // invalid_text_representation
      }
      if (e instanceof IllegalStateException) {
        return "25000"; // invalid_transaction_state
      }
      if (m.startsWith("invalid input syntax for type date")
          || m.startsWith("invalid input syntax for type time")) {
        return "22007"; // invalid_datetime_format
      }
      if (m.startsWith("invalid input syntax")) {
        return "22P02"; // invalid_text_representation
      }
      if (m.startsWith("date/time field value out of range")) {
        return "22008"; // datetime_field_overflow
      }
      if (m.startsWith("value \"") && m.contains("is out of range")) {
        return "22003"; // numeric_value_out_of_range
      }
      if (m.startsWith("The statement would write more than")) {
        return "54000"; // program_limit_exceeded: the write cap
      }
      if (m.startsWith("invalid value for parameter")) {
        return "22023"; // invalid_parameter_value
      }
      if (m.startsWith("unrecognized configuration parameter")) {
        return "42704"; // undefined_object
      }
      if (m.startsWith("null value in column")) {
        return "23502"; // not_null_violation
      }
      if (m.startsWith("division by zero")) {
        return "22012";
      }
      if (m.startsWith("Unknown column")) {
        return "42703"; // undefined_column
      }
      if (m.startsWith("Ambiguous column")) {
        return "42702"; // ambiguous_column
      }
      if (m.startsWith("Table not found")) {
        return "42P01"; // undefined_table
      }
      if (m.startsWith("Unsupported") || m.contains("are not supported")) {
        return "0A000"; // feature_not_supported
      }
      if (m.startsWith("Subquery must return")) {
        return "21000"; // cardinality_violation
      }
      if (m.contains("DB-CORE-10020")) {
        return "23502"; // not_null_violation: a NULL key column
      }
      return e instanceof IllegalArgumentException ? "42601" : "XX000";
    }

    private void readyForQuery(QueryProcessor processor) throws IOException {
      send('Z', new Msg().i8(processor.isAborted() ? 'E' : processor.inTransaction() ? 'T' : 'I'));
      out.flush();
    }

    private void send(char type, Msg m) throws IOException {
      byte[] body = m.bytes.toByteArray();
      out.writeByte(type);
      out.writeInt(body.length + 4);
      out.write(body);
    }
  }

  /** A message body under construction. */
  private static final class Msg {
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final DataOutputStream data = new DataOutputStream(bytes);

    Msg i8(int v) throws IOException {
      data.writeByte(v);
      return this;
    }

    Msg i16(int v) throws IOException {
      data.writeShort(v);
      return this;
    }

    Msg i32(int v) throws IOException {
      data.writeInt(v);
      return this;
    }

    Msg bytes(byte[] v) throws IOException {
      data.write(v);
      return this;
    }

    Msg str(String s) throws IOException {
      data.write(s.getBytes(StandardCharsets.UTF_8));
      data.writeByte(0);
      return this;
    }
  }

  private static String readString(ByteBuffer b) {
    int start = b.position();
    while (b.get() != 0) {
      // find the terminator
    }
    return new String(
        b.array(), b.arrayOffset() + start, b.position() - start - 1, StandardCharsets.UTF_8);
  }

  /** Splits a simple-query string into statements on semicolons outside quotes. */
  static List<String> split(String sql) {
    List<String> out = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean inString = false;
    boolean inIdentifier = false;
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (c == '\'' && !inIdentifier) {
        inString = !inString;
      } else if (c == '"' && !inString) {
        inIdentifier = !inIdentifier;
      }
      if (c == ';' && !inString && !inIdentifier) {
        if (current.toString().trim().length() > 0) {
          out.add(current.toString().trim());
        }
        current.setLength(0);
      } else {
        current.append(c);
      }
    }
    if (current.toString().trim().length() > 0) {
      out.add(current.toString().trim());
    }
    return out;
  }

  /** Replaces {@code $n} placeholders outside quotes with quoted literals (or NULL). */
  /** A bound statement: its SQL with {@code $n} placeholders, their values, and result formats. */
  private static final class Portal {
    final String sql;
    final List<Object> parameters;
    final short[] formats;

    Portal(String sql, List<Object> parameters, short[] formats) {
      this.sql = sql;
      this.parameters = parameters;
      this.formats = formats;
    }
  }

  /**
   * A parameter as the value its literal would have had: a Long or Double for a number, a Boolean,
   * else the text. Numbers and booleans are recognized by the declared type or, for an undeclared
   * one, by the text itself, as a client writing literals in simple-query mode would have sent.
   */
  @Nullable
  static Object typed(@Nullable String value, int oid) {
    if (value == null) {
      return null;
    }
    if (oid == 16 && (value.equals("t") || value.equals("f"))) {
      return value.equals("t");
    }
    if (!plain(value, oid)) {
      return value;
    }
    if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
      return Boolean.valueOf(value.toLowerCase(Locale.ROOT));
    }
    try {
      return Long.valueOf(value);
    } catch (NumberFormatException e) {
      if (oid == 700 || oid == 701) {
        return Double.valueOf(value);
      }
      try {
        return Evaluator.decimal(value); // numeric, or an undeclared decimal: exact, as a literal
      } catch (NumberFormatException notDecimal) {
        return Double.valueOf(value); // NaN, Infinity
      }
    }
  }

  /** The format code of result column {@code i}: one code applies to all, or one per column. */
  private static int format(short[] formats, int i) {
    return formats.length == 0 ? 0 : formats.length == 1 ? formats[0] : formats[i];
  }

  private static int[] oids(
      Map<String, DataType> columns, @Nullable List<Map<String, Object>> rows, int[] inferred) {
    int[] oids = new int[columns.size()];
    int i = 0;
    for (Map.Entry<String, DataType> column : columns.entrySet()) {
      int oid = oid(column.getValue(), column.getKey(), rows);
      // A computed column: the type inferred from the expression is PostgreSQL's (name[] for
      // array_agg(attname), int2vector for indkey); the first value only decides when it is not
      if (column.getValue() == null
          && i < inferred.length
          && inferred[i] != 0
          && (inferred[i] != TEXT_OID || oid == TEXT_OID)) {
        oid = inferred[i];
      }
      oids[i++] = oid;
    }
    return oids;
  }

  private static final LocalDateTime PG_EPOCH = LocalDateTime.of(2000, 1, 1, 0, 0);

  /** A value in PostgreSQL's binary result format for the type {@code oid}. */
  static byte[] binary(int oid, Object value) {
    switch (oid) {
      case 3802: // jsonb: a version byte, then the text
        {
          byte[] text = Evaluator.text(value).getBytes(StandardCharsets.UTF_8);
          byte[] out = new byte[text.length + 1];
          out[0] = 1;
          System.arraycopy(text, 0, out, 1, text.length);
          return out;
        }
      case 114: // json
        return Evaluator.text(value).getBytes(StandardCharsets.UTF_8);
      case 2950: // uuid
        {
          java.util.UUID uuid = java.util.UUID.fromString(Evaluator.text(value).trim());
          return ByteBuffer.allocate(16)
              .putLong(uuid.getMostSignificantBits())
              .putLong(uuid.getLeastSignificantBits())
              .array();
        }
      case 16:
        return new byte[] {(byte) (parseBoolean(value) ? 1 : 0)};
      case 21:
        return ByteBuffer.allocate(2).putShort((short) number(value).intValue()).array();
      case 23:
        return ByteBuffer.allocate(4).putInt(number(value).intValue()).array();
      case 20:
        return ByteBuffer.allocate(8).putLong(number(value).longValue()).array();
      case 700:
        return ByteBuffer.allocate(4).putFloat(number(value).floatValue()).array();
      case 701:
        return ByteBuffer.allocate(8).putDouble(number(value).doubleValue()).array();
      case 1700:
        return numeric(
            value instanceof java.math.BigDecimal
                ? (java.math.BigDecimal) value
                : new java.math.BigDecimal(number(value).toString()));
      case 1000:
      case 1001:
      case 1005:
      case 1007:
      case 1016:
      case 1021:
      case 1022:
      case 1009:
      case 1014:
      case 1015:
      case 1182:
      case 1183:
      case 1115:
      case 1185:
      case 1231:
        return arrayBinary((int) Catalog.elementOid(oid), Evaluator.elements(value));
      case 17:
        if (value instanceof ByteBuffer) {
          ByteBuffer b = ((ByteBuffer) value).duplicate();
          byte[] bytes = new byte[b.remaining()];
          b.get(bytes);
          return bytes;
        }
        return value instanceof byte[] ? (byte[]) value : QueryParser.hex(text(value));
      case 1082:
        return ByteBuffer.allocate(4)
            .putInt(
                (int)
                    java.time.temporal.ChronoUnit.DAYS.between(
                        PG_EPOCH.toLocalDate(), (LocalDate) value))
            .array();
      case 1083:
        return ByteBuffer.allocate(8).putLong(((LocalTime) value).toNanoOfDay() / 1000).array();
      case 1114:
        return ByteBuffer.allocate(8)
            .putLong(java.time.temporal.ChronoUnit.MICROS.between(PG_EPOCH, (LocalDateTime) value))
            .array();
      case 1184:
        return ByteBuffer.allocate(8)
            .putLong(
                java.time.temporal.ChronoUnit.MICROS.between(
                    PG_EPOCH.toInstant(ZoneOffset.UTC), (Instant) value))
            .array();
      default:
        return text(value).getBytes(StandardCharsets.UTF_8); // text types: the same bytes
    }
  }

  private static Number number(Object value) {
    return value instanceof Number ? (Number) value : new java.math.BigDecimal(value.toString());
  }

  private static boolean parseBoolean(Object value) {
    return value instanceof Boolean
        ? (Boolean) value
        : value.toString().equalsIgnoreCase("true") || value.toString().equalsIgnoreCase("t");
  }

  private static final java.util.regex.Pattern NUMBER =
      java.util.regex.Pattern.compile("-?\\d+(\\.\\d+)?([eE][-+]?\\d+)?");

  /** True if the parameter stands as it is in SQL: a number or a boolean. */
  private static boolean plain(String value, int oid) {
    switch (oid) {
      case 16: // bool
      case 20: // int8
      case 21: // int2
      case 23: // int4
      case 700: // float4
      case 701: // float8
      case 1700: // numeric
        return true;
      case 0: // undeclared: the text decides
        return NUMBER.matcher(value).matches()
            || value.equalsIgnoreCase("true")
            || value.equalsIgnoreCase("false");
      default:
        return false;
    }
  }

  /** The highest {@code $n} placeholder number in the statement. */
  static int parameterCount(String sql) {
    return QueryParser.parameterCount(sql);
  }

  /** Decodes a binary-format parameter into the text a client would have sent. */
  static String binary(int oid, byte[] value) {
    ByteBuffer b = ByteBuffer.wrap(value);
    switch (oid) {
      case 3802: // jsonb: a version byte, then the text
        return value.length > 0 && value[0] == 1
            ? new String(value, 1, value.length - 1, StandardCharsets.UTF_8)
            : new String(value, StandardCharsets.UTF_8);
      case 114:
        return new String(value, StandardCharsets.UTF_8);
      case 2950: // uuid: 16 raw bytes, kept as its text form (a uuid column is TEXT here)
        {
          if (value.length != 16) {
            throw new IllegalArgumentException("Invalid binary uuid length: " + value.length);
          }
          java.util.UUID uuid = new java.util.UUID(b.getLong(), b.getLong());
          return uuid.toString();
        }
      case 16:
        return value.length > 0 && value[0] != 0 ? "true" : "false";
      case 21:
        return String.valueOf(b.getShort());
      case 23:
        return String.valueOf(b.getInt());
      case 20:
        return String.valueOf(b.getLong());
      case 700:
        return String.valueOf(b.getFloat());
      case 701:
        return String.valueOf(b.getDouble());
      case 17:
        return "\\x" + hex(value);
      case 1082:
        return LocalDate.of(2000, 1, 1).plusDays(b.getInt()).toString();
      case 1083:
        return LocalTime.ofNanoOfDay(b.getLong() * 1000).toString();
      case 1114:
        return timestamp(LocalDateTime.of(2000, 1, 1, 0, 0).plusNanos(b.getLong() * 1000));
      case 1184:
        return timestamp(LocalDateTime.of(2000, 1, 1, 0, 0).plusNanos(b.getLong() * 1000))
            + "+00:00";
      case 1700:
        return numeric(b);
      case 0:
      case 19:
      case 25:
      case 1042:
      case 1043:
        return new String(value, StandardCharsets.UTF_8);
      case 1000: // bool[]
      case 1001: // bytea[]
      case 1005: // int2[]
      case 1007: // int4[]
      case 1016: // int8[]
      case 1021: // float4[]
      case 1022: // float8[]
      case 1009: // text[]
      case 1014: // bpchar[]
      case 1015: // varchar[]
      case 1182: // date[]
      case 1183: // time[]
      case 1115: // timestamp[]
      case 1185: // timestamptz[]
      case 1231: // numeric[]
        return arrayText(b);
      default:
        throw new IllegalArgumentException(
            "Binary parameters of type " + oid + " are not supported");
    }
  }

  /** A one-dimensional array in binary format: the mirror of {@link #arrayText(ByteBuffer)}. */
  private static byte[] arrayBinary(int elementOid, List<?> elements) {
    List<byte[]> encoded = new ArrayList<>();
    int size = 20;
    boolean nulls = false;
    for (Object e : elements) {
      if (e == null) {
        encoded.add(null);
        nulls = true;
        size += 4;
        continue;
      }
      Object v = e;
      if (v instanceof String && elementOid != 25 && elementOid != 1043 && elementOid != 1042) {
        v = Evaluator.cast(Catalog.typeName(elementOid), v); // elements parsed from array text
      }
      byte[] bytes = binary(elementOid, v);
      encoded.add(bytes);
      size += 4 + bytes.length;
    }
    ByteBuffer b = ByteBuffer.allocate(size);
    b.putInt(1).putInt(nulls ? 1 : 0).putInt(elementOid).putInt(elements.size()).putInt(1);
    for (byte[] bytes : encoded) {
      if (bytes == null) {
        b.putInt(-1);
      } else {
        b.putInt(bytes.length).put(bytes);
      }
    }
    return b.array();
  }

  /**
   * A binary array as PostgreSQL's array text: the dimensions, a null flag and the element type
   * come first, then each element's length and bytes (-1 for NULL). One dimension is supported.
   */
  private static String arrayText(ByteBuffer b) {
    int dims = b.getInt();
    b.getInt(); // has nulls
    int elementOid = b.getInt();
    if (dims > 1) {
      throw new IllegalArgumentException("Multidimensional arrays are not supported");
    }
    int count = dims == 0 ? 0 : b.getInt();
    if (dims == 1) {
      b.getInt(); // lower bound
    }
    StringBuilder sb = new StringBuilder("{");
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        sb.append(',');
      }
      int length = b.getInt();
      if (length < 0) {
        sb.append("NULL");
        continue;
      }
      byte[] bytes = new byte[length];
      b.get(bytes);
      String s = binary(elementOid, bytes);
      boolean quote =
          s.isEmpty()
              || s.equalsIgnoreCase("NULL")
              || s.chars()
                  .anyMatch(
                      c ->
                          c == '{'
                              || c == '}'
                              || c == ','
                              || c == '"'
                              || c == '\\'
                              || Character.isWhitespace(c));
      sb.append(quote ? "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"" : s);
    }
    return sb.append('}').toString();
  }

  /**
   * A binary numeric: base-10000 digit groups, the weight (power of 10000) of the first group, a
   * sign word and the display scale.
   */
  private static String numeric(ByteBuffer b) {
    int groups = b.getShort();
    int weight = b.getShort();
    int sign = b.getShort() & 0xFFFF;
    int scale = b.getShort();
    switch (sign) {
      case 0xC000:
        return "NaN";
      case 0xD000:
        return "Infinity";
      case 0xF000:
        return "-Infinity";
      default:
        break;
    }
    StringBuilder digits = new StringBuilder();
    for (int i = 0; i < groups; i++) {
      digits.append(String.format("%04d", b.getShort()));
    }
    java.math.BigDecimal value =
        groups == 0
            ? java.math.BigDecimal.ZERO
            : new java.math.BigDecimal(new java.math.BigInteger(digits.toString()))
                .scaleByPowerOfTen(4 * (weight - groups + 1));
    if (sign == 0x4000) {
      value = value.negate();
    }
    return value.setScale(scale, java.math.RoundingMode.HALF_UP).toPlainString();
  }

  /** A numeric in binary format: the mirror of {@link #numeric(ByteBuffer)}. */
  private static byte[] numeric(java.math.BigDecimal v) {
    if (v.scale() < 0) {
      v = v.setScale(0);
    }
    int scale = v.scale();
    int fractionGroups = (scale + 3) / 4;
    // Pad the fraction to whole groups, then split the digits into base-10000 groups
    java.math.BigInteger digits =
        v.unscaledValue().abs().multiply(java.math.BigInteger.TEN.pow(fractionGroups * 4 - scale));
    java.util.List<Integer> groups = new java.util.ArrayList<>(); // least significant first
    java.math.BigInteger base = java.math.BigInteger.valueOf(10000);
    while (digits.signum() > 0) {
      java.math.BigInteger[] qr = digits.divideAndRemainder(base);
      groups.add(qr[1].intValue());
      digits = qr[0];
    }
    int weight = groups.size() - fractionGroups - 1;
    java.util.Collections.reverse(groups);
    while (!groups.isEmpty() && groups.get(groups.size() - 1) == 0) {
      groups.remove(groups.size() - 1); // trailing zero groups are implied
    }
    ByteBuffer b = ByteBuffer.allocate(8 + 2 * groups.size());
    b.putShort((short) groups.size())
        .putShort((short) (groups.isEmpty() ? 0 : weight))
        .putShort((short) (v.signum() < 0 ? 0x4000 : 0))
        .putShort((short) scale);
    for (int g : groups) {
      b.putShort((short) g);
    }
    return b.array();
  }

  /** A value in PostgreSQL text format. */
  static String text(Object value) {
    if (value instanceof Boolean) {
      return (Boolean) value ? "t" : "f";
    }
    return Evaluator.text(value); // temporal and binary values in PostgreSQL's formats
  }

  private static String timestamp(LocalDateTime t) {
    return Evaluator.timestampText(t);
  }

  private static String hex(byte[] bytes) {
    return Evaluator.hex(bytes);
  }

  /** The PostgreSQL type OID for a column: from its ScalarDB type, else from its first value. */
  private static int oid(
      @Nullable DataType type, String column, @Nullable List<Map<String, Object>> rows) {
    if (type != null) {
      switch (type) {
        case BOOLEAN:
          return 16;
        case INT:
          return 23;
        case BIGINT:
          return 20;
        case FLOAT:
          return 700;
        case DOUBLE:
          return 701;
        case TEXT:
          return TEXT_OID;
        case BLOB:
          return 17;
        case DATE:
          return 1082;
        case TIME:
          return 1083;
        case TIMESTAMP:
          return 1114;
        case TIMESTAMPTZ:
          return 1184;
        default:
          return TEXT_OID;
      }
    }
    if (rows != null) {
      for (Map<String, Object> row : rows) {
        Object v = row.get(column);
        if (v instanceof Boolean) {
          return 16;
        }
        if (v instanceof java.math.BigDecimal) {
          return 1700;
        }
        if (v instanceof Interval) {
          return 1186;
        }
        if (v instanceof Json.Value) {
          return ((Json.Value) v).binary ? 3802 : 114;
        }
        if (v instanceof Catalog.Int2Vector) {
          return 22;
        }
        if (v instanceof List) {
          Object element = null;
          for (Object item : (List<?>) v) {
            if (item != null) {
              element = item;
              break;
            }
          }
          int elementOid =
              element == null
                  ? 25
                  : oid(null, "", Collections.singletonList(Collections.singletonMap("", element)));
          return (int) Catalog.arrayOid(elementOid);
        }
        if (v instanceof Integer || v instanceof Short) {
          return 23;
        }
        if (v instanceof Long) {
          return 20;
        }
        if (v instanceof Float) {
          return 700;
        }
        if (v instanceof Double) {
          return 701;
        }
        if (v != null) {
          return TEXT_OID;
        }
      }
    }
    return TEXT_OID;
  }

  private static int size(int oid) {
    switch (oid) {
      case 16:
        return 1;
      case 23:
      case 700:
      case 1082:
        return 4;
      case 20:
      case 701:
      case 1083:
      case 1114:
      case 1184:
        return 8;
      default:
        return -1;
    }
  }
}
