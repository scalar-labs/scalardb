package com.scalar.db.frontend.postgres;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import javax.annotation.Nullable;

/**
 * Virtual {@code pg_catalog} and {@code information_schema} tables built from ScalarDB metadata,
 * enough for hand-written catalog queries and for psql's {@code \l}, {@code \dn}, {@code \dt},
 * {@code \d}, {@code \d table}, and {@code \di}. Namespaces appear as both schemas and databases;
 * every table gets a {@code <table>_pkey} primary-key index and a {@code <table>_<column>_idx}
 * index per secondary index. Rows are rebuilt lazily after each {@link #invalidate()}.
 */
final class Catalog {
  static final String OWNER = "scalardb";
  private static final long OWNER_OID = 10;
  private static final long HEAP_AM = 2;
  private static final long BTREE_AM = 403;

  private static final Map<String, List<String>> PG_CATALOG = new LinkedHashMap<>();
  private static final Map<String, List<String>> INFORMATION_SCHEMA = new LinkedHashMap<>();
  private static final Map<Long, String> TYPE_NAMES = new LinkedHashMap<>();
  private static final Map<Long, String> INTERNAL_TYPE_NAMES = new LinkedHashMap<>();

  static {
    PG_CATALOG.put(
        "pg_database",
        cols(
            "oid",
            "datname",
            "datdba",
            "encoding",
            "datlocprovider",
            "datcollate",
            "datctype",
            "datlocale",
            "daticulocale",
            "daticurules",
            "datacl"));
    PG_CATALOG.put("pg_namespace", cols("oid", "nspname", "nspowner"));
    PG_CATALOG.put(
        "pg_class",
        cols(
            "oid",
            "relname",
            "relnamespace",
            "relkind",
            "relowner",
            "relam",
            "reltablespace",
            "relhasindex",
            "relhasrules",
            "relhastriggers",
            "relrowsecurity",
            "relforcerowsecurity",
            "relispartition",
            "reloftype",
            "relpersistence",
            "relreplident",
            "reltoastrelid",
            "relchecks",
            "reloptions",
            "relpartbound",
            "reltuples"));
    PG_CATALOG.put("pg_am", cols("oid", "amname"));
    PG_CATALOG.put(
        "pg_attribute",
        cols(
            "attrelid",
            "attnum",
            "attname",
            "atttypid",
            "atttypmod",
            "attnotnull",
            "atthasdef",
            "attisdropped",
            "attcollation",
            "attidentity",
            "attgenerated",
            "attstorage",
            "attcompression",
            "attstattarget"));
    PG_CATALOG.put(
        "pg_type",
        cols(
            "oid",
            "typname",
            "typnamespace",
            "typcollation",
            "typelem",
            "typdelim",
            "typinput",
            "typtype",
            "typbasetype",
            "typarray",
            "typlen",
            "typcategory",
            "typrelid",
            "typnotnull",
            "typtypmod",
            "typowner"));
    PG_CATALOG.put(
        "pg_range",
        cols(
            "rngtypid",
            "rngsubtype",
            "rngmultitypid",
            "rngcollation",
            "rngsubopc",
            "rngcanonical",
            "rngsubdiff"));
    PG_CATALOG.put("pg_enum", cols("oid", "enumtypid", "enumsortorder", "enumlabel"));
    PG_CATALOG.put("pg_statio_all_tables", cols("relid", "schemaname", "relname"));
    INFORMATION_SCHEMA.put(
        "table_constraints",
        cols(
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "table_catalog",
            "table_schema",
            "table_name",
            "constraint_type"));
    INFORMATION_SCHEMA.put(
        "key_column_usage",
        cols(
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position"));
    PG_CATALOG.put(
        "pg_index",
        cols(
            "indexrelid",
            "indrelid",
            "indisprimary",
            "indisunique",
            "indisclustered",
            "indisvalid",
            "indisreplident",
            "indkey"));
    PG_CATALOG.put(
        "pg_constraint",
        cols(
            "oid",
            "conname",
            "contype",
            "conrelid",
            "conindid",
            "condeferrable",
            "condeferred",
            "conperiod",
            "conparentid",
            "confrelid",
            "conkey",
            "connoinherit",
            "conislocal",
            "coninhcount",
            "convalidated"));
    PG_CATALOG.put("pg_attrdef", cols("adrelid", "adnum", "adbin"));
    PG_CATALOG.put("pg_collation", cols("oid", "collname"));
    PG_CATALOG.put(
        "pg_policy",
        cols(
            "oid",
            "polname",
            "polpermissive",
            "polroles",
            "polqual",
            "polwithcheck",
            "polcmd",
            "polrelid"));
    PG_CATALOG.put(
        "pg_statistic_ext",
        cols("oid", "stxrelid", "stxnamespace", "stxname", "stxkind", "stxstattarget"));
    PG_CATALOG.put("pg_inherits", cols("inhrelid", "inhparent", "inhseqno", "inhdetachpending"));
    PG_CATALOG.put("pg_roles", cols("oid", "rolname"));
    PG_CATALOG.put("pg_publication", cols("oid", "pubname", "puballtables"));
    PG_CATALOG.put("pg_publication_rel", cols("prpubid", "prrelid", "prqual", "prattrs"));
    PG_CATALOG.put("pg_publication_namespace", cols("pnpubid", "pnnspid"));
    PG_CATALOG.put("pg_rewrite", cols("oid", "rulename", "ev_class", "ev_enabled"));
    PG_CATALOG.put("pg_trigger", cols("oid", "tgname", "tgrelid", "tgenabled", "tgisinternal"));
    PG_CATALOG.put("pg_description", cols("objoid", "classoid", "objsubid", "description"));
    PG_CATALOG.put("pg_tablespace", cols("oid", "spcname"));
    PG_CATALOG.put(
        "pg_indexes", cols("schemaname", "tablename", "indexname", "tablespace", "indexdef"));
    INFORMATION_SCHEMA.put("schemata", cols("catalog_name", "schema_name"));
    INFORMATION_SCHEMA.put(
        "tables", cols("table_catalog", "table_schema", "table_name", "table_type"));
    INFORMATION_SCHEMA.put(
        "columns",
        cols(
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position",
            "data_type",
            "is_nullable",
            "udt_name",
            "character_maximum_length",
            "column_default"));
    type(16, "bool", "boolean");
    type(17, "bytea", "bytea");
    type(20, "int8", "bigint");
    type(23, "int4", "integer");
    type(25, "text", "text");
    type(700, "float4", "real");
    type(701, "float8", "double precision");
    type(1082, "date", "date");
    type(1083, "time", "time without time zone");
    type(1114, "timestamp", "timestamp without time zone");
    type(1184, "timestamptz", "timestamp with time zone");
    type(1186, "interval", "interval");
    type(1700, "numeric", "numeric");
    type(21, "int2", "smallint");
    type(1043, "varchar", "character varying");
    type(114, "json", "json");
    type(3802, "jsonb", "jsonb");
  }

  /** PostgreSQL's int2vector, as pg_index.indkey: a list whose text is space-separated. */
  static final class Int2Vector extends ArrayList<Object> {
    Int2Vector(List<Object> values) {
      super(values);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (Object v : this) {
        sb.append(sb.length() > 0 ? " " : "").append(v);
      }
      return sb.toString();
    }
  }

  /** The type OID of a catalog column that is not inferred from its value: name, int2vector. */
  static long catalogColumnOid(String column) {
    if (column.equals("indkey")) {
      return 22;
    }
    return column.endsWith("name") ? 19 : 0;
  }

  private static final Map<Long, Long> ARRAY_OIDS = new LinkedHashMap<>();

  static {
    long[][] pairs = {
      {16, 1000},
      {17, 1001},
      {19, 1003},
      {20, 1016},
      {21, 1005},
      {23, 1007},
      {25, 1009},
      {700, 1021},
      {701, 1022},
      {1043, 1015},
      {1082, 1182},
      {1083, 1183},
      {1114, 1115},
      {1184, 1185},
      {1186, 1187},
      {1700, 1231},
      {114, 199},
      {3802, 3807}
    };
    for (long[] p : pairs) {
      ARRAY_OIDS.put(p[0], p[1]);
    }
  }

  /** The array type's OID for an element type; text[] when the element type is unknown. */
  static long arrayOid(long element) {
    return ARRAY_OIDS.getOrDefault(element, 1009L);
  }

  /** The element type of an array type's OID, or 0. */
  static long elementOid(long array) {
    for (Map.Entry<Long, Long> e : ARRAY_OIDS.entrySet()) {
      if (e.getValue() == array) {
        return e.getKey();
      }
    }
    return 0;
  }

  private static long typeLength(long oid) {
    switch ((int) oid) {
      case 16:
        return 1;
      case 21:
        return 2;
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
      case 1186:
        return 16;
      default:
        return -1;
    }
  }

  private static String typeCategory(long oid) {
    switch ((int) oid) {
      case 16:
        return "B";
      case 17:
        return "U";
      case 25:
      case 1043:
        return "S";
      case 1082:
      case 1083:
      case 1114:
      case 1184:
        return "D";
      case 1186:
        return "T";
      default:
        return "N";
    }
  }

  private static List<String> cols(String... names) {
    return Collections.unmodifiableList(Arrays.asList(names));
  }

  private static void type(long oid, String internalName, String name) {
    INTERNAL_TYPE_NAMES.put(oid, internalName);
    TYPE_NAMES.put(oid, name);
  }

  private final DistributedTransactionAdmin admin;
  final String database;
  @Nullable private Map<String, List<Map<String, Object>>> tables;
  private final Map<Long, String> indexDefs = new HashMap<>();
  private final Map<Long, String> constraintDefs = new HashMap<>();
  private final Map<String, Long> relationOids = new HashMap<>(); // namespace.name -> oid
  private final Map<String, Long> namespaceOids = new HashMap<>();

  Catalog(DistributedTransactionAdmin admin, String database) {
    this.admin = admin;
    this.database = database;
  }

  /** The columns of a catalog table, or null if {@code schema.name} is not one. */
  @Nullable
  static List<String> columns(@Nullable String schema, String name) {
    if (schema == null || schema.equals("pg_catalog")) {
      List<String> columns = PG_CATALOG.get(name);
      if (columns != null) {
        return columns;
      }
    }
    return "information_schema".equals(schema) ? INFORMATION_SCHEMA.get(name) : null;
  }

  static String qualifiedName(@Nullable String schema, String name) {
    return ("information_schema".equals(schema) ? "information_schema." : "pg_catalog.") + name;
  }

  /** The PostgreSQL type OID of a ScalarDB type. */
  static long typeOid(DataType type) {
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
        return 25;
    }
  }

  /** The SQL type name for a type OID, as {@code format_type} returns it. */
  static String typeName(long oid) {
    String name = TYPE_NAMES.get(oid);
    return name == null ? "unknown" : name;
  }

  /** Forgets the loaded rows so the next query sees the current ScalarDB metadata. */
  void invalidate() {
    tables = null;
    relationOids.clear();
    namespaceOids.clear();
  }

  /** The schema name a namespace is shown under: the connected one is {@code public}. */
  String schemaName(String namespace) {
    return namespace.equals(database) ? "public" : namespace;
  }

  /** The OID of a table or index named as in a {@code regclass} cast: {@code [schema.]name}. */
  long relationOid(String name) {
    try {
      load();
    } catch (ExecutionException e) {
      throw new IllegalStateException(e);
    }
    String n = name.trim().replace("\"", "");
    Long oid = n.contains(".") ? relationOids.get(n) : relationOids.get(database + "." + n);
    if (oid == null && !n.contains(".")) {
      for (Map.Entry<String, Long> e : relationOids.entrySet()) {
        if (e.getKey().endsWith("." + n)) {
          oid = e.getValue();
          break;
        }
      }
    }
    if (oid == null) {
      throw new IllegalArgumentException("relation \"" + n + "\" does not exist");
    }
    return oid;
  }

  /** The OID of a namespace named as in a {@code regnamespace} cast. */
  long namespaceOid(String name) {
    try {
      load();
    } catch (ExecutionException e) {
      throw new IllegalStateException(e);
    }
    Long oid = namespaceOids.get(name.trim().replace("\"", ""));
    if (oid == null) {
      throw new IllegalArgumentException("schema \"" + name + "\" does not exist");
    }
    return oid;
  }

  /** The OID of a type named as in a {@code regtype} cast, by internal or SQL name. */
  static long typeOid(String name) {
    String n = name.trim().toLowerCase(Locale.ROOT);
    n = n.startsWith("pg_catalog.") ? n.substring("pg_catalog.".length()) : n;
    for (Map.Entry<Long, String> e : INTERNAL_TYPE_NAMES.entrySet()) {
      if (e.getValue().equals(n) || TYPE_NAMES.get(e.getKey()).equals(n)) {
        return e.getKey();
      }
    }
    throw new IllegalArgumentException("type \"" + name + "\" does not exist");
  }

  @Nullable
  String indexDef(long oid) throws ExecutionException {
    load();
    return indexDefs.get(oid);
  }

  @Nullable
  String constraintDef(long oid) throws ExecutionException {
    load();
    return constraintDefs.get(oid);
  }

  /** The rows of a catalog table, with keys qualified as {@code qualifier.column}. */
  List<Map<String, Object>> rows(@Nullable String schema, String name, String qualifier)
      throws ExecutionException {
    load();
    List<Map<String, Object>> out = new ArrayList<>();
    for (Map<String, Object> row :
        tables.getOrDefault(qualifiedName(schema, name), Collections.emptyList())) {
      Map<String, Object> qualified = new LinkedHashMap<>();
      for (Map.Entry<String, Object> e : row.entrySet()) {
        qualified.put(qualifier + "." + e.getKey(), e.getValue());
      }
      out.add(qualified);
    }
    return out;
  }

  private void load() throws ExecutionException {
    if (tables != null) {
      return;
    }
    Map<String, List<Map<String, Object>>> t = new HashMap<>();
    for (String name : PG_CATALOG.keySet()) {
      t.put("pg_catalog." + name, new ArrayList<>());
    }
    for (String name : INFORMATION_SCHEMA.keySet()) {
      t.put("information_schema." + name, new ArrayList<>());
    }
    row(t, "pg_catalog.pg_am", HEAP_AM, "heap");
    row(t, "pg_catalog.pg_am", BTREE_AM, "btree");
    row(t, "pg_catalog.pg_roles", OWNER_OID, OWNER);
    for (Map.Entry<Long, String> type : INTERNAL_TYPE_NAMES.entrySet()) {
      long oid = type.getKey();
      String name = type.getValue();
      long arrayOid = ARRAY_OIDS.getOrDefault(oid, 0L);
      row(
          t,
          "pg_catalog.pg_type",
          oid,
          name,
          11L,
          oid == 25 || oid == 1043 ? 100L : 0L,
          0L,
          ",",
          name + "in",
          "b",
          0L,
          arrayOid,
          typeLength(oid),
          typeCategory(oid),
          0L,
          false,
          -1L,
          OWNER_OID);
      if (arrayOid != 0) {
        // the array type of each base type, which drivers look up by typelem
        row(
            t,
            "pg_catalog.pg_type",
            arrayOid,
            "_" + name,
            11L,
            0L,
            oid,
            ",",
            "array_in",
            "b",
            0L,
            0L,
            -1L,
            "A",
            0L,
            false,
            -1L,
            OWNER_OID);
      }
    }
    long oid = 16384;
    for (String namespace : new TreeSet<>(admin.getNamespaceNames())) {
      if (namespace.equals("scalardb") || namespace.equals("coordinator")) {
        continue; // ScalarDB's own namespaces
      }
      long namespaceOid = oid++;
      String schema = schemaName(namespace); // the connected namespace is PostgreSQL's public
      namespaceOids.put(namespace, namespaceOid);
      namespaceOids.put(schema, namespaceOid);
      row(t, "pg_catalog.pg_namespace", namespaceOid, schema, OWNER_OID);
      row(
          t,
          "pg_catalog.pg_database",
          namespaceOid,
          namespace,
          OWNER_OID,
          6L,
          "c",
          "C",
          "C",
          null,
          null,
          null,
          null);
      row(t, "information_schema.schemata", database, schema);
      for (String table : new TreeSet<>(admin.getNamespaceTableNames(namespace))) {
        TableMetadata metadata = admin.getTableMetadata(namespace, table);
        if (metadata == null) {
          continue;
        }
        long relOid = oid++;
        relation(t, relOid, table, namespaceOid, "r", HEAP_AM, true);
        relationOids.put(namespace + "." + table, relOid);
        relationOids.put(schema + "." + table, relOid);
        row(t, "pg_catalog.pg_statio_all_tables", relOid, schema, table);
        row(t, "information_schema.tables", database, schema, table, "BASE TABLE");
        List<String> keys = new ArrayList<>(metadata.getPartitionKeyNames());
        keys.addAll(metadata.getClusteringKeyNames());
        List<Object> keyNumbers = new ArrayList<>();
        int attnum = 0;
        for (String column : metadata.getColumnNames()) {
          attnum++;
          DataType type = metadata.getColumnDataType(column);
          boolean key = keys.contains(column);
          if (key) {
            keyNumbers.add((long) attnum);
          }
          row(
              t,
              "pg_catalog.pg_attribute",
              relOid,
              (long) attnum,
              column,
              typeOid(type),
              -1L,
              key,
              false,
              false,
              0L,
              "",
              "",
              "p",
              "",
              -1L);
          row(
              t,
              "information_schema.columns",
              database,
              schema,
              table,
              column,
              (long) attnum,
              typeName(typeOid(type)),
              key ? "NO" : "YES",
              INTERNAL_TYPE_NAMES.get(typeOid(type)),
              null,
              null);
        }
        String columnList = String.join(", ", keys);
        row(
            t,
            "information_schema.table_constraints",
            database,
            schema,
            table + "_pkey",
            database,
            schema,
            table,
            "PRIMARY KEY");
        for (int k = 0; k < keys.size(); k++) {
          row(
              t,
              "information_schema.key_column_usage",
              database,
              schema,
              table + "_pkey",
              database,
              schema,
              table,
              keys.get(k),
              (long) (k + 1));
        }
        long pkeyOid = oid++;
        relation(t, pkeyOid, table + "_pkey", namespaceOid, "i", BTREE_AM, false);
        row(
            t,
            "pg_catalog.pg_index",
            pkeyOid,
            relOid,
            true,
            true,
            false,
            true,
            false,
            new Int2Vector(keyNumbers));
        String pkeyDef =
            "CREATE UNIQUE INDEX "
                + table
                + "_pkey ON "
                + schema
                + "."
                + table
                + " USING btree ("
                + columnList
                + ")";
        indexDefs.put(pkeyOid, pkeyDef);
        relationOids.put(namespace + "." + table + "_pkey", pkeyOid);
        relationOids.put(schema + "." + table + "_pkey", pkeyOid);
        row(t, "pg_catalog.pg_indexes", schema, table, table + "_pkey", null, pkeyDef);
        long constraintOid = oid++;
        row(
            t,
            "pg_catalog.pg_constraint",
            constraintOid,
            table + "_pkey",
            "p",
            relOid,
            pkeyOid,
            false,
            false,
            false,
            0L,
            0L,
            keyNumbers,
            false,
            true,
            0L,
            true);
        constraintDefs.put(constraintOid, "PRIMARY KEY (" + columnList + ")");
        for (String index : new TreeSet<>(metadata.getSecondaryIndexNames())) {
          long indexOid = oid++;
          String indexName = table + "_" + index + "_idx";
          relation(t, indexOid, indexName, namespaceOid, "i", BTREE_AM, false);
          int indexPosition = new ArrayList<>(metadata.getColumnNames()).indexOf(index) + 1;
          long indexAttnum = indexPosition;
          row(
              t,
              "pg_catalog.pg_index",
              indexOid,
              relOid,
              false,
              false,
              false,
              true,
              false,
              new Int2Vector(Collections.singletonList((Object) indexAttnum)));
          String indexDef =
              "CREATE INDEX "
                  + indexName
                  + " ON "
                  + schema
                  + "."
                  + table
                  + " USING btree ("
                  + index
                  + ")";
          indexDefs.put(indexOid, indexDef);
          relationOids.put(namespace + "." + indexName, indexOid);
          relationOids.put(schema + "." + indexName, indexOid);
          row(t, "pg_catalog.pg_indexes", schema, table, indexName, null, indexDef);
        }
      }
    }
    tables = t;
  }

  private static void relation(
      Map<String, List<Map<String, Object>>> t,
      long oid,
      String name,
      long namespaceOid,
      String kind,
      long am,
      boolean hasIndex) {
    row(
        t,
        "pg_catalog.pg_class",
        oid,
        name,
        namespaceOid,
        kind,
        OWNER_OID,
        am,
        0L,
        hasIndex,
        false,
        false,
        false,
        false,
        false,
        0L,
        "p",
        "d",
        0L,
        0L,
        null,
        null,
        -1.0);
  }

  private static void row(
      Map<String, List<Map<String, Object>>> t, String table, Object... values) {
    List<String> columns =
        table.startsWith("pg_catalog.")
            ? PG_CATALOG.get(table.substring("pg_catalog.".length()))
            : INFORMATION_SCHEMA.get(table.substring("information_schema.".length()));
    if (columns.size() != values.length) {
      throw new AssertionError(table + " expects " + columns.size() + " values");
    }
    Map<String, Object> row = new LinkedHashMap<>();
    for (int i = 0; i < values.length; i++) {
      row.put(columns.get(i), values[i]);
    }
    t.get(table).add(row);
  }
}
