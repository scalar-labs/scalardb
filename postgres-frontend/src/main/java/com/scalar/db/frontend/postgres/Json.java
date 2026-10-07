package com.scalar.db.frontend.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * PostgreSQL's json and jsonb in memory. ScalarDB has no JSON column, so a json or jsonb column is
 * TEXT in storage; values exist as Jackson trees only while a statement runs: a cast, an operator
 * result, a function result. jsonb prints with sorted keys and {@code "k": v} separators, json with
 * its keys in order and {@code "k" : v} separators, or verbatim when it came from a literal.
 */
final class Json {
  private static final JsonNodeFactory NODES = JsonNodeFactory.withExactBigDecimals(true);
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .setNodeFactory(NODES);

  private Json() {}

  /** A JSON value with its type: jsonb (binary) or json. */
  static final class Value {
    final JsonNode node;
    final boolean binary;
    @Nullable final String verbatim; // a json literal prints as it was written

    Value(JsonNode node, boolean binary, @Nullable String verbatim) {
      this.node = node;
      this.binary = binary;
      this.verbatim = verbatim;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Value && same(node, ((Value) o).node);
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    @Override
    public String toString() {
      return binary ? text(node, true) : verbatim != null ? verbatim : text(node, false);
    }
  }

  static JsonNode parse(String text) {
    try {
      JsonNode node = MAPPER.readTree(text);
      if (node == null || node.isMissingNode()) {
        throw new IllegalArgumentException("invalid input syntax for type json");
      }
      return node;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "invalid input syntax for type json: " + e.getOriginalMessage());
    }
  }

  /** {@code 'text'::jsonb} or {@code 'text'::json}. */
  static Value cast(String text, boolean binary) {
    return new Value(parse(text), binary, binary ? null : text);
  }

  /**
   * An operand of a JSON operator: a JSON value, or the text of a json column (TEXT in storage,
   * read as jsonb).
   */
  @Nullable
  static Value of(@Nullable Object v) {
    if (v == null) {
      return null;
    }
    if (v instanceof Value) {
      return (Value) v;
    }
    if (v instanceof String) {
      return new Value(parse((String) v), true, null);
    }
    throw new IllegalArgumentException("Not a JSON value: " + v);
  }

  /** {@code to_json(v)}: a SQL value as a JSON tree. */
  static JsonNode toNode(@Nullable Object v) {
    if (v == null) {
      return NODES.nullNode();
    }
    if (v instanceof Value) {
      return ((Value) v).node;
    }
    if (v instanceof Boolean) {
      return NODES.booleanNode((Boolean) v);
    }
    if (v instanceof Integer || v instanceof Long) {
      return NODES.numberNode(((Number) v).longValue());
    }
    if (v instanceof BigDecimal) {
      return NODES.numberNode((BigDecimal) v);
    }
    if (v instanceof Number) {
      return NODES.numberNode(new BigDecimal(Evaluator.text(v)));
    }
    if (v instanceof List) {
      ArrayNode array = NODES.arrayNode();
      for (Object item : (List<?>) v) {
        array.add(toNode(item));
      }
      return array;
    }
    if (v instanceof LocalDateTime) {
      return NODES.textNode(((LocalDateTime) v).toString());
    }
    if (v instanceof Instant) {
      return NODES.textNode(
          LocalDateTime.ofInstant((Instant) v, ZoneOffset.UTC).toString() + "+00:00");
    }
    if (v instanceof LocalDate || v instanceof LocalTime || v instanceof ByteBuffer) {
      return NODES.textNode(Evaluator.text(v));
    }
    return NODES.textNode(Evaluator.text(v));
  }

  /** {@code j -> key}: a member by text key or an element by index (negative from the end). */
  @Nullable
  static JsonNode get(JsonNode node, @Nullable Object key) {
    if (key == null) {
      return null;
    }
    if (node.isObject()) {
      return key instanceof String ? node.get((String) key) : null;
    }
    if (node.isArray() && !(key instanceof String)) {
      int i = ((Number) key).intValue();
      if (i < 0) {
        i += node.size();
      }
      return i >= 0 && i < node.size() ? node.get(i) : null;
    }
    return null;
  }

  /** {@code j #> path}: members and elements in turn; array steps are taken from text. */
  @Nullable
  static JsonNode path(JsonNode node, List<?> path) {
    JsonNode cur = node;
    for (Object step : path) {
      if (cur == null || step == null) {
        return null;
      }
      String s = Evaluator.text(step);
      if (cur.isArray()) {
        try {
          cur = get(cur, Long.parseLong(s.trim()));
        } catch (NumberFormatException e) {
          return null;
        }
      } else {
        cur = get(cur, s);
      }
    }
    return cur;
  }

  /** {@code ->>}: a string without its quotes, a JSON null as SQL NULL, anything else as text. */
  @Nullable
  static String scalarText(@Nullable JsonNode node, boolean binary) {
    if (node == null || node.isNull()) {
      return null;
    }
    return node.isTextual() ? node.textValue() : text(node, binary);
  }

  /** jsonb containment, {@code a @> b}. */
  static boolean contains(JsonNode a, JsonNode b) {
    if (a.isObject() && b.isObject()) {
      for (Iterator<Map.Entry<String, JsonNode>> it = b.fields(); it.hasNext(); ) {
        Map.Entry<String, JsonNode> e = it.next();
        JsonNode mine = a.get(e.getKey());
        if (mine == null || !contains(mine, e.getValue())) {
          return false;
        }
      }
      return true;
    }
    if (a.isArray() && b.isArray()) {
      for (JsonNode theirs : b) {
        boolean found = false;
        for (JsonNode mine : a) {
          if (theirs.isContainerNode()
              ? mine.getNodeType() == theirs.getNodeType() && contains(mine, theirs)
              : same(mine, theirs)) {
            found = true;
            break;
          }
        }
        if (!found) {
          return false;
        }
      }
      return true;
    }
    if (a.isArray()) {
      // a scalar is contained in an array that has it at the top level
      for (JsonNode mine : a) {
        if (same(mine, b)) {
          return true;
        }
      }
      return false;
    }
    return same(a, b);
  }

  /** Equality as jsonb sees it: numbers by value, containers member by member. */
  static boolean same(JsonNode a, JsonNode b) {
    if (a.isNumber() && b.isNumber()) {
      return a.decimalValue().compareTo(b.decimalValue()) == 0;
    }
    if (a.isObject() && b.isObject()) {
      if (a.size() != b.size()) {
        return false;
      }
      for (Iterator<Map.Entry<String, JsonNode>> it = a.fields(); it.hasNext(); ) {
        Map.Entry<String, JsonNode> e = it.next();
        JsonNode theirs = b.get(e.getKey());
        if (theirs == null || !same(e.getValue(), theirs)) {
          return false;
        }
      }
      return true;
    }
    if (a.isArray() && b.isArray()) {
      if (a.size() != b.size()) {
        return false;
      }
      for (int i = 0; i < a.size(); i++) {
        if (!same(a.get(i), b.get(i))) {
          return false;
        }
      }
      return true;
    }
    return a.equals(b);
  }

  /** {@code j ? key}: an object key, an array's string element, or the string itself. */
  static boolean exists(JsonNode node, String key) {
    if (node.isObject()) {
      return node.has(key);
    }
    if (node.isArray()) {
      for (JsonNode item : node) {
        if (item.isTextual() && item.textValue().equals(key)) {
          return true;
        }
      }
      return false;
    }
    return node.isTextual() && node.textValue().equals(key);
  }

  /** {@code a || b} for jsonb: objects merge, arrays append, anything else joins as an array. */
  static JsonNode concat(JsonNode a, JsonNode b) {
    if (a.isObject() && b.isObject()) {
      ObjectNode out = ((ObjectNode) a).deepCopy();
      out.setAll((ObjectNode) b);
      return out;
    }
    ArrayNode out = NODES.arrayNode();
    if (a.isArray()) {
      out.addAll((ArrayNode) a);
    } else {
      out.add(a);
    }
    if (b.isArray()) {
      out.addAll((ArrayNode) b);
    } else {
      out.add(b);
    }
    return out;
  }

  /** {@code j - key}: without an object member or the equal string elements; {@code j - n}. */
  static JsonNode delete(JsonNode node, Object key) {
    if (node.isObject()) {
      if (!(key instanceof String)) {
        throw new IllegalArgumentException("cannot delete from object using integer index");
      }
      ObjectNode out = ((ObjectNode) node).deepCopy();
      out.remove((String) key);
      return out;
    }
    if (node.isArray()) {
      ArrayNode out = NODES.arrayNode();
      int index = key instanceof String ? Integer.MIN_VALUE : ((Number) key).intValue();
      if (index != Integer.MIN_VALUE && index < 0) {
        index += node.size();
      }
      for (int i = 0; i < node.size(); i++) {
        JsonNode item = node.get(i);
        boolean drop =
            key instanceof String ? item.isTextual() && item.textValue().equals(key) : i == index;
        if (!drop) {
          out.add(item);
        }
      }
      return out;
    }
    throw new IllegalArgumentException("cannot delete from scalar");
  }

  static String typeOf(JsonNode node) {
    if (node.isObject()) {
      return "object";
    }
    if (node.isArray()) {
      return "array";
    }
    if (node.isTextual()) {
      return "string";
    }
    if (node.isNumber()) {
      return "number";
    }
    if (node.isBoolean()) {
      return "boolean";
    }
    return "null";
  }

  static long arrayLength(JsonNode node) {
    if (!node.isArray()) {
      throw new IllegalArgumentException(
          "cannot get array length of a " + (node.isObject() ? "non-array" : "scalar"));
    }
    return node.size();
  }

  /** {@code json_build_object(k1, v1, ...)}: keys as text, in the order given. */
  static ObjectNode buildObject(List<Object> args) {
    if (args.size() % 2 != 0) {
      throw new IllegalArgumentException(
          "argument list must have even number of elements (key, value pairs)");
    }
    ObjectNode out = NODES.objectNode();
    for (int i = 0; i < args.size(); i += 2) {
      if (args.get(i) == null) {
        throw new IllegalArgumentException("argument " + (i + 1) + ": key must not be null");
      }
      out.set(Evaluator.text(args.get(i)), toNode(args.get(i + 1)));
    }
    return out;
  }

  static ArrayNode buildArray(List<?> items) {
    ArrayNode out = NODES.arrayNode();
    for (Object item : items) {
      out.add(toNode(item));
    }
    return out;
  }

  /** The elements of an array, as values of the given type. */
  static List<Object> elements(JsonNode node, boolean binary, boolean asText) {
    if (!node.isArray()) {
      throw new IllegalArgumentException(
          "cannot extract elements from " + (node.isObject() ? "an object" : "a scalar"));
    }
    List<Object> out = new ArrayList<>();
    for (JsonNode item : node) {
      out.add(asText ? scalarText(item, binary) : new Value(item, binary, null));
    }
    return out;
  }

  /** The text PostgreSQL prints: jsonb with sorted keys and {@code ": "}, json in order. */
  static String text(JsonNode node, boolean binary) {
    StringBuilder sb = new StringBuilder();
    write(node, binary, sb);
    return sb.toString();
  }

  private static void write(JsonNode node, boolean binary, StringBuilder sb) {
    if (node.isObject()) {
      sb.append('{');
      Iterable<Map.Entry<String, JsonNode>> entries = sortedIfBinary(node, binary);
      boolean first = true;
      for (Map.Entry<String, JsonNode> e : entries) {
        if (!first) {
          sb.append(", ");
        }
        first = false;
        sb.append('"').append(JsonStringEncoder.getInstance().quoteAsString(e.getKey()));
        sb.append(binary ? "\": " : "\" : ");
        write(e.getValue(), binary, sb);
      }
      sb.append('}');
    } else if (node.isArray()) {
      sb.append('[');
      for (int i = 0; i < node.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        write(node.get(i), binary, sb);
      }
      sb.append(']');
    } else if (node.isTextual()) {
      sb.append('"')
          .append(JsonStringEncoder.getInstance().quoteAsString(node.textValue()))
          .append('"');
    } else if (node.isNumber()) {
      sb.append(node.decimalValue().toPlainString());
    } else {
      sb.append(node.asText()); // true, false, null
    }
  }

  /** jsonb orders keys by length, then bytewise; json keeps the order they were written in. */
  private static Iterable<Map.Entry<String, JsonNode>> sortedIfBinary(
      JsonNode object, boolean binary) {
    if (!binary) {
      List<Map.Entry<String, JsonNode>> out = new ArrayList<>();
      object.fields().forEachRemaining(out::add);
      return out;
    }
    TreeMap<String, JsonNode> sorted =
        new TreeMap<>(
            (x, y) -> {
              byte[] a = x.getBytes(StandardCharsets.UTF_8);
              byte[] b = y.getBytes(StandardCharsets.UTF_8);
              if (a.length != b.length) {
                return Integer.compare(a.length, b.length);
              }
              for (int i = 0; i < a.length; i++) {
                int c = Integer.compare(a[i] & 0xff, b[i] & 0xff);
                if (c != 0) {
                  return c;
                }
              }
              return 0;
            });
    object.fields().forEachRemaining(e -> sorted.put(e.getKey(), e.getValue()));
    return sorted.entrySet();
  }
}
