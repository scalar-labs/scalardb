package com.scalar.db.graphql.server;

import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.graphql.GraphQlFactory;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

class GraphQlHandler extends AbstractHandler {
  private static final Pattern CONTENT_TYPE_JSON =
      Pattern.compile(
          "^application/(?:graphql\\+)?json\\s*(?:;\\s*charset=(.+))?", Pattern.CASE_INSENSITIVE);
  private static final Pattern CONTENT_TYPE_GRAPHQL =
      Pattern.compile("^application/graphql\\s*(?:;\\s*charset=(.+))?", Pattern.CASE_INSENSITIVE);
  private static final Gson GSON =
      new GsonBuilder()
          // This is important because the graphql spec says that null values should be present
          .serializeNulls()
          .create();
  private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {}.getType();
  private final GraphQL graphql;
  private final String path;
  private final boolean isGraphiqlEnabled;
  private final byte[] graphiqlHtml;

  GraphQlHandler(
      String path, DatabaseConfig databaseConfig, List<String[]> tables, boolean isGraphiqlEnabled)
      throws IOException, ExecutionException {
    // ensure the path starts with /
    if (path.startsWith("/")) {
      this.path = path;
    } else {
      this.path = "/" + path;
    }
    this.isGraphiqlEnabled = isGraphiqlEnabled;

    GraphQlFactory.Builder builder =
        GraphQlFactory.newBuilder()
            .storageFactory(new StorageFactory(databaseConfig))
            .transactionFactory(new TransactionFactory(databaseConfig));
    for (String[] table : tables) {
      builder.table(table[0], table[1]);
    }
    this.graphql = builder.build().createGraphQL();

    try (InputStream is = getClass().getClassLoader().getResourceAsStream("graphiql.html")) {
      assert is != null;
      graphiqlHtml = ByteStreams.toByteArray(is);
    }
  }

  static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public void handle(
      String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    if (!target.equals(path)) {
      return;
    }
    switch (request.getMethod()) {
      case "POST":
        handlePost(request, response);
        break;
      case "GET":
        handleGet(request, response);
        break;
      default:
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        break;
    }
    baseRequest.setHandled(true);
  }

  private void handlePost(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    String contentType = request.getContentType();
    if (contentType == null) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      return;
    }
    Matcher matcher = CONTENT_TYPE_JSON.matcher(contentType);
    if (matcher.matches()) {
      Charset cs = getCharset(matcher.group(1));
      Reader reader = new InputStreamReader(request.getInputStream(), cs);
      ExecutionInput input = createExecutionInput(JsonParser.parseReader(reader).getAsJsonObject());
      execute(response, input);
      return;
    }
    matcher = CONTENT_TYPE_GRAPHQL.matcher(contentType);
    if (matcher.matches()) {
      Charset cs = getCharset(matcher.group(1));
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(request.getInputStream(), cs));
      String query = reader.lines().collect(Collectors.joining("\n"));
      ExecutionInput input = ExecutionInput.newExecutionInput(query).build();
      execute(response, input);
      return;
    }
    if (request.getParameter("query") != null) {
      ExecutionInput input =
          createExecutionInput(
              request.getParameter("query"),
              request.getParameter("operationName"),
              request.getParameter("variables"));
      execute(response, input);
      return;
    }
    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
  }

  private Charset getCharset(String charsetName) {
    if (charsetName == null) {
      return StandardCharsets.UTF_8;
    }
    try {
      return Charset.forName(charsetName);
    } catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
      return StandardCharsets.UTF_8;
    }
  }

  private void handleGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    if (request.getParameter("query") != null) {
      ExecutionInput input =
          createExecutionInput(
              request.getParameter("query"),
              request.getParameter("operationName"),
              request.getParameter("variables"));
      execute(response, input);
    } else if (isGraphiqlEnabled) {
      response.setContentType("text/html");
      response.setStatus(HttpServletResponse.SC_OK);
      response.getOutputStream().write(graphiqlHtml);
    } else {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }
  }

  private ExecutionInput createExecutionInput(JsonObject jsonObject) {
    ExecutionInput.Builder executionInput = ExecutionInput.newExecutionInput();
    JsonElement query = jsonObject.get("query");
    if (query != null && query.isJsonPrimitive()) {
      executionInput.query(query.getAsString());
    }
    JsonElement operationName = jsonObject.get("operationName");
    if (operationName != null && operationName.isJsonPrimitive()) {
      executionInput.operationName(operationName.getAsString());
    }
    JsonElement variables = jsonObject.get("variables");
    if (variables != null && variables.isJsonObject()) {
      executionInput.variables(GSON.fromJson(variables, MAP_TYPE));
    }
    return executionInput.build();
  }

  private ExecutionInput createExecutionInput(
      String query, String operationName, String variables) {
    ExecutionInput.Builder builder = ExecutionInput.newExecutionInput();
    if (query != null) {
      builder.query(query);
    }
    if (operationName != null) {
      builder.operationName(operationName);
    }
    Map<String, Object> variablesMap = GSON.fromJson(variables, MAP_TYPE);
    if (variablesMap != null) {
      builder.variables(variablesMap);
    }
    return builder.build();
  }

  private void execute(HttpServletResponse response, ExecutionInput executionInput)
      throws IOException {
    ExecutionResult result = graphql.execute(executionInput);
    response.setContentType("application/json");
    response.setStatus(HttpServletResponse.SC_OK);
    GSON.toJson(result.toSpecification(), response.getWriter());
  }

  public static final class Builder {
    private final List<String[]> tables = new ArrayList<>();
    private String path;
    private DatabaseConfig databaseConfig;
    private Boolean isGraphiqlEnabled;

    private Builder() {}

    public Builder path(String path) {
      this.path = path;
      return this;
    }

    public Builder databaseConfig(DatabaseConfig databaseConfig) {
      this.databaseConfig = databaseConfig;
      return this;
    }

    public Builder table(String namespace, String table) {
      tables.add(new String[] {namespace, table});
      return this;
    }

    public Builder setGraphiqlEnabled(boolean enabled) {
      this.isGraphiqlEnabled = enabled;
      return this;
    }

    public GraphQlHandler build() throws IOException, ExecutionException {
      if (path == null) {
        throw new IllegalStateException("Need to specify path");
      }
      if (databaseConfig == null) {
        throw new IllegalStateException("Need to specify databaseConfig");
      }
      if (tables.isEmpty()) {
        throw new IllegalStateException("Need to specify at least one table");
      }
      if (isGraphiqlEnabled == null) {
        throw new IllegalStateException("Need to specify if Graphiql is enabled");
      }
      return new GraphQlHandler(path, databaseConfig, tables, isGraphiqlEnabled);
    }
  }
}
