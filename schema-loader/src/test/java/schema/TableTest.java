package schema;

import static org.mockito.Mockito.when;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TableTest {

  Table table;

  public static final Map<String, String> TABLE_OPTIONS = new HashMap<String, String>() {{
    put("to1", "vto1");
    put("to2", "vto2");
  }};
  public static final Map<String, String> META_OPTIONS = new HashMap<String, String>() {{
    put("mo1", "vmo1");
    put("mo2", "vmo2");
  }};

  @Mock
  private JsonObject tableDefinition;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void buildOptions_GivenOptionsMapFromTableDefinitionAndMetaOptions_ShouldReturnMergedMap() {
    // Arrange
    Map<String, String> metaOptions = new HashMap<String, String>() {{
      put("mo1", "vmo1");
      put("mo2", "vmo2");
    }};
    Map<String, String> expectedOptions = new HashMap<>(metaOptions);
    expectedOptions.put("to1", "vto1");
    expectedOptions.put("to2", "vto2");

    table = new Table(new HashSet<String>() {{
      add("traveled1");
    }});

    when(tableDefinition.entrySet()).thenReturn(new HashMap<String, JsonElement>() {{
      put("traveled1", new JsonPrimitive("vtl1"));
      put("to1", new JsonPrimitive("vto1"));
      put("to2", new JsonPrimitive("vto2"));
    }}.entrySet());

    // Act
    Map<String, String> options = table.buildOptions(tableDefinition, metaOptions);

    // Assert
    Assertions.assertThat(options).isEqualTo(expectedOptions);
  }
}
