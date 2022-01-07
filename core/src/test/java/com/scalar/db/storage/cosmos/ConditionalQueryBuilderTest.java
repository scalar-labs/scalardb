package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.io.IntValue;
import org.jooq.Condition;
import org.jooq.SQLDialect;
import org.jooq.SelectConditionStep;
import org.jooq.impl.DSL;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ConditionalQueryBuilderTest {
  private static final String ANY_ID = "any_id";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final int ANY_INT = 1;
  private static final IntValue ANY_INT_VALUE = new IntValue("any_int", ANY_INT);

  @Mock private SelectConditionStep<org.jooq.Record> select;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void getQuery_NoConditionsGiven_ShouldReturnQuery() {
    // Arrange
    SelectConditionStep<org.jooq.Record> testSelect =
        DSL.using(SQLDialect.DEFAULT).selectFrom("Record r").where(DSL.field("r.id").eq(ANY_ID));

    ConditionalQueryBuilder builder = new ConditionalQueryBuilder(testSelect);

    // Act
    String actual = builder.getQuery();

    // Assert
    assertThat(actual).isEqualTo("select * from Record r where r.id = '" + ANY_ID + "'");
  }

  @Test
  public void getQuery_PutIfAcceptCalled_ShouldReturnQuery() {
    // Arrange
    SelectConditionStep<org.jooq.Record> testSelect =
        DSL.using(SQLDialect.DEFAULT).selectFrom("Record r").where(DSL.field("r.id").eq(ANY_ID));
    PutIf condition =
        new PutIf(
            new ConditionalExpression(ANY_NAME_1, ANY_INT_VALUE, Operator.EQ),
            new ConditionalExpression(ANY_NAME_2, ANY_INT_VALUE, Operator.GT));
    ConditionalQueryBuilder builder = new ConditionalQueryBuilder(testSelect);

    // Act
    condition.accept(builder);
    String actual = builder.getQuery();

    // Assert
    assertThat(actual)
        .isEqualTo(
            "select * from Record r where (r.id = '"
                + ANY_ID
                + "' and r.values[\""
                + ANY_NAME_1
                + "\"]"
                + " = "
                + ANY_INT
                + " and r.values[\""
                + ANY_NAME_2
                + "\"]"
                + " > "
                + ANY_INT
                + ")");
  }

  @Test
  public void visit_PutIfAcceptCalled_ShouldCallWhere() {
    // Arrange
    PutIf condition =
        new PutIf(
            new ConditionalExpression(ANY_NAME_1, ANY_INT_VALUE, Operator.EQ),
            new ConditionalExpression(ANY_NAME_2, ANY_INT_VALUE, Operator.GT));
    ConditionalQueryBuilder builder = new ConditionalQueryBuilder(select);

    // Act
    condition.accept(builder);

    // Assert
    verify(select).and(DSL.field("r.values[\"" + ANY_NAME_1 + "\"]").equal(ANY_INT));
    verify(select).and(DSL.field("r.values[\"" + ANY_NAME_2 + "\"]").greaterThan(ANY_INT));
  }

  @Test
  public void visit_PutIfExistsAcceptCalled_ShouldNotCallWhere() {
    // Arrange
    PutIfExists condition = new PutIfExists();
    ConditionalQueryBuilder builder = new ConditionalQueryBuilder(select);

    // Act
    condition.accept(builder);

    // Assert
    verify(select, never()).and(any(Condition.class));
  }

  @Test
  public void visit_PutIfNotExistsAcceptCalled_ShouldNotCallWhere() {
    // Arrange
    PutIfNotExists condition = new PutIfNotExists();
    ConditionalQueryBuilder builder = new ConditionalQueryBuilder(select);

    // Act
    condition.accept(builder);

    // Assert
    verify(select, never()).and(any(Condition.class));
  }

  @Test
  public void visit_DeleteIfAcceptCalled_ShouldCallWhere() {
    // Arrange
    DeleteIf condition =
        new DeleteIf(
            new ConditionalExpression(ANY_NAME_1, ANY_INT_VALUE, Operator.EQ),
            new ConditionalExpression(ANY_NAME_2, ANY_INT_VALUE, Operator.GT));
    ConditionalQueryBuilder builder = new ConditionalQueryBuilder(select);

    // Act
    condition.accept(builder);

    // Assert
    verify(select).and(DSL.field("r.values[\"" + ANY_NAME_1 + "\"]").equal(ANY_INT));
    verify(select).and(DSL.field("r.values[\"" + ANY_NAME_2 + "\"]").greaterThan(ANY_INT));
  }
}
