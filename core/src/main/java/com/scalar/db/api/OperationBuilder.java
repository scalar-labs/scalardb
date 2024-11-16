package com.scalar.db.api;

import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * This class defines common interfaces used by {@link DeleteBuilder}, {@link PutBuilder}, {@link
 * InsertBuilder}, {@link UpsertBuilder}, {@link UpdateBuilder}, {@link GetBuilder} and {@link
 * ScanBuilder}
 */
public class OperationBuilder {
  public interface Namespace<T> {
    /**
     * Sets the specified target namespace for this operation
     *
     * @param namespaceName target namespace for this operation
     * @return the operation builder
     */
    T namespace(String namespaceName);
  }

  public interface ClearNamespace<T> {
    /**
     * Removes the namespace
     *
     * @return the operation builder
     */
    T clearNamespace();
  }

  public interface Table<T> {
    /**
     * Sets the specified target table for this operation
     *
     * @param tableName target table name for this operation
     * @return the operation builder
     */
    T table(String tableName);
  }

  public interface PartitionKey<T> {
    /**
     * Constructs the operation with the specified partition {@link Key}.
     *
     * @param partitionKey a partition {@code Key} (it might be composed of multiple values)
     * @return the operation builder
     */
    T partitionKey(Key partitionKey);
  }

  public interface ClusteringKey<T> {
    /**
     * Constructs the operation with the specified clustering {@link Key}.
     *
     * @param clusteringKey a clustering {@code Key} (it might be composed of multiple values)
     * @return the operation builder
     */
    T clusteringKey(Key clusteringKey);
  }

  public interface ClearClusteringKey<T> {
    /**
     * Removes the clustering key
     *
     * @return the operation builder
     */
    T clearClusteringKey();
  }

  public interface Consistency<E> {
    /**
     * Sets the specified consistency level for this operation
     *
     * @param consistency consistency level to set
     * @return the operation builder
     */
    E consistency(com.scalar.db.api.Consistency consistency);
  }

  public interface Projection<T> {
    /**
     * Appends the specified column name to the list of projections.
     *
     * @param projection a column name to project
     * @return the operation builder
     */
    T projection(String projection);

    /**
     * Appends the specified collection of the specified column names to the list of projections.
     *
     * @param projections a collection of the column names to project
     * @return the operation builder
     */
    T projections(Collection<String> projections);

    /**
     * Appends the specified collection of the specified column names to the list of projections.
     *
     * @param projections the column names to project
     * @return the operation builder
     */
    T projections(String... projections);
  }

  public interface ClearProjections<T> {
    /**
     * Clears the list of projections.
     *
     * @return the operation builder
     */
    T clearProjections();
  }

  public interface Condition<T> {
    /**
     * Sets the specified {@link MutationCondition}
     *
     * @param condition a {@code MutationCondition}
     * @return the operation builder
     */
    T condition(MutationCondition condition);
  }

  public interface ClearCondition<T> {
    /**
     * Removes the condition.
     *
     * @return the operation builder
     */
    T clearCondition();
  }

  public interface Values<T> {
    /**
     * Adds the specified BOOLEAN value to the list of put values.
     *
     * @param columnName a column name of the value
     * @param value a BOOLEAN value to put
     * @return the Put operation builder
     */
    T booleanValue(String columnName, boolean value);

    /**
     * Adds the specified BOOLEAN value to the list of put values.
     *
     * @param columnName a column name of the value
     * @param value a BOOLEAN value to put
     * @return the Put operation builder
     */
    T booleanValue(String columnName, @Nullable Boolean value);

    /**
     * Adds the specified INT value to the list of put values.
     *
     * @param columnName a column name of the value
     * @param value a INT value to put
     * @return the Put operation builder
     */
    T intValue(String columnName, int value);

    /**
     * Adds the specified INT value to the list of put values.
     *
     * @param columnName a column name of the value
     * @param value a INT value to put
     * @return the Put operation builder
     */
    T intValue(String columnName, @Nullable Integer value);

    /**
     * Adds the specified BIGINT value to the list of put values.
     *
     * @param columnName a column name of the value
     * @param value a BIGINT value to put
     * @return the Put operation builder
     */
    T bigIntValue(String columnName, long value);

    /**
     * Adds the specified BIGINT value to the list of put values.
     *
     * @param columnName a column name of the value
     * @param value a BIGINT value to put
     * @return the Put operation builder
     */
    T bigIntValue(String columnName, @Nullable Long value);

    /**
     * Adds the specified FLOAT value to the list of put values.
     *
     * @param columnName a column name of the value
     * @param value a FLOAT value to put
     * @return the Put operation builder
     */
    T floatValue(String columnName, float value);

    /**
     * Adds the specified FLOAT value to the list of put values.
     *
     * @param columnName a column name of the value
     * @param value a FLOAT value to put
     * @return the Put operation builder
     */
    T floatValue(String columnName, @Nullable Float value);

    /**
     * Adds the specified DOUBLE value to the list of put values.
     *
     * @param columnName a column name of the value
     * @param value a DOUBLE value to put
     * @return the Put operation builder
     */
    T doubleValue(String columnName, double value);

    /**
     * Adds the specified DOUBLE value to the list of put values.
     *
     * @param columnName a column name of the value
     * @param value a DOUBLE value to put
     * @return the Put operation builder
     */
    T doubleValue(String columnName, @Nullable Double value);

    /**
     * Adds the specified TEXT value to the list of put values.
     *
     * @param columnName a column name of the value
     * @param value a TEXT value to put
     * @return the Put operation builder
     */
    T textValue(String columnName, @Nullable String value);

    /**
     * Adds the specified BLOB value as a byte array to the list of put values.
     *
     * @param columnName a column name of the value
     * @param value a BLOB value to put
     * @return the Put operation builder
     */
    T blobValue(String columnName, @Nullable byte[] value);

    /**
     * Adds the specified BLOB value as a ByteBuffer to the list of put values.
     *
     * @param columnName a column name of the value
     * @param value a BLOB value to put
     * @return the Put operation builder
     */
    T blobValue(String columnName, @Nullable ByteBuffer value);

    /**
     * Adds a column to the list of put values.
     *
     * <p>This method is primarily for internal use. Breaking changes can and will be introduced to
     * this method. Users should not depend on it.
     *
     * @param column a column to put
     * @return the Put operation builder
     */
    T value(Column<?> column);
  }

  public interface ClearValues<T> {
    /**
     * Clears the list of values.
     *
     * @return the operation builder
     */
    T clearValues();

    /**
     * Clears the value for the given column.
     *
     * @param columnName a column name
     * @return the operation builder
     */
    T clearValue(String columnName);
  }

  public interface ImplicitPreReadEnabled<T> {
    /**
     * Disables implicit pre-read for this put operation. This is a utility method for Consensus
     * Commit.
     *
     * @return the operation builder
     */
    T disableImplicitPreRead();

    /**
     * Enables implicit pre-read for this put operation. This is a utility method for Consensus
     * Commit.
     *
     * @return the operation builder
     */
    T enableImplicitPreRead();

    /**
     * Sets whether implicit pre-read is enabled or not for this put operation. This is a utility
     * method for Consensus Commit.
     *
     * @param implicitPreReadEnabled whether implicit pre-read is enabled or not
     * @return the operation builder
     */
    T implicitPreReadEnabled(boolean implicitPreReadEnabled);
  }

  public interface InsertModeEnabled<T> {
    /**
     * Disables the insert mode for this put operation. This is a utility method for Consensus
     * Commit.
     *
     * @return the operation builder
     */
    T disableInsertMode();

    /**
     * Enables the insert mode for this put operation. This is a utility method for Consensus
     * Commit.
     *
     * @return the operation builder
     */
    T enableInsertMode();

    /**
     * Sets whether the insert mode is enabled or not for this put operation. This is a utility
     * method for Consensus Commit.
     *
     * @param insertModeEnabled whether the insert mode is enabled or not
     * @return the operation builder
     */
    T insertModeEnabled(boolean insertModeEnabled);
  }

  public interface Limit<T> {
    /**
     * Sets the specified number of results to be returned
     *
     * @param limit the number of results to be returned
     * @return the scan operation builder
     */
    T limit(int limit);
  }

  public interface Ordering<T> {
    /**
     * Sets the specified scan ordering. Ordering can only be specified with clustering keys. To
     * sort results by multiple clustering keys, call this method multiple times in the order of
     * sorting or call {@link #orderings(Collection)} or {@link #orderings(Scan.Ordering...)}.
     *
     * @param ordering a scan ordering
     * @return the scan operation builder
     */
    T ordering(Scan.Ordering ordering);

    /**
     * Sets the specified scan orderings. Ordering can only be specified with clustering keys.
     *
     * @param orderings scan orderings
     * @return the scan operation builder
     */
    T orderings(Collection<Scan.Ordering> orderings);

    /**
     * Sets the specified scan orderings. Ordering can only be specified with clustering keys.
     *
     * @param orderings scan orderings
     * @return the scan operation builder
     */
    T orderings(Scan.Ordering... orderings);
  }

  public interface ClearOrderings<T> {
    /**
     * Clears the list of orderings.
     *
     * @return the scan operation builder
     */
    T clearOrderings();
  }

  public interface ClusteringKeyFiltering<T> {
    /**
     * Sets the specified clustering key as a starting point for scan. The boundary is inclusive.
     *
     * @param clusteringKey a starting clustering key
     * @return the scan operation builder
     */
    default T start(Key clusteringKey) {
      return start(clusteringKey, true);
    }

    /**
     * Sets the specified clustering key with the specified boundary as a starting point for scan.
     *
     * @param clusteringKey a starting clustering key
     * @param inclusive indicates whether the boundary is inclusive or not
     * @return the scan operation builder
     */
    T start(Key clusteringKey, boolean inclusive);

    /**
     * Sets the specified clustering key as an ending point for scan. The boundary is inclusive.
     *
     * @param clusteringKey an ending clustering key
     * @return the scan operation builder
     */
    default T end(Key clusteringKey) {
      return end(clusteringKey, true);
    }

    /**
     * Sets the specified clustering key with the specified boundary as an ending point for scan.
     *
     * @param clusteringKey an ending clustering key
     * @param inclusive indicates whether the boundary is inclusive or not
     * @return the scan operation builder
     */
    T end(Key clusteringKey, boolean inclusive);
  }

  public interface ClearBoundaries<T> {
    /**
     * Removes the scan starting boundary.
     *
     * @return the scan operation builder
     */
    T clearStart();

    /**
     * Removes the scan ending boundary.
     *
     * @return the scan operation builder
     */
    T clearEnd();
  }

  public interface All<T> {
    /**
     * Specifies the Scan operation will retrieve all the entries of the database.
     *
     * @return the scan operation builder
     */
    T all();
  }

  public interface IndexKey<T> {
    /**
     * Constructs the operation with the specified index {@link Key}.
     *
     * @param indexKey an index {@code Key}
     * @return the operation builder
     */
    T indexKey(Key indexKey);
  }

  public interface Where<T> {
    /**
     * Appends the specified condition.
     *
     * @param condition a condition
     * @return the operation builder
     */
    T where(ConditionalExpression condition);
  }

  public interface WhereAnd<T> {
    /**
     * Appends the specified set of or-wise conditions.
     *
     * @param orConditionSet a set of or-wise conditions
     * @return the operation builder
     */
    T where(OrConditionSet orConditionSet);

    /**
     * Appends the specified sets of or-wise condition set.
     *
     * @param orConditionSets sets of or-wise condition set
     * @return the operation builder
     */
    T whereAnd(Set<OrConditionSet> orConditionSets);
  }

  public interface WhereOr<T> {
    /**
     * Appends the specified set of and-wise conditions.
     *
     * @param andConditionSet a set of and-wise conditions
     * @return the operation builder
     */
    T where(AndConditionSet andConditionSet);

    /**
     * Appends the specified sets of and-wise condition set.
     *
     * @param andConditionSets sets of and-wise condition set
     * @return the operation builder
     */
    T whereOr(Set<AndConditionSet> andConditionSets);
  }

  public interface And<T> {
    /**
     * Appends the specified condition.
     *
     * @param condition a condition
     * @return the operation builder
     */
    T and(ConditionalExpression condition);

    /**
     * Appends the specified set of or-wise conditions.
     *
     * @param conditions a set of conditions
     * @return the operation builder
     */
    T and(OrConditionSet conditions);
  }

  public interface Or<T> {
    /**
     * Appends the specified condition.
     *
     * @param condition a condition
     * @return the operation builder
     */
    T or(ConditionalExpression condition);

    /**
     * Appends the specified set of and-wise conditions.
     *
     * @param conditions a set of conditions
     * @return the operation builder
     */
    T or(AndConditionSet conditions);
  }

  public interface ClearConditions<T> {
    /**
     * Clears all conditions.
     *
     * @return the scan operation builder
     */
    T clearConditions();
  }

  public interface Attribute<T> {
    /**
     * Adds the specified attribute.
     *
     * @param name the name of the attribute
     * @param value the value of the attribute
     * @return the operation builder
     */
    T attribute(String name, String value);

    /**
     * Adds the specified attributes.
     *
     * @param attributes the attributes to add
     * @return the operation builder
     */
    T attributes(Map<String, String> attributes);
  }

  public interface ClearAttribute<T> {
    /**
     * Clears all attributes.
     *
     * @return the operation builder
     */
    T clearAttributes();

    /**
     * Clears the attribute with the specified name.
     *
     * @param name the name of the attribute
     * @return the operation builder
     */
    T clearAttribute(String name);
  }

  public interface AbacReadTagAttribute<T> {
    /**
     * Adds a read tag attribute for the specified policy. This is a utility method for
     * Attribute-Based Access Control.
     *
     * @param policyName the policy name
     * @param readTag the read tag
     * @return the operation builder
     */
    T readTag(String policyName, String readTag);
  }

  public interface AbacWriteTagAttribute<T> {
    /**
     * Adds a write tag attribute for the specified policy. This is a utility method for
     * Attribute-Based Access Control.
     *
     * @param policyName the policy name
     * @param writeTag the write tag
     * @return the operation builder
     */
    T writeTag(String policyName, String writeTag);
  }

  public interface ClearAbacReadTagAttribute<T> {
    /**
     * Clear the read tag attribute for the specified policy. This is a utility method for
     * Attribute-Based Access Control.
     *
     * @param policyName the policy name
     * @return the operation builder
     */
    T clearReadTag(String policyName);

    /**
     * Clear all read tags. This is a utility method for Attribute-Based Access Control.
     *
     * @return the operation builder
     */
    T clearReadTags();
  }

  public interface ClearAbacWriteTagAttribute<T> {
    /**
     * Clear the write tag attribute for the specified policy. This is a utility method for
     * Attribute-Based Access Control.
     *
     * @param policyName the policy name
     * @return the operation builder
     */
    T clearWriteTag(String policyName);

    /**
     * Clear all write tags. This is a utility method for Attribute-Based Access Control.
     *
     * @return the operation builder
     */
    T clearWriteTags();
  }

  public abstract static class TableBuilder<T> implements Table<T> {
    final String namespace;

    public TableBuilder(String namespace) {
      this.namespace = namespace;
    }
  }

  public abstract static class PartitionKeyBuilder<T> implements PartitionKey<T> {
    @Nullable final String namespaceName;
    final String tableName;

    public PartitionKeyBuilder(@Nullable String namespaceName, String tableName) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
    }
  }

  public abstract static class Buildable<T> {
    @Nullable String namespaceName;
    String tableName;
    Key partitionKey;

    public Buildable(@Nullable String namespaceName, String tableName, Key partitionKey) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.partitionKey = partitionKey;
    }

    public abstract T build();
  }
}
