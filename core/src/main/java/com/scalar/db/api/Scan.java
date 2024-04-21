package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.ScanBuilder.BuildableScanOrScanAllFromExisting;
import com.scalar.db.api.ScanBuilder.Namespace;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A command to retrieve entries from a storage. The scan range is defined with a starting
 * clustering key and an ending clustering key. {@link Ordering} can also be specified to return
 * {@link Result}s in ascending order or descending order of clustering keys. The number of {@link
 * Result} can also be limited. If none of these are set, it will return all the {@link Result}s
 * with a specified partition key in default clustering orders.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public class Scan extends Selection {

  private final List<Ordering> orderings;
  private Optional<Key> startClusteringKey;
  private boolean startInclusive;
  private Optional<Key> endClusteringKey;
  private boolean endInclusive;
  private int limit;
  private final Set<Conjunction> conjunctions;

  /**
   * Constructs a {@code Scan} with the specified partition {@link Key}.
   *
   * @param partitionKey a partition key (it might be composed of multiple values)
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     Scan#newBuilder()} instead
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public Scan(Key partitionKey) {
    super(partitionKey, null);
    startClusteringKey = Optional.empty();
    endClusteringKey = Optional.empty();
    orderings = new ArrayList<>();
    limit = 0;
    conjunctions = new HashSet<>();
  }

  /**
   * Copy a Scan.
   *
   * @param scan a Scan
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link
   *     Scan#newBuilder(Scan)} ()} instead
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public Scan(Scan scan) {
    super(scan);
    startClusteringKey = scan.startClusteringKey;
    startInclusive = scan.startInclusive;
    endClusteringKey = scan.endClusteringKey;
    endInclusive = scan.endInclusive;
    orderings = new ArrayList<>(scan.orderings);
    limit = scan.limit;
    conjunctions = new HashSet<>(scan.conjunctions);
  }

  /**
   * Build a {@code Scan} or {@code ScanAll} operation using a builder.
   *
   * @return a {@code Scan} operation builder
   */
  public static Namespace newBuilder() {
    return new Namespace();
  }

  /**
   * Build a {@code Scan} operation from an existing {@code Scan} object using a builder. The
   * builder will be parametrized by default with all the existing {@code Scan} attributes
   *
   * @param scan an existing {@code Scan} operation
   * @return a {@code Scan} operation builder
   */
  public static BuildableScanOrScanAllFromExisting newBuilder(Scan scan) {
    checkNotNull(scan);
    return new BuildableScanOrScanAllFromExisting(scan);
  }

  /**
   * Sets the specified clustering key as a starting point for scan. The boundary is inclusive.
   *
   * @param clusteringKey a starting clustering key
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Deprecated
  public Scan withStart(Key clusteringKey) {
    return withStart(clusteringKey, true);
  }

  /**
   * Sets the specified clustering key with the specified boundary as a starting point for scan.
   *
   * @param clusteringKey a starting clustering key
   * @param inclusive indicates whether the boundary is inclusive or not
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Deprecated
  public Scan withStart(Key clusteringKey, boolean inclusive) {
    startClusteringKey = Optional.ofNullable(clusteringKey);
    startInclusive = inclusive;
    return this;
  }

  /**
   * Returns the starting clustering {@link Key} for scan.
   *
   * @return an {@code Optional} with the starting clustering {@code Key}
   */
  @Nonnull
  public Optional<Key> getStartClusteringKey() {
    return startClusteringKey;
  }

  /**
   * Indicates whether the starting point of the scan range is inclusive or not.
   *
   * @return {@code true} if it is inclusive otherwise {@code false}
   */
  public boolean getStartInclusive() {
    return startInclusive;
  }

  /**
   * Sets the specified clustering key as an ending point for scan. The boundary is inclusive.
   *
   * @param clusteringKey an ending clustering key
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Deprecated
  public Scan withEnd(Key clusteringKey) {
    return withEnd(clusteringKey, true);
  }

  /**
   * Sets the specified clustering key with the specified boundary as an ending point for scan.
   *
   * @param clusteringKey an ending clustering key
   * @param inclusive indicates whether the boundary is inclusive or not
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Deprecated
  public Scan withEnd(Key clusteringKey, boolean inclusive) {
    endClusteringKey = Optional.ofNullable(clusteringKey);
    endInclusive = inclusive;
    return this;
  }

  /**
   * Returns the ending clustering {@link Key} for scan.
   *
   * @return an {@code Optional} with the ending clustering {@code Key}
   */
  @Nonnull
  public Optional<Key> getEndClusteringKey() {
    return endClusteringKey;
  }

  /**
   * Indicates whether the end range of clustering key is inclusive or not.
   *
   * @return {@code true} if it is inclusive otherwise {@code false}
   */
  public boolean getEndInclusive() {
    return endInclusive;
  }

  /**
   * Returns the scan orderings
   *
   * @return a {@code List} of scan ordering
   */
  @Nonnull
  public List<Ordering> getOrderings() {
    return ImmutableList.copyOf(orderings);
  }

  /**
   * Sets the specified scan ordering. Ordering can only be specified with clustering keys. To sort
   * results by multiple clustering keys, call this method multiple times in the order of sorting.
   *
   * @param ordering a scan ordering
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Deprecated
  public Scan withOrdering(Ordering ordering) {
    orderings.add(ordering);
    return this;
  }

  /**
   * Returns the number of results to be returned
   *
   * @return the number of results to be returned
   */
  public int getLimit() {
    return limit;
  }

  /**
   * Sets the specified number of results to be returned
   *
   * @param limit the number of results to be returned
   * @return this object
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Deprecated
  public Scan withLimit(int limit) {
    this.limit = limit;
    return this;
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public Scan forNamespace(String namespace) {
    return (Scan) super.forNamespace(namespace);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public Scan forTable(String tableName) {
    return (Scan) super.forTable(tableName);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public Scan withConsistency(Consistency consistency) {
    return (Scan) super.withConsistency(consistency);
  }

  @Override
  public void accept(OperationVisitor v) {
    v.visit(this);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public Scan withProjection(String projection) {
    return (Scan) super.withProjection(projection);
  }

  /**
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use the setter method of the
   *     Scan builder instead; to create a Scan builder, use {@link Scan#newBuilder()}
   */
  @Override
  @Deprecated
  public Scan withProjections(Collection<String> projections) {
    return (Scan) super.withProjections(projections);
  }

  Scan withConjunctions(Collection<Conjunction> conjunctions) {
    this.conjunctions.addAll(conjunctions);
    return this;
  }

  /**
   * Returns the set of {@code Conjunction}. We regard this set as a disjunction of conjunctions
   * (i.e., a disjunctive normal form, DNF).
   *
   * <p>This method is primarily for internal use. Breaking changes can and will be introduced to
   * this method. Users should not depend on it.
   *
   * @return set of {@code Conjunction}
   */
  @Nonnull
  public Set<Conjunction> getConjunctions() {
    return ImmutableSet.copyOf(conjunctions);
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code Scan} and
   *   <li>both instances have the same clustering key range, orderings and limit
   * </ul>
   *
   * @param o an object to be tested for equality
   * @return {@code true} if the other object is "equal to" this object otherwise {@code false}
   */
  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof Scan)) {
      return false;
    }
    Scan other = (Scan) o;
    return (startClusteringKey.equals(other.startClusteringKey)
        && startInclusive == other.startInclusive
        && endInclusive == other.endInclusive
        && endClusteringKey.equals(other.endClusteringKey)
        && orderings.equals(other.orderings)
        && limit == other.limit
        && conjunctions.equals(other.conjunctions));
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        startClusteringKey,
        startInclusive,
        endClusteringKey,
        endInclusive,
        orderings,
        limit,
        conjunctions);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespace", forNamespace())
        .add("table", forTable())
        .add("partitionKey", getPartitionKey())
        .add("projections", getProjections())
        .add("consistency", getConsistency())
        .add("startClusteringKey", startClusteringKey)
        .add("startInclusive", startInclusive)
        .add("endClusteringKey", endClusteringKey)
        .add("endInclusive", endInclusive)
        .add("orderings", orderings)
        .add("limit", limit)
        .add("conjunctions", conjunctions)
        .toString();
  }

  /** An optional parameter of {@link Scan} command to specify ordering of returned results. */
  @Immutable
  public static class Ordering {
    private final String columnName;
    private final Order order;

    /**
     * Constructs a {@code Ordering} with the specified name of a clustering key of an entry and the
     * given order
     *
     * @param columnName the column name of a clustering key in an entry to order
     * @param order the {@code Order} of results
     * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link #asc(String)}
     *     or {@link #desc(String)} to create an Ordering object
     */
    @Deprecated
    public Ordering(String columnName, Order order) {
      this.columnName = columnName;
      this.order = order;
    }

    /**
     * Creates an Ordering object for ASC order with the specified column.
     *
     * @param columnName a name of a target column
     * @return an Ordering object
     */
    public static Ordering asc(String columnName) {
      return new Ordering(columnName, Order.ASC);
    }

    /**
     * Creates an Ordering object for DESC order with the specified column.
     *
     * @param columnName a name of a target column
     * @return an Ordering object
     */
    public static Ordering desc(String columnName) {
      return new Ordering(columnName, Order.DESC);
    }

    /**
     * Returns the column name of the ordering clustering key
     *
     * @return the column name of the ordering clustering key
     * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
     */
    @Deprecated
    public String getName() {
      return columnName;
    }

    /**
     * Returns the column name of the ordering clustering key
     *
     * @return the column name of the ordering clustering key
     */
    public String getColumnName() {
      return columnName;
    }

    /**
     * Returns the order of the ordering clustering key
     *
     * @return the order of the ordering clustering key
     */
    public Order getOrder() {
      return order;
    }

    /**
     * Indicates whether some other object is "equal to" this object. The other object is considered
     * equal if:
     *
     * <ul>
     *   <li>it is also an {@code Ordering}
     *   <li>both instances have the same column name and order
     * </ul>
     *
     * @param o an object to be tested for equality
     * @return {@code true} if the other object is "equal to" this object otherwise {@code false}
     */
    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof Ordering)) {
        return false;
      }
      Ordering other = (Ordering) o;
      return (columnName.equals(other.columnName) && order.equals(other.order));
    }

    @Override
    public int hashCode() {
      return Objects.hash(columnName, order);
    }

    @Override
    public String toString() {
      return columnName + "-" + order;
    }

    public enum Order {
      ASC,
      DESC,
    }
  }

  /**
   * A conjunctive set of {@link ConditionalExpression}, and it is an internal representation of the
   * optional parameter for a {@link Scan} command, which specifies arbitrary conditions with a
   * disjunction of {@link Conjunction}s (i.e., a disjunctive normal form, DNF). Its functionality
   * is similar to {@link ScanBuilder.AndConditionSet}, but unlike {@link
   * ScanBuilder.AndConditionSet}, this class is primarily used for an internal purpose. Breaking
   * changes can and will be introduced to this class. Users should not depend on it.
   */
  @Immutable
  public static class Conjunction {
    private final ImmutableSet<ConditionalExpression> conditions;

    private Conjunction(ImmutableSet<ConditionalExpression> conditions) {
      this.conditions = conditions;
    }

    /**
     * Returns the set of {@code ConditionalExpression} which this conjunction is composed of.
     *
     * @return set of {@code ConditionalExpression} which this conjunction is composed of
     */
    public Set<ConditionalExpression> getConditions() {
      return conditions;
    }

    /**
     * Creates a {@code Conjunction} object with a single conditional expression.
     *
     * @param condition a conditional expression
     * @return a {@code Conjunction} object
     */
    public static Conjunction of(ConditionalExpression condition) {
      return new Conjunction(ImmutableSet.of(condition));
    }

    /**
     * Creates a {@code Conjunction} object with a collection of conditional expressions.
     *
     * @param conditions a collection of conditional expressions
     * @return a {@code Conjunction} object
     */
    public static Conjunction of(Collection<ConditionalExpression> conditions) {
      return new Conjunction(ImmutableSet.copyOf(conditions));
    }

    /**
     * Creates a {@code Conjunction} object with conditional expressions.
     *
     * @param conditions conditional expressions
     * @return a {@code Conjunction} object
     */
    public static Conjunction of(ConditionalExpression... conditions) {
      return new Conjunction(ImmutableSet.copyOf(conditions));
    }

    /**
     * Indicates whether some other object is "equal to" this object. The other object is considered
     * equal if:
     *
     * <ul>
     *   <li>it is also an {@code Conjunction}
     *   <li>both instances have the same set of {@code ConditionalExpression}
     * </ul>
     *
     * @param o an object to be tested for equality
     * @return {@code true} if the other object is "equal to" this object otherwise {@code false}
     */
    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof Conjunction)) {
        return false;
      }
      Conjunction other = (Conjunction) o;
      return conditions.equals(other.conditions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(conditions);
    }

    @Override
    public String toString() {
      return conditions.toString();
    }
  }
}
