package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A command to retrieve entries within a partition from {@link DistributedStorage}. The scan range
 * is defined with a starting clustering key and an ending clustering key. {@link Ordering} can also
 * be specified to return {@link Result}s in ascending order or descending order of clustering keys.
 * The number of {@link Result} can also be limited. If none of these are set, it will return all
 * the {@link Result}s with a specified partition key in some order.
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public class Scan extends Selection {
  private Optional<Key> startClusteringKey;
  private boolean startInclusive;
  private Optional<Key> endClusteringKey;
  private boolean endInclusive;
  private final List<Ordering> orderings;
  private int limit;

  /**
   * Constructs a {@code Scan} with the specified partition {@link Key}.
   *
   * @param partitionKey a partition key (it might be composed of multiple values)
   */
  public Scan(Key partitionKey) {
    super(partitionKey, null);
    startClusteringKey = Optional.empty();
    endClusteringKey = Optional.empty();
    orderings = new ArrayList<>();
    limit = 0;
  }

  public Scan(Scan scan) {
    super(scan);
    startClusteringKey = scan.startClusteringKey;
    startInclusive = scan.startInclusive;
    endClusteringKey = scan.endClusteringKey;
    endInclusive = scan.endInclusive;
    orderings = new ArrayList<>(scan.orderings);
    limit = scan.limit;
  }

  /**
   * Sets the specified clustering key as a starting point for scan. The boundary is inclusive.
   *
   * @param clusteringKey a starting clustering key
   * @return this object
   */
  public Scan withStart(Key clusteringKey) {
    return withStart(clusteringKey, true);
  }

  /**
   * Sets the specified clustering key with the specified boundary as a starting point for scan.
   *
   * @param clusteringKey a starting clustering key
   * @param inclusive indicates whether the boundary is inclusive or not
   * @return this object
   */
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
   */
  public Scan withEnd(Key clusteringKey) {
    return withEnd(clusteringKey, true);
  }

  /**
   * Sets the specified clustering key with the specified boundary as an ending point for scan.
   *
   * @param clusteringKey an ending clustering key
   * @param inclusive indicates whether the boundary is inclusive or not
   * @return this object
   */
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
    return orderings;
  }

  /**
   * Sets the specified scan ordering. Ordering can only be specified with clustering keys. To sort
   * results by multiple clustering keys, call this method multiple times in the order of sorting.
   *
   * @param ordering a scan ordering
   * @return this object
   */
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
   */
  public Scan withLimit(int limit) {
    this.limit = limit;
    return this;
  }

  @Override
  public Scan forNamespace(String namespace) {
    return (Scan) super.forNamespace(namespace);
  }

  @Override
  public Scan forTable(String tableName) {
    return (Scan) super.forTable(tableName);
  }

  @Override
  public Scan withConsistency(Consistency consistency) {
    return (Scan) super.withConsistency(consistency);
  }

  @Override
  public void accept(OperationVisitor v) {
    v.visit(this);
  }

  @Override
  public Scan withProjection(String projection) {
    return (Scan) super.withProjection(projection);
  }

  @Override
  public Scan withProjections(Collection<String> projections) {
    return (Scan) super.withProjections(projections);
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
        && limit == other.limit);
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
        limit);
  }

  @Override
  public String toString() {
    return super.toString()
        + MoreObjects.toStringHelper(this)
            .add("startClusteringKey", startClusteringKey)
            .add("startInclusive", startInclusive)
            .add("endClusteringKey", endClusteringKey)
            .add("endInclusive", endInclusive)
            .add("orderings", orderings)
            .add("limit", limit);
  }

  /** An optional parameter of {@link Scan} command to specify ordering of returned results. */
  @Immutable
  public static class Ordering {
    private final String name;
    private final Order order;

    /**
     * Constructs a {@code Ordering} with the specified name of a clustering key of an entry and the
     * given order
     *
     * @param name the name of a clustering key in an entry to order
     * @param order the {@code Order} of results
     */
    public Ordering(String name, Order order) {
      this.name = name;
      this.order = order;
    }

    /**
     * Returns the name of the ordering clustering key
     *
     * @return the name of the ordering clustering key
     */
    public String getName() {
      return name;
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
     *   <li>both instances have the same name and order
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
      return (name.equals(other.name) && order.equals(other.order));
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, order);
    }

    @Override
    public String toString() {
      return name + "-" + order;
    }

    public enum Order {
      ASC,
      DESC,
    }
  }
}
