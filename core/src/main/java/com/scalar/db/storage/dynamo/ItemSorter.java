package com.scalar.db.storage.dynamo;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import com.scalar.db.io.DataType;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A sorter for items retrieved from DynamoDB
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public class ItemSorter {
  private final Scan scan;
  private final Comparator<Map<String, AttributeValue>> comparator;

  public ItemSorter(Scan scan, TableMetadata metadata) {
    checkNotNull(metadata);
    this.scan = checkNotNull(scan);
    this.comparator = getComparator(scan, metadata);
  }

  public List<Map<String, AttributeValue>> sort(List<Map<String, AttributeValue>> items) {
    checkNotNull(items);

    if (!scan.getOrderings().isEmpty()) {
      items.sort(comparator);
    }

    int limit = scan.getLimit();
    if (limit > 0 && limit < items.size()) {
      items = items.subList(0, limit);
    }

    return items;
  }

  private Comparator<Map<String, AttributeValue>> getComparator(Scan scan, TableMetadata metadata) {
    return (o1, o2) -> {
      int compareResult = 0;
      for (Scan.Ordering ordering : scan.getOrderings()) {
        String name = ordering.getName();
        compareResult =
            compareAttributeValues(metadata.getColumnDataType(name), o1.get(name), o2.get(name));
        compareResult *= ordering.getOrder() == Scan.Ordering.Order.ASC ? 1 : -1;
        if (compareResult != 0) {
          break;
        }
      }

      return compareResult;
    };
  }

  private int compareAttributeValues(DataType dataType, AttributeValue a1, AttributeValue a2)
      throws UnsupportedTypeException {
    if (a1 == null && a2 == null) {
      return 0;
    } else if (a1 == null) {
      return -1;
    } else if (a2 == null) {
      return 1;
    }

    switch (dataType) {
      case BOOLEAN:
        return a1.bool().compareTo(a2.bool());
      case INT:
        return Integer.valueOf(a1.n()).compareTo(Integer.valueOf(a2.n()));
      case BIGINT:
        return Long.valueOf(a1.n()).compareTo(Long.valueOf(a2.n()));
      case FLOAT:
        return Float.valueOf(a1.n()).compareTo(Float.valueOf(a2.n()));
      case DOUBLE:
        return Double.valueOf(a1.n()).compareTo(Double.valueOf(a2.n()));
      case TEXT:
        return a1.s().compareTo(a2.s());
      case BLOB:
        return a1.b().asByteBuffer().compareTo(a2.b().asByteBuffer());
      default:
        throw new AssertionError();
    }
  }
}
