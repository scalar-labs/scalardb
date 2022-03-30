package com.scalar.db.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ClusteringOrderingTest {

  @Test
  public void column_ShouldBuildProperly() {
    // Arrange

    // Act
    ClusteringOrdering actual1 = ClusteringOrdering.column("col1").asc();
    ClusteringOrdering actual2 = ClusteringOrdering.column("col2").desc();

    // Assert
    assertThat(actual1.columnName).isEqualTo("col1");
    assertThat(actual1.clusteringOrder).isEqualTo(ClusteringOrder.ASC);

    assertThat(actual2.columnName).isEqualTo("col2");
    assertThat(actual2.clusteringOrder).isEqualTo(ClusteringOrder.DESC);
  }
}
