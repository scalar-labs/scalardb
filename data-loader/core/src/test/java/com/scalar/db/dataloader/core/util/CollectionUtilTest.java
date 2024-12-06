package com.scalar.db.dataloader.core.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for CollectionUtils */
class CollectionUtilTest {

  @Test
  void areSameLength_CollectionsAllSameLength_ShouldReturnTrue() {
    List<String> listOne = new ArrayList<>();
    List<Integer> listTwo = new ArrayList<>();
    boolean actual = CollectionUtil.areSameLength(listOne, listTwo);
    assertThat(actual).isTrue();
  }

  @Test
  void areSameLength_CollectionsDifferentLength_ShouldReturnFalse() {
    List<String> listOne = new ArrayList<>();
    List<Integer> listTwo = new ArrayList<>();
    listTwo.add(5);
    boolean actual = CollectionUtil.areSameLength(listOne, listTwo);
    assertThat(actual).isFalse();
  }
}
