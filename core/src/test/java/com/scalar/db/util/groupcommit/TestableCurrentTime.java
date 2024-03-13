package com.scalar.db.util.groupcommit;

class TestableCurrentTime extends CurrentTime {
  private long currentTimeMillis;

  TestableCurrentTime(long currentTimeMillis) {
    this.currentTimeMillis = currentTimeMillis;
  }

  TestableCurrentTime() {
    this(System.currentTimeMillis());
  }

  void incrementCurrentTimeMillis(long diff) {
    currentTimeMillis += diff;
  }

  @Override
  public long currentTimeMillis() {
    return currentTimeMillis;
  }
}
