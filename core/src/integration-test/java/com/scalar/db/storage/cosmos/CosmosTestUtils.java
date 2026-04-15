package com.scalar.db.storage.cosmos;

public final class CosmosTestUtils {

  // Cosmos DB stores numbers internally as double-precision floating-point, which can only
  // guarantee integer precision up to 2^53. BigInt values must be within this range.
  public static final long BIGINT_MAX_VALUE = 9007199254740992L; // 2^53
  public static final long BIGINT_MIN_VALUE = -9007199254740992L; // -2^53

  private CosmosTestUtils() {}
}
