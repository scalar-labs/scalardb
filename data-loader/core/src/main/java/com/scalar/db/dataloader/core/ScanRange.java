package com.scalar.db.dataloader.core;

import com.scalar.db.io.Key;
import lombok.Value;

/** * The scan range which is used in data export scan filtering */
@Value
public class ScanRange {
  /** The key for scan start filter */
  Key scanStartKey;
  /** The key for scan end filter */
  Key scanEndKey;
  /** To include the scan start key value in the export data scan */
  boolean isStartInclusive;
  /** To include the scan end key value in the export data scan */
  boolean isEndInclusive;
}
