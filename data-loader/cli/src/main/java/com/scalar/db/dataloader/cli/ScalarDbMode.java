package com.scalar.db.dataloader.cli;

/**
 * @deprecated As of release 3.17.0 This enum is deprecated and will be removed in release 4.0.0.
 *     The behavior is now determined by the transaction manager configuration in the ScalarDB
 *     properties file. This enum is kept only for backward compatibility with the deprecated --mode
 *     CLI option.
 */
@Deprecated
public enum ScalarDbMode {
  /** Storage mode (single-crud operations). */
  STORAGE,

  /** Transaction mode (consensus commit). */
  TRANSACTION
}
