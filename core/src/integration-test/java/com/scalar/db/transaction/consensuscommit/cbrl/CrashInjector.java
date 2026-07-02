package com.scalar.db.transaction.consensuscommit.cbrl;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only crash hook driven by Byteman rules (see {@code
 * CbrlBackupRestoreIntegrationTest.restore_CrashInjectedAtNamedSites_ShouldConvergeOnReRun}). A
 * {@code @BMRule} at a named CBRL restore method calls {@link #hit(String)}; the crash fires on
 * exactly the k-th armed invocation of the armed site, so a restore can be killed deterministically
 * mid-recovery, mid-replay, or mid-write-back.
 *
 * <p>This is public only so a Byteman rule injected into a method in another package (e.g. {@code
 * JdbcDatabase.put}) can call it; it lives in the integration-test source set and adds no hook to
 * any production class — Byteman intercepts the real methods without modifying them.
 *
 * <p>The counter and fired count give the crash test its teeth: if a rule never binds (renamed
 * method or wrong signature), {@link #hit(String)} is never called, {@link #firedCount()} stays 0,
 * and the test's {@code fired == 1} assertion fails loudly. {@link #hit(String)} uses atomics so it
 * fires exactly once even if a site is reached concurrently.
 */
public final class CrashInjector {
  private static final AtomicBoolean armed = new AtomicBoolean(false);
  private static volatile String armedSite = null;
  private static volatile int fireAt = 1;
  private static final AtomicInteger invocations = new AtomicInteger();
  private static final AtomicInteger fired = new AtomicInteger();

  private CrashInjector() {}

  /** Arms the crash at {@code site}, to fire on the {@code fireAtInvocation}-th call of it. */
  public static void arm(String site, int fireAtInvocation) {
    armedSite = site;
    fireAt = fireAtInvocation;
    invocations.set(0);
    fired.set(0);
    armed.set(true);
  }

  /** Disarms all injection: subsequent {@link #hit(String)} calls are no-ops. */
  public static void disarm() {
    armed.set(false);
  }

  /** How many times the crash has fired since the last {@link #arm(String, int)} (0 or 1). */
  public static int firedCount() {
    return fired.get();
  }

  /**
   * Called from a Byteman rule at an injection point. No-op unless {@code site} is the armed site;
   * on the k-th armed hit it records the fire and throws, killing the restore at that point.
   */
  public static void hit(String site) {
    if (!armed.get() || !site.equals(armedSite)) {
      return;
    }
    if (invocations.incrementAndGet() == fireAt) {
      fired.incrementAndGet();
      throw new CrashInjectionError(site, fireAt);
    }
  }

  /** The injected failure — a distinct type so the test can tell it from a real restore error. */
  public static final class CrashInjectionError extends RuntimeException {
    CrashInjectionError(String site, int invocation) {
      super("Injected CBRL restore crash at '" + site + "' on invocation " + invocation);
    }
  }
}
