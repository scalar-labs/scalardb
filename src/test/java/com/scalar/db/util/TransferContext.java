package com.scalar.db.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransferContext {
  private final int numAccounts;
  private final int runFor;
  private final int ramp;
  private final int clientSeed;
  private final boolean isBenchmark;

  private int committed = 0;
  private int failed = 0;
  private Map<String, List<Integer>> unknownTransactions = new HashMap<>();

  public TransferContext(int numAccounts, int runFor, int ramp, int clientSeed) {
    this(numAccounts, runFor, ramp, clientSeed, false);
  }

  public TransferContext(
      int numAccounts, int runFor, int ramp, int clientSeed, boolean isBenchmark) {
    this.numAccounts = numAccounts;
    this.runFor = runFor;
    this.ramp = ramp;
    this.clientSeed = clientSeed;
    this.isBenchmark = isBenchmark;
  }

  public int getNumAccounts() {
    return numAccounts;
  }

  public int getRunTime() {
    return runFor;
  }

  public int getRampTime() {
    return ramp;
  }

  public int getClientSeed() {
    return clientSeed;
  }

  public synchronized int getCommitted() {
    return committed;
  }

  public synchronized int getFailed() {
    return failed;
  }

  public Map<String, List<Integer>> getUnknownTransactions() {
    return unknownTransactions;
  }

  public synchronized void addUnknownTx(String txId, List<Integer> ids) {
    unknownTransactions.put(txId, ids);
  }

  public synchronized void logLatency(long runStart, long start, long end) {
    if (isBenchmark && runStart < start) {
      System.out.println((start - runStart) + " " + (end - start));
    }
  }

  public synchronized void logStart(String txId, List<Integer> ids, int amount) {
    logTxInfo("started", txId, ids, amount);
  }

  public synchronized void logSuccess(String txId, List<Integer> ids, int amount) {
    committed += ids.size();
    logTxInfo("succeeded", txId, ids, amount);
  }

  public synchronized void logFailure(String txId, List<Integer> ids, int amount) {
    failed++;
    logTxInfo("falied", txId, ids, amount);
  }

  private void logTxInfo(String status, String txId, List<Integer> ids, int amount) {
    if (isBenchmark) {
      return;
    }

    int fromId = ids.get(0);
    int toId = ids.get(1);

    String message =
        status
            + " - id: "
            + txId
            + " from: "
            + fromId
            + ",0"
            + " to: "
            + toId
            + ","
            + ((fromId == toId) ? 1 : 0);

    if (ids.size() > 2) {
      message += " another: " + ids.get(2) + ",0";
    }

    message += " amount: " + amount;
    System.err.println(message);
  }
}
