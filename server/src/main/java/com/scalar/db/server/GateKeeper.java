package com.scalar.db.server;

import java.util.concurrent.TimeUnit;

/** A gate keeper that manages the front gate of the server that processes requests. */
public interface GateKeeper {

  /** Opens the gate to allow incoming requests to be processed. */
  void open();

  /**
   * Closes the gate to disallow incoming requests to be processed. After this call returns, new
   * requests are rejected. Note that this method will not wait for outstanding requests to finish
   * before returning. awaitDrained(long, TimeUnit) needs to be called to wait for outstanding
   * requests to finish.
   */
  void close();

  /**
   * Waits for the server to finish outstanding requests, giving up if the timeout is reached.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return true if the server finishes outstanding requests and false otherwise
   * @throws InterruptedException if interrupted while waiting
   */
  boolean awaitDrained(long timeout, TimeUnit unit) throws InterruptedException;

  /**
   * Lets a new request in to process it in the server if the gate is open.
   *
   * @return true if the gate is open
   */
  boolean letIn();

  /** Lets a processed request out. */
  void letOut();
}
