import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.LockSupport;

/**
 * A TCP proxy that adds a fixed one-way delay to every chunk in both directions, to measure the
 * stack under a realistic network round trip (RTT = 2 x delay). Each chunk is forwarded {@code
 * delay} after it arrived, independently of the chunks before it, as a link would; byte order is
 * preserved, so any protocol passes through.
 *
 * <pre>java DelayProxy.java LISTEN_PORT TARGET_HOST TARGET_PORT ONE_WAY_DELAY_MICROS</pre>
 */
public class DelayProxy {
  public static void main(String[] args) throws Exception {
    int listen = Integer.parseInt(args[0]);
    String host = args[1];
    int port = Integer.parseInt(args[2]);
    long delayNanos = Long.parseLong(args[3]) * 1000L;
    try (ServerSocket server = new ServerSocket(listen)) {
      System.out.println("delaying " + listen + " -> " + host + ":" + port + " by " + args[3] + " us each way");
      while (true) {
        Socket client = server.accept();
        Socket target = new Socket(host, port);
        client.setTcpNoDelay(true);
        target.setTcpNoDelay(true);
        link(client, target, delayNanos);
        link(target, client, delayNanos);
      }
    }
  }

  private static final class Chunk {
    final byte[] data;
    final long due;

    Chunk(byte[] data, long due) {
      this.data = data;
      this.due = due;
    }
  }

  /** One direction: a reader stamps each chunk with its arrival time, a writer sends it when due. */
  private static void link(Socket from, Socket to, long delayNanos) {
    LinkedBlockingQueue<Chunk> queue = new LinkedBlockingQueue<>();
    daemon(
        () -> {
          byte[] buffer = new byte[65536];
          try (InputStream in = from.getInputStream()) {
            for (int n = in.read(buffer); n > 0; n = in.read(buffer)) {
              byte[] data = new byte[n];
              System.arraycopy(buffer, 0, data, 0, n);
              queue.put(new Chunk(data, System.nanoTime() + delayNanos));
            }
          } catch (Exception e) {
            // the other side went away
          } finally {
            queue.add(new Chunk(new byte[0], 0)); // end of stream
          }
        });
    daemon(
        () -> {
          try (OutputStream out = to.getOutputStream()) {
            while (true) {
              Chunk chunk = queue.take();
              if (chunk.data.length == 0) {
                break;
              }
              // park until close to the deadline, then spin: parkNanos alone overshoots by
              // hundreds of microseconds on macOS
              for (long left = chunk.due - System.nanoTime(); left > 500_000; left = chunk.due - System.nanoTime()) {
                LockSupport.parkNanos(left - 500_000);
              }
              while (System.nanoTime() < chunk.due) {
                Thread.onSpinWait();
              }
              out.write(chunk.data);
              out.flush();
            }
          } catch (Exception e) {
            // the other side went away
          } finally {
            try {
              from.close();
              to.close();
            } catch (Exception ignored) {
              // nothing to do
            }
          }
        });
  }

  private static void daemon(Runnable r) {
    Thread t = new Thread(r);
    t.setDaemon(true);
    t.start();
  }
}
