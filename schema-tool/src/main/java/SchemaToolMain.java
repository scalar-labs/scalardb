import command.SchemaToolCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;

public class SchemaToolMain implements Runnable {
  public static void main(String... args) {
    new CommandLine(new SchemaToolCommand()).execute(args);
  }

  @Override
  public void run() {}
}
