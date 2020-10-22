package sample;

public class ElectronicMoneyMain {
  public static void main(String[] args) throws Exception {
    String mode = null;
    String action = null;
    int amount = 0;
    String to = null;
    String from = null;

    for (int i = 0; i < args.length; ++i) {
      if ("-mode".equals(args[i])) {
        mode = args[++i];
      } else if ("-action".equals(args[i])) {
        action = args[++i];
      } else if ("-amount".equals(args[i])) {
        amount = Integer.parseInt(args[++i]);
      } else if ("-to".equals(args[i])) {
        to = args[++i];
      } else if ("-from".equals(args[i])) {
        from = args[++i];
      } else if ("-help".equals(args[i])) {
        printUsageAndExit();
      }
    }
    if (mode == null || action == null || to == null || amount < 0) {
      printUsageAndExit();
    }

    ElectronicMoney eMoney = null;
    if (mode.equalsIgnoreCase("storage")) {
      eMoney = new ElectronicMoneyWithStorage();
    } else {
      eMoney = new ElectronicMoneyWithTransaction();
    }

    if (action.equalsIgnoreCase("charge")) {
      eMoney.charge(to, amount);
    } else if (action.equalsIgnoreCase("pay")) {
      if (from == null) {
        printUsageAndExit();
      }
      eMoney.pay(from, to, amount);
    }
    eMoney.close();
  }

  private static void printUsageAndExit() {
    System.err.println(
        "ElectronicMoneyMain -mode storage/transaction -action charge/pay -amount number -to id [-from id (needed for pay)]");
    System.exit(1);
  }
}
