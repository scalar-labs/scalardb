package sample;

import java.util.Properties;

public abstract class ElectronicMoney {
  protected static final String NAMESPACE = "emoney";
  protected static final String TABLENAME = "account";
  protected static final String ID = "id";
  protected static final String BALANCE = "balance";
  protected Properties props;

  public ElectronicMoney() {
    props = new Properties();
    props.setProperty("scalar.database.contact_points", "localhost");
    props.setProperty("scalar.database.username", "cassandra");
    props.setProperty("scalar.database.password", "cassandra");
  }

  abstract void charge(String id, int amount) throws Exception;

  abstract void pay(String fromId, String toId, int amount) throws Exception;

  abstract void close();
}
