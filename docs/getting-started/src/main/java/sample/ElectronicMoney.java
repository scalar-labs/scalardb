package sample;

import com.scalar.db.config.DatabaseConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public abstract class ElectronicMoney {
  private static final String SCALARDB_PROPERTIES =
      System.getProperty("user.dir") + File.separator + "scalardb.properties";
  protected static final String NAMESPACE = "emoney";
  protected static final String TABLENAME = "account";
  protected static final String ID = "id";
  protected static final String BALANCE = "balance";
  protected DatabaseConfig dbConfig;

  public ElectronicMoney() throws IOException {
    dbConfig = new DatabaseConfig(new FileInputStream(SCALARDB_PROPERTIES));
  }

  abstract void charge(String id, int amount) throws Exception;

  abstract void pay(String fromId, String toId, int amount) throws Exception;

  abstract void close();
}
