package core;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.AdminService;
import com.scalar.db.service.StorageModule;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;
import utils.CoordinatorSchema;
import utils.Table;

public class SchemaOperator {
  protected DatabaseConfig dbConfig;

  protected AdminService service;

  public SchemaOperator(String configPath) throws IOException {
    dbConfig = new DatabaseConfig(new FileInputStream(configPath));
    Injector injector = Guice.createInjector(new StorageModule(dbConfig));
    service = injector.getInstance(AdminService.class);
  }

  public void createTables(boolean hasTransactionTable, List<Table> tableList) {
    if (hasTransactionTable) {
      CoordinatorSchema coordinatorSchema = new CoordinatorSchema();
      try {
        service.createTable(
            coordinatorSchema.getNamespace(),
            coordinatorSchema.getTable(),
            coordinatorSchema.getTableMetadata(),
            null);
        Logger.getGlobal().info("Created coordinator schema.");
      } catch (ExecutionException e) {
        Logger.getGlobal().warning("Ignored coordinator schema creation. " + e.getCause());
      }
    }

    for (Table table : tableList) {
      try {
        service.createTable(
            table.getNamespace(), table.getTable(), table.getTableMetadata(), table.getOptions());
        Logger.getGlobal()
            .info(
                "Create table "
                    + table.getTable()
                    + " in namespace "
                    + table.getNamespace()
                    + " successfully.");
      } catch (ExecutionException e) {
        Logger.getGlobal().warning("Create table " + table.getTable() + " failed. " + e.getCause());
      }
    }
  }
}
