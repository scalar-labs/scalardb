package core;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.AdminService;
import com.scalar.db.service.StorageModule;
import java.util.List;
import java.util.logging.Logger;
import utils.CoordinatorSchema;
import utils.Table;

public class SchemaOperator {

  protected AdminService service;

  public SchemaOperator(DatabaseConfig dbConfig) {
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
        Logger.getGlobal()
            .warning("Ignored coordinator schema creation. " + e.getCause().getMessage());
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
        Logger.getGlobal()
            .warning("Created table " + table.getTable() + " failed. " + e.getCause());
      }
    }
  }

  public void deleteTables(List<Table> tableList) {
    for (Table table : tableList) {
      try {
        service.dropTable(table.getNamespace(), table.getTable());
        Logger.getGlobal()
            .info(
                "Deleted table "
                    + table.getTable()
                    + " in namespace "
                    + table.getNamespace()
                    + " successfully.");
      } catch (ExecutionException e) {
        Logger.getGlobal().warning("Delete table " + table.getTable() + " failed. " + e.getCause());
      }
    }
  }
}
