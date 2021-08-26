package core;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.AdminService;
import com.scalar.db.service.StorageModule;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schema.CoordinatorSchema;
import schema.Table;

public class SchemaOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaOperator.class);

  private AdminService service;

  public SchemaOperator(DatabaseConfig dbConfig) {
    Injector injector = Guice.createInjector(new StorageModule(dbConfig));
    service = injector.getInstance(AdminService.class);
  }

  public void createTables(List<Table> tableList) {
    boolean hasTransactionTable = false;


    for (Table table : tableList) {
      if (table.isTransactionTable()) {
        hasTransactionTable = true;
      }
      try {
        service.createTable(
            table.getNamespace(), table.getTable(), table.getTableMetadata(), table.getOptions());
        LOGGER.info(
            "Create table "
                + table.getTable()
                + " in namespace "
                + table.getNamespace()
                + " successfully.");
      } catch (ExecutionException e) {
        LOGGER.warn("Created table " + table.getTable() + " failed. " + e.getCause());
      }
    }

    if (hasTransactionTable) {
      CoordinatorSchema coordinatorSchema = new CoordinatorSchema();
      try {
        service.createTable(
            coordinatorSchema.getNamespace(),
            coordinatorSchema.getTable(),
            coordinatorSchema.getTableMetadata(),
            null);
        LOGGER.info("Created coordinator schema.");
      } catch (ExecutionException e) {
        LOGGER.warn("Ignored coordinator schema creation. " + e.getCause().getMessage());
      }
    }
  }

  public void deleteTables(List<Table> tableList) {
    for (Table table : tableList) {
      try {
        service.dropTable(table.getNamespace(), table.getTable());
        LOGGER.info(
            "Deleted table "
                + table.getTable()
                + " in namespace "
                + table.getNamespace()
                + " successfully.");
      } catch (ExecutionException e) {
        LOGGER.warn("Delete table " + table.getTable() + " failed. " + e.getCause());
      }
    }
  }
}
