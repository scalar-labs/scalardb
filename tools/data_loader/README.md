# DataLoader CLI

DataLoader is a command-line tool that loads data into Scalar DB.

## Usage

### Generate an executable

```bash
./gradlew installDist
```

After a successful build, the executable could be found in `/build/install/DataLoader/bin`.

### Requirements

Make sure you have all the following files ready. 

* A Scalar DB configuration file (see [sample](./sample/scalardb.properties))
* A Scalar DB table schema file (see [sample](./sample/table.json)). It is required to create the 
database beforehand.
* A file containing the data to insert, update or delete (see [sample](./sample/data_file.json))



### Commands
To display the usage 
```
./build/install/data_loader/bin/data_loader --help
```

The following command loads data from `data_file.json` and (by default) `insert` it into the Scalar DB database. 
```
./build/install/data_loader/bin/data_loader --data-file sample/data_file.json --properties sample/scalardb.properties --table-schema sample/table.json
```

The following command loads data from `data_file.json` and `update` the data in the Scalar DB database.
```
./build/install/data_loader/bin/data_loader --data-file sample/data_file.json --properties sample/scalardb.properties --table-schema sample/table.json update
```

The following command loads data from `data_file.json` and `delete` the data in the Scalar DB database.
```
./build/install/data_loader/bin/data_loader --data-file sample/data_file.json --properties sample/scalardb.properties --table-schema sample/table.json --rule delete
```
