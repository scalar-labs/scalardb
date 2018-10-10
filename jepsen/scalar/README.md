# Scalar DB tests with Jepsen

Scalar DB is a storage abstraction and distributed transaction implementation on top of the storage.

The current Scalar DB tests work with [Cassandra test tools in Jepsen](https://github.com/scalar-labs/jepsen/tree/cassandra).

## How to run tests

1. Get Jepsen which has Cassandra tests

```
git clone -b cassandra https://github.com/scalar-labs/jepsen.git
```

2. Copy this directory and database.jar to your Jepsen directory

```
cp -r ${SCALAR_DB_HOME}/jepsen/scalar ${JEPSEN}/
cp ${SCALAR_DB_HOME}/build/libs/database.jar ${JEPSEN}/scalar/resources/
```

3. Start Jepsen with docker
  - The script starts 5 nodes and a control node (jepsen-control)

```
cd ${JEPSEN}/docker
./up.sh
```

  - Login jepsen-control

  ```
  docker exec -it jepsen-control bash
  ```

4. Install Cassandra test tool and Cassaforte

```
# in jepsen-control

# Cassandra test tool
cd /jepsen/cassandra
lein install

# Cassaforte
git clone -b driver-3.0-for-jepsen https://github.com/scalar-labs/cassaforte
cd cassaforte
lein install
```

- Or, you can add the following lines after `RUN cd /jepsen/jepsen && lein install` to `${JEPSEN}/docker/control/Dockerfile`

```
RUN cd /root && git clone -b driver-3.0-for-jepsen https://github.com/scalar-labs/cassaforte
RUN cd /root/cassaforte && lein install
RUN cd /jepsen/cassandra && lein install
```

5. Run a test of Scalar DB

```
# in jepsen-control

cd /jepsen/scalar
lein test :only scalar.transfer-test/transfer-bridge
```

  - You can find the list of tests in `/jepsen/scalar/test/scalar/transfer_test.clj`
