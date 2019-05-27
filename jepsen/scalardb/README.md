# Jepsen tests for Scalar DB

This guide will teach you how to run Jepsen tests for Scalar DB.
The current tests use [Cassandra test tools in Jepsen](https://github.com/scalar-labs/jepsen/tree/cassandra).

## How to run tests

1. Get Jepsen which has Cassandra tests

    ```
    $ git clone -b cassandra https://github.com/scalar-labs/jepsen.git
    ```

2. Copy this directory to your Jepsen directory

    ```
    $ cp -r ${SCALAR_DB_HOME}/jepsen/scalardb ${JEPSEN}/
    ```

3. Start Jepsen with docker

    - The script starts 5 nodes and a control node (jepsen-control)

    ```
    $ cd ${JEPSEN}/docker
    $ ./up.sh
    ```

    - Login to jepsen-control

    ```
    $ docker exec -it jepsen-control bash
    ```

4. Install the Cassandra test tool

    ```
    # in jepsen-control
    
    $ cd /jepsen/cassandra
    $ lein install
    ```

    Or, you can add the following line after `RUN cd /jepsen/jepsen && lein install` to `${JEPSEN}/docker/control/Dockerfile`

    ```
    RUN cd /jepsen/cassandra && lein install
    ```

5. Run a test of Scalar DB

    ```
    # in jepsen-control

    $ cd /jepsen/scalardb
    $ lein run test --test transfer --nemesis crash --join decommission --time-limit 300
    ```

    Use `lein run test --help` to see a list of the full options
