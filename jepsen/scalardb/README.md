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

    Before you start docker you will need to edit the node ubuntu Dockerfile so that the `openjdk-8-jre` package is installed. That is, edit `${JEPSEN}/docker/node/Dockerfile-ubuntu` to look like
    
    ```docker
    FROM       jacobmbr/ubuntu-jepsen:v0.1.0
    
    RUN rm /etc/apt/apt.conf.d/docker-clean && apt-get update
    
    # Install Jepsen dependencies
    RUN apt-get install -y openssh-server \
        curl faketime iproute2 iptables iputils-ping libzip4 \
        logrotate man man-db net-tools ntpdate psmisc python rsyslog \
        sudo unzip vim wget apt-transport-https \
        openjdk-8-jre \
        && apt-get remove -y --purge --auto-remove systemd
    ```
    
    - Fire up docker. The script starts five nodes and a control node (jepsen-control)

    ```
    $ cd ${JEPSEN}/docker
    $ ./up.sh --ubuntu
    ```

    - Login to jepsen-control

    ```
    $ docker exec -it jepsen-control bash
    ```

4. In jepsen-control install Cassaforte (Clojure wrapper for Cassandra) and the Cassandra test tool 

```
# in jepsen-control

# Cassaforte
$ git clone -b driver-3.0-for-jepsen https://github.com/scalar-labs/cassaforte
$ cd cassaforte
$ lein install

# Cassandra test tool
$ cd /jepsen/cassandra
$ lein install
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

$ cd /jepsen/scalardb
$ lein run test --test transfer --nemesis crash --join decommission --time-limit 300
```

Use `lein run test --help` to see a list of the full options
