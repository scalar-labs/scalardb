# How to use
1. Install Java, C* and pdsh on each node
- Refer to [Apache document](https://cassandra.apache.org/download/) and [datastax documant](https://docs.datastax.com/en/cassandra/3.0/cassandra/install/installJdkRHEL.html)

- Example for RHEL-based Systems
```
# install pdsh
sudo yum --enablerepo=epel install -y pdsh

# JAVA
sudo rpm -ivh jdk-8u162-linux-x64.rpm
sudo alternatives --install /usr/bin/java java /usr/java/jdk1.8.0_162/bin/java 200000

# if needed
sudo alternatives --config java

# setup repo for C*
sudo sh -c 'cat << EOS > /etc/yum.repos.d/cassandra.repo
[cassandra]
name=Apache Cassandra
baseurl=https://www.apache.org/dist/cassandra/redhat/311x/
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://www.apache.org/dist/cassandra/KEYS
EOS'

# install C*
sudo yum install -y cassandra
```

2. Setup SSH on each node

3. Setup storage devices for C*
- Example for AWS i3 instance
```
sudo /sbin/mkfs -t ext4 /dev/nvme0n1
sudo mount -t ext4 /dev/nvme0n1 /data

sudo mkdir -p /data/cassandra/data
sudo mkdir -p /data/cassandra/commitlog
sudo mkdir -p /data/cassandra/hints
sudo mkdir -p /data/cassandra/saved_caches

sudo chown -R cassandra:cassandra /data/cassandra
```

4. Edit `node_params` and write addresses of nodes, seed addresses and each directory path as below
```
node:192.168.1.1,192.168.1.2,192.168.1.3
seed:192.168.1.1,192.168.1.3
core:4
memory_gb:16
request_concurrency:32
commitlog_dir:/data/cassandra/commitlog
data_dir:/data/cassandra/data
hints_dir:/data/cassandra/hints
saved_caches_dir:/data/cassandra/saved_caches
conf_dir:/etc/cassandra/conf
data_drive:nvme0n1
message_coalescing:false
cassandra_cmd:sudo /etc/init.d/cassandra start
```
- `core` and `memory_gb` of selected nodes
- `request_concurrency` is the estimated concurrency from clients at peak time for your system
- It is better to set `message_coalescing` `true` when concurrency will be always high (more than 48)
- `data_drive` is a data drive for Cassandra whose name can be seen by `lsblk` etc.
  - In this example, it is the drive which `/data` is mounted on

5. Make C* config files and copy them to each node with the script
```
./config_cassandra_for_dlt.sh
```

## Appendix
### Create keyspaces for DLT
```
cqlsh -e "CREATE KEYSPACE coordinator WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'};"
cqlsh -e "CREATE KEYSPACE benchmark WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'};"
```
