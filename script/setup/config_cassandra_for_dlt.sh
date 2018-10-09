#!/bin/sh

# Support only Cassandra 3.11.x.

cd $(dirname $0)

YAML_FILE=cassandra.yaml
ENV_FILE=cassandra-env.sh
LOGBACK_FILE=logback.xml
DEFAULT_YAML=default/$YAML_FILE
DEFAULT_ENV=default/$ENV_FILE
DEFAULT_LOGBACK=default/$LOGBACK_FILE
CASSANDRA_CMD="sudo /etc/init.d/cassandra start"

# check node_params
while read line
do
    attr=${line%:*}
    params=${line#*:}

    case $attr in
        "node" ) NODES=$params
                 NODE_LIST=(${NODES//,/ }) ;;
        "seed" ) SEED_LIST=$params ;;
        "commitlog_dir" ) COMMITLOG_DIR=${params//\//\\/} ;;
        "data_dir" ) DATA_DIR=${params//\//\\/} ;;
        "hints_dir" ) HINTS_DIR=${params//\//\\/} ;;
        "saved_caches_dir" ) SAVED_CACHES_DIR=${params//\//\\/} ;;
        "conf_dir") CONF_DIR=$params ;;
        "cassandra_cmd") CASSANDRA_CMD=$params ;;
        * ) echo "ERROR: unexpected parameters"
            exit 1 ;;
    esac
done < node_params

# logback.xml isn't changed from the default for now
cp $DEFAULT_LOGBACK ./$LOGBACK_FILE

# check system
half_memory_in_gb=$(free -m | awk '/:/ {printf("%.f", $2/1024/2);exit}')
cores=$(egrep -c 'processor([[:space:]]+):.*' /proc/cpuinfo)
heap_newsize=$(expr $cores \* 100)

# change cassandra-env.sh
sed -e "s/_MAX_HEAP_SIZE_/$half_memory_in_gb/" \
    -e "s/_HEAP_NEWSIZE_/$heap_newsize/" \
    $DEFAULT_ENV > $ENV_FILE

# make cassandra.yaml and copy them to each node
for node in ${NODE_LIST[@]}
do
    sed -e "s/_IP_/$node/g" \
        -e "s/_SEEDS_/$SEED_LIST/" \
        -e "s/_COMMITLOG_DIR_/$COMMITLOG_DIR/" \
        -e "s/_DATA_DIR_/$DATA_DIR/" \
        -e "s/_HINTS_DIR_/$HINTS_DIR/" \
        -e "s/_SAVED_CACHES_DIR_/$SAVED_CACHES_DIR/" \
        $DEFAULT_YAML > $YAML_FILE.$node

    scp $YAML_FILE.$node $node:~/$YAML_FILE
    scp $LOGBACK_FILE $node:~/$LOGBACK_FILE
    scp $ENV_FILE $node:~/$ENV_FILE
done

pdsh -w $NODES "sudo mv ~/$YAML_FILE $CONF_DIR"
pdsh -w $NODES "sudo mv ~/$LOGBACK_FILE $CONF_DIR"
pdsh -w $NODES "sudo mv ~/$ENV_FILE $CONF_DIR"

# Linux tuning
pdsh -w $NODES "sudo sh -c 'echo 8 > /sys/block/nvme0n1/queue/read_ahead_kb'"
##pdsh -w $NODES "sudo sh -c 'echo deadline > /sys/block/nvme0n1/queue/scheduler'"
pdsh -w $NODES "sudo swapoff --all"

# start C*
for node in ${NODE_LIST[@]}
do
    pdsh -w $node $CASSANDRA_CMD
    sleep 60
done
