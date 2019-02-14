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
        "core" ) CORES=$params ;;
        "memory_gb" ) MEMORY_GB=$params ;;
        "request_concurrency" ) CONCURRENCY=$params ;;
        "commitlog_dir" ) COMMITLOG_DIR=${params//\//\\/} ;;
        "data_dir" ) DATA_DIR=${params//\//\\/} ;;
        "hints_dir" ) HINTS_DIR=${params//\//\\/} ;;
        "saved_caches_dir" ) SAVED_CACHES_DIR=${params//\//\\/} ;;
        "conf_dir") CONF_DIR=$params ;;
        "message_coalescing") MESSAGE_COALESCING=$params ;;
        "cassandra_cmd") CASSANDRA_CMD=$params ;;
        "data_drive") DATA_DRIVE=$params ;;
        * ) echo "ERROR: unexpected parameters"
            exit 1 ;;
    esac
done < node_params

# logback.xml isn't changed from the default for now
cp $DEFAULT_LOGBACK ./$LOGBACK_FILE

# check system
half_memory_in_gb=$(expr $MEMORY_GB / 2)
heap_newsize=$(expr $CORES \* 100)
cpu_concurrency=$(expr $CORES \* 8)
if [ $cpu_concurrency -gt $CONCURRENCY ]; then
    CONCURRENCY=$cpu_concurrency
fi

# change cassandra-env.sh
sed -e "s/_MAX_HEAP_SIZE_/$half_memory_in_gb/" \
    -e "s/_HEAP_NEWSIZE_/$heap_newsize/" \
    $DEFAULT_ENV > $ENV_FILE

# make cassandra.yaml and copy them to each node
for node in ${NODE_LIST[@]}
do
    sed -e "s/_IP_/$node/g" \
        -e "s/_SEEDS_/$SEED_LIST/" \
        -e "s/_CONCURRENCY_/$CONCURRENCY/" \
        -e "s/_COMMITLOG_DIR_/$COMMITLOG_DIR/" \
        -e "s/_DATA_DIR_/$DATA_DIR/" \
        -e "s/_HINTS_DIR_/$HINTS_DIR/" \
        -e "s/_SAVED_CACHES_DIR_/$SAVED_CACHES_DIR/" \
        $DEFAULT_YAML > $YAML_FILE.$node

    if $MESSAGE_COALESCING; then
        echo "otc_coalescing_strategy: TIMEHORIZON" >> $YAML_FILE.$node
        echo "otc_coalescing_window_us: 1000" >> $YAML_FILE.$node
    fi

    scp $YAML_FILE.$node $node:~/$YAML_FILE
    scp $LOGBACK_FILE $node:~/$LOGBACK_FILE
    scp $ENV_FILE $node:~/$ENV_FILE
done

pdsh -w $NODES "sudo mv ~/$YAML_FILE $CONF_DIR"
pdsh -w $NODES "sudo mv ~/$LOGBACK_FILE $CONF_DIR"
pdsh -w $NODES "sudo mv ~/$ENV_FILE $CONF_DIR"

# Linux tuning
pdsh -w $NODES "sudo sh -c 'echo 8 > /sys/block/${DATA_DRIVE}/queue/read_ahead_kb'"
pdsh -w $NODES "sudo swapoff --all"

# start C*
for node in ${NODE_LIST[@]}
do
    pdsh -w $node $CASSANDRA_CMD
    sleep 60
done
