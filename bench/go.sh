#!/usr/bin/env bash

set -e

ITERATIONS=1

declare -a ORCHESTRATORS=(
#    1
    5
    10
    20
)

declare -a NUM_GROUPS=(
#    1
    3
)

declare -a SIZES=(
    20
    30
)

export FLOW_CONFIG_PATH=`pwd`/config

function cleanup {
    kill $(jobs -p)
}
trap "cleanup" EXIT

# setup rabbit
RMQ_VHOST=core_bench
RMQ_USER=guest
RMQA="./rabbitmqadmin -H localhost"
$RMQA declare vhost name=$RMQ_VHOST > /dev/null
$RMQA declare permission vhost=$RMQ_VHOST user=$RMQ_USER \
    configure='.*' write='.*' read='.*' > /dev/null

flow configure-rabbitmq


# setup redis
redis-server config/redis.conf > /dev/null &
REDIS_PID=`jobs -p`


# Run benchmark
for N in ${ORCHESTRATORS[@]}; do
    mkdir -p logs
    for o in `seq 1 $N`; do
        flow orchestrator &> "logs/orchestrator-$o.log" &
    done

    for M in ${NUM_GROUPS[@]}; do
        for SIZE in ${SIZES[@]}; do
            OPERATIONS=$(($SIZE**$M))
            for iteration in `seq 1 $ITERATIONS`; do
                RUNTIME="$(flow benchmark --groups $M --size $SIZE)"
                echo "$N,$OPERATIONS,$RUNTIME"
            done
        done
    done

    kill $(jobs -p | tail -n +2)
    sleep 1
done

sleep 1

# cleanup redis
kill $REDIS_PID
sleep 1


# cleanup rabbit
$RMQA -f tsv list connections name vhost | \grep $RMQ_VHOST | cut -f1 | while read conn
do
    echo "Closing connection: $conn"
    $RMQA close connection name="$conn" || true &
done
wait

$RMQA delete vhost name=$RMQ_VHOST > /dev/null || true
