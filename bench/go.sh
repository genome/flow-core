#!/usr/bin/env bash

set -e

ITERATIONS=1

declare -a ORCHESTRATORS=(
    1
    5
    10
    20
)

declare -a OPERATIONS=(
    50
    500
    5000
    50000
)


export FLOW_CONFIG_PATH=`pwd`/config


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

    for M in ${OPERATIONS[@]}; do
        echo -n "$N,$M"
        for iteration in `seq 1 $ITERATIONS`; do
            RUNTIME="$(flow benchmark --size $M)"
            echo -n ",$RUNTIME"
        done

        echo # newline
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
