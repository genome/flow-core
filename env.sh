export FLOW_HOME=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

export PATH=$FLOW_HOME/bin:$PATH
export AMQP_URL=amqp://guest:guest@vmpool82:5672/workflow
export FLOW_REDIS_URL=vmpool83

echo ""
echo "Setting up flow base environment in $FLOW_HOME"
echo ""
echo "AMQP_URL: $AMQP_URL"
echo "FLOW_REDIS_URL: $FLOW_REDIS_URL"
echo ""
