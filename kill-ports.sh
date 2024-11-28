#!/bin/bash

# List of ports to check and kill processes for
PORTS=(
    60081 60082 60083  # controller
    63001 63101        # group 1 node 1
    63002 63102        # group 1 node 2
    63003 63103        # group 1 node 3
    64001 64101        # group 2 node 1
    64002 64102        # group 2 node 2
    64003 64103        # group 2 node 3
)

for PORT in "${PORTS[@]}"; do
    # echo "Checking for processes listening on port $PORT..."
    # Find the process ID (PID) listening on the port
    PID=$(lsof -ti tcp:$PORT)

    if [ -n "$PID" ]; then
        # echo "Found process with PID $PID on port $PORT. Killing it..."
        kill -SIGINT $PID
        echo "Process $PID on port $PORT killed."
    else
        echo "No process found on port $PORT."
    fi
done