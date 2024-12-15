#!/bin/bash

# List of ports to check and kill processes for
PORTS=(
    60081 60082 60083  # controller
    63001 63101 51001 52001 53001       # group 1 node 1
    63002 63102 51002 52002 53002       # group 1 node 2
    63003 63103 51003 52003 53003       # group 1 node 3
    64001 64101 51004 52004 53004       # group 2 node 1
    64002 64102 51005 52005 53005       # group 2 node 2
    64003 64103 51006 52006 53006       # group 2 node 3
)

for PORT in "${PORTS[@]}"; do
    # echo "Checking for processes listening on port $PORT..."
    # Find the process ID (PID) listening on the port
    PID=$(lsof -ti tcp:$PORT)

    if [ -n "$PID" ]; then
        # echo "Found process with PID $PID on port $PORT. Killing it..."
        echo "Process $PID is running on $PORT."
    else
        echo "No process found on port $PORT."
    fi
done