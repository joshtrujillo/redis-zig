#!/usr/bin/env bash
# Concurrent PING flood. Usage: ./ping_flood.sh [host] [port] [connections] [pings_each]
HOST=${1:-127.0.0.1}
PORT=${2:-6379}
CONNS=${3:-50}
PINGS=${4:-1000}

flood() {
    for _ in $(seq 1 "$PINGS"); do
        printf "*1\r\n\$4\r\nPING\r\n"
    done | nc -N "$HOST" "$PORT" > /dev/null
}

export -f flood
export HOST PORT PINGS

echo "Flooding $HOST:$PORT with $CONNS concurrent connections, $PINGS pings each..."
start=$(date +%s%N)

seq 1 "$CONNS" | xargs -P "$CONNS" -I{} bash -c 'flood'

end=$(date +%s%N)
elapsed=$(( (end - start) / 1000000 ))
total=$(( CONNS * PINGS ))
echo "Done. $total pings in ${elapsed}ms"
