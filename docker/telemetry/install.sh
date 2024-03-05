#!/bin/sh

# Set the environment variables
if [ -z "$DATA_PATH" ]; then
    # Set DATA_PATH to the current absolute path
    export DATA_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi
echo "DATA_PATH set to: $DATA_PATH"

start_containers() {
  docker compose -f ./docker-compose.yaml up -d
  echo "Done."
}

# Define the function to stop the containers
stop_containers() {
  docker compose -f ./docker-compose.yaml stop
  echo "Done."
}

restart_containers() {
  docker compose -f ./docker-compose.yaml restart
  echo "Done."
}

remove_containers() {
  docker compose -f ./docker-compose.yaml stop
  docker compose -f ./docker-compose.yaml rm
  echo "Done."
}

# Check the command line argument
case "$1" in
  start)
    start_containers
    ;;
  stop)
    stop_containers
    ;;
  restart)
    restart_containers
    ;;
  remove)
    remove_containers
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|remove}"
    exit 1
esac
