#!/bin/bash

start_service() {

  exec /opt/bitnami/kafka/bin/kafka-server-start.sh /opt/bitnami/kafka/config/server.properties
}

stop_server() {
    echo "Stopping Kafka..."

    kafka_pid=$(jps -l | grep 'Kafka' | awk '{print $1}')

    if [ -n "$kafka_pid" ]; then
        echo "Kafka process found with PID: $kafka_pid. Attempting to stop..."

        kill -SIGTERM "$kafka_pid"

        if [ $? -eq 0 ]; then
            echo "Kafka stopped successfully."
        else
            echo "Failed to stop Kafka."
        fi
    else
        echo "Kafka process not found."
    fi
}

check_kafka_online() {
  local type=$1

  podId=$(hostname | awk -F- '{print $NF}')

  if [ "$type" == "broker" ]; then
      nodeId=$((podId + 1000))
  elif [ "$type" == "controller" ]; then
      nodeId=$podId
  else
      echo "Invalid type. Please use 'broker' or 'controller'."
      exit 1
  fi

  java -Xms64M -Xmx64M -cp "/opt/automq/kafka/libs/*" com.automq.enterprise.shell.AutoMQCLI cluster describe --bootstrap-server localhost:9092 | grep "nodeId=$nodeId," | grep ACTIVE || false

  if [ $? -eq 0 ]; then
      echo "Kafka is online."
  else
      echo "Kafka is not online."
      exit 1
  fi
}

if [ "$1" == "start" ]; then
    start_service
elif [ "$1" == "stop" ]; then
    stop_server
else
    echo "Invalid argument. Please use 'start' to start services or 'check' to check if Kafka is online."
    exit 1
fi
