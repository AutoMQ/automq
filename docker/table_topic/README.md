# AutoMQ Table Topic Quick Start

This document will guide you on how to quickly start and experience the AutoMQ Table Topic feature.

## Docker Compose Components

This Docker Compose setup integrates the following components for a Table Topic experience:

*   **AutoMQ (`automq`)**: A single-node AutoMQ instance, supporting Table Topics and using Minio for storage.
*   **Minio (`minio`)**: S3-compatible object storage for AutoMQ data and Iceberg tables. Buckets (`warehouse`, `automq-data`, `automq-ops`) are auto-created by the `mc` service.
*   **Spark & Jupyter (`spark-iceberg`)**: Includes a Spark engine for Iceberg table operations and a Jupyter Notebook (accessible at `http://localhost:8888`), pre-configured for AutoMQ and Iceberg interaction.
*   **Iceberg REST Catalog (`rest`)**: Metadata service for Iceberg tables, utilized by AutoMQ and Spark.
*   **Schema Registry (`schema-registry`)**: Confluent Schema Registry for managing schemas used with Table Topics.

## Usage Instructions

1.  **Start the Docker Compose Environment**:

    Execute the following command in the current directory to start all services:

    ```bash
    docker-compose up -d
    ```

2.  **Access Jupyter Notebook**:

    After the services have started successfully, open `http://localhost:8888` in your web browser.

3.  **Run the Table Topic Demo Notebook**:

    In the Jupyter Notebook file browser, navigate to the `notebooks` folder, and then open the notebook named `TableTopic - Getting Started.ipynb`.

    Through this notebook, you can perform the following operations:
    *   **Automatic Iceberg Table Creation**: Demonstrates how Table Topic automatically creates corresponding Iceberg tables based on Kafka Topic configurations.
    *   **Upsert Mode**: Experience how sending messages with operation flags (Insert, Update, Delete) to a Kafka Topic synchronizes data to the Iceberg table.
    *   **Data Partitioning**: Understand how Table Topic partitions data for storage based on configurations.
    *   **Query Validation**: After each operation (insert, update, delete), query the Iceberg table using Spark SQL to verify data consistency and correctness.
    *   **Resource Cleanup**: Demonstrates how to delete Topics and their corresponding Iceberg tables.

## Notes

*   Please ensure that Docker and Docker Compose are installed on your machine.
*   The initial startup may take some time to download images and initialize services.
*   To stop and remove all containers, use the `docker-compose down` command.
