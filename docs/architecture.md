# Project Architecture

```mermaid
flowchart TD
    subgraph User
        A[Python Producer]
    end
    subgraph Streaming
        B[Kafka]
    end
    subgraph Processing
        C[Spark Streaming]
        D[Spark Batch]
    end
    subgraph Storage
        E[MinIO (S3)]
        F[Iceberg Tables<br>Bronze/Silver/Gold]
    end
    subgraph Orchestration
        G[Airflow]
    end

    A -->|JSON Events| B
    B -->|Real-time Data| C
    C -->|Write Bronze| F
    D -->|Bronze → Silver → Gold| F
    G -->|Schedule Jobs| D
    G -->|Schedule Streaming| C
    F -->|Data Lake| E
    E -.->|S3 API| F
```

---

## Explanation
- **Python Producer** generates real-time events and sends them to **Kafka**.
- **Kafka** acts as the streaming backbone, buffering events for processing.
- **Spark Streaming** reads from Kafka and writes raw data to the **Bronze** Iceberg tables.
- **Spark Batch** jobs transform data from Bronze → Silver → Gold layers in Iceberg.
- **MinIO** provides S3-compatible storage for all Iceberg table data files.
- **Airflow** orchestrates and schedules all Spark jobs (batch and streaming).
- All data is stored in Iceberg tables, which are managed and queried by Spark. 