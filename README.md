# Arachne: The Seeker

A distributed, high-performance web crawler designed for resilience, throughput, and the relentless pursuit of knowledge.

---

## 1. Overview

Arachne is the foundational data-gathering component of the **Logos Search Engine** ecosystem. Its sole purpose is to explore the vastness of the internet with extreme efficiency, discovering and fetching content to feed the downstream indexing and query systems. It is designed from the ground up to be horizontally scalable, fault-tolerant, and capable of sustaining a high-volume, respectful crawl.

This system is not just a tool; it is **The Seeker**—the first, essential step in the journey to transform the chaotic web into structured, accessible knowledge.

## 2. Core Philosophy & Design

The architecture of Arachne is guided by three core principles:

*   **Extreme Performance:** Every component is written in **Rust** to guarantee memory safety, fearless concurrency, and bare-metal performance. This is complemented by the use of ScyllaDB and Apache Kafka, technologies renowned for their low-latency and high-throughput capabilities.
*   **Decoupled Scalability:** The system is architected around a message queue, completely decoupling the coordination logic from the fleet of crawling workers. This allows for effortless horizontal scaling—simply add more worker instances to increase crawl throughput.
*   **Resilience and Durability:** By using Kafka as a central bus and ScyllaDB for state persistence, the system can withstand worker or node failures without losing data or crawl progress. Tasks are durable and processing can be resumed by any available worker.

## 3. System Architecture

Arachne consists of four primary components that work in concert:

![img.png](img.png)

1.  **Coordinator Node:** The brain of the operation. It manages the master state of the crawl.
    *   **URL Frontier:** Prioritizes which URLs should be crawled next.
    *   **Seen Set:** A high-performance lookup table to prevent re-crawling the same content.
    *   **Politeness Manager:** Enforces `robots.txt` rules and rate limits to ensure Arachne is a good citizen of the web.
    *   **Coordinator Logic:** Dispatches URLs to the `url-to-crawl` topic.

2.  **Message Queue (The Current):** Powered by **Apache Kafka**, this acts as the central nervous system.
    *   `url-to-crawl` topic: The Coordinator places URLs here for the workers to consume.
    *   `crawl-results` topic: Workers publish the outcome of their fetching tasks here, which are then consumed by the Logos indexing pipeline.

3.  **Worker Fleet (Fleet):** A scalable fleet of stateless workers that perform the actual crawling.
    *   Each worker consumes a URL from the queue, fetches the content from the internet, performs preliminary parsing, and returns the content.
    *   Results (content location and metadata) are published back to the `crawl-results` topic.

4.  **Data Persistence (The Chronicle):**
    *   **Metadata Database (ScyllaDB):** Stores page metadata, link graphs, crawl timestamps, and other essential information needed for coordination and ranking. Chosen for its extreme low-latency read/write performance at scale.
    *   **Content Storage (AWS S3 / Object Store):** Raw fetched content (HTML, PDFs, media) is stored in a durable, cost-effective object store.
    *   **Crawl State (Redis / ScyllaDB):** Caches and ephemeral state for the Seen Set and Frontier for maximum performance.

## 4. Technology Stack

*   **Core Language:** [**Rust**](https://www.rust-lang.org/) (for its performance, safety, and concurrency)
*   **Message Broker:** [**Apache Kafka**](https://kafka.apache.org/) (for durable, high-throughput messaging)
*   **Metadata Store:** [**ScyllaDB**](https://www.scylladb.com/) (for its masterclass performance as a NoSQL database)
*   **Content Store:** [**AWS S3**](https://aws.amazon.com/s3/) (or any S3-compatible object store)
*   **Deployment:** [**Docker**](https://www.docker.com/) & [**Docker Compose**](https://docs.docker.com/compose/) (for easy, reproducible deployments)

## 5. Getting Started

The entire Arachne stack is containerized for simple, one-command initialization.

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/Noel-Alex/Logos.git
    cd Logos
    cd Arachne
    ```

2.  **Initialize the infrastructure:**
    This command will start Kafka and ScyllaDB services in the background.
    ```bash
    docker-compose up
    ```

3.  **Build and run the Rust components:**
    (Build instructions for the Coordinator and Worker binaries will go here).
    ```bash
    # Example
    cargo build --release
    ./target/release/coordinator &
    ./target/release/worker &
    ```

## 6. License

This project is licensed under the [MIT License](LICENSE).