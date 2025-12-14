// src/bin/worker.rs
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use tokio::time::{self, Duration};
use arachne::{CrawlResult, CrawlStatus};
use std::env;

use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use url::{ParseError, Url};
use arachne::db;

#[derive(Debug, thiserror::Error)]
enum CrawlerError {
    #[error("Request error: {0}")]
    RequestError(#[from] reqwest::Error),
    #[error("URL parsing error: {0}")]
    UrlParseError(#[from] ParseError),
    #[error("Robots.txt parsing error")]
    RobotsParseError,
}



/// Asynchronously crawls a given URL, returning the page content and discovered links.
///
/// # Arguments
/// * `client` - A `reqwest::Client` to make HTTP requests.
/// * `url_str` - The URL to crawl.
///
/// # Returns
/// A `Result` containing a `CrawlResult` on success or a specific `CrawlerError` on failure.
async fn crawl_url(client: &Client, url_str: &str) -> Result<CrawlResult, CrawlerError> {
    let base_url = Url::parse(url_str)?;

    // Make the HTTP request
    let response = client.get(base_url.clone()).send().await?;

    // Check for non-successful status codes (e.g., 404, 500)
    if !response.status().is_success() {
        return Ok(CrawlResult {
            source_url: url_str.to_string(),
            status: CrawlStatus::HttpError(response.status().as_u16()),
            content: None,
            discovered_urls: vec![],
        });
    }

    // Await the response body as text
    let body = response.text().await?;
    let document = Html::parse_document(&body);
    // This selector is static and known to be valid, so .unwrap() is safe.
    let selector = Selector::parse("a[href]").unwrap();

    let mut found_links = HashSet::new(); // Use a HashSet to automatically handle duplicates

    for element in document.select(&selector) {
        if let Some(href) = element.value().attr("href") {
            // Join the found href with the base URL to resolve relative links (e.g., "/about")
            if let Ok(mut new_url) = base_url.join(href) {
                // Remove the fragment part (e.g., #section-name) from the URL
                new_url.set_fragment(None);
                found_links.insert(new_url.to_string());
            }
        }
    }

    // If we've reached here, the crawl was successful.
    Ok(CrawlResult {
        source_url: url_str.to_string(),
        status: CrawlStatus::Success,
        content: Some(body), // Include the HTML content
        discovered_urls: found_links.into_iter().collect(),
    })
}


#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    // Assuming connect_to_db is an async function you have defined
    // db::connect_to_db().await.expect("Scylla db connection failed");

    // --- Configuration ---
    let bootstrap_servers = env::var("KAFKA_SERVER").expect("KAFKA_SERVER not in .env");
    let consume_topic = "urls-to-crawl";
    let produce_topic = "crawl-results";
    let group_id = "arachne-worker-group";

    // --- Create Kafka Consumer ---
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&[consume_topic]).expect("Can't subscribe");

    // --- Create Kafka Producer ---
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("queue.buffering.max.messages", "100000")
        .set("linger.ms", "1000")
        .set("batch.size", "6553600")
        .set("compression.type", "lz4")
        .set("acks", "1")
        .create()
        .expect("Producer creation failed");

    // --- Create a reusable reqwest client ---
    let http_client = Client::new();

    println!("Worker started. Waiting for URLs...");

    // --- Main Worker Loop ---
    loop {
        match consumer.recv().await {
            Err(e) => eprintln!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => {
                        eprintln!("Message with empty payload");
                        continue; // Skip this message
                    },
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        eprintln!("Error viewing message payload as string: {}", e);
                        continue; // Skip this message
                    }
                };

                let source_url = payload.to_string();
                println!("Received URL to crawl: {}", source_url);

                // Perform the web crawling and parsing
                let crawl_result = match crawl_url(&http_client, &source_url).await {
                    Ok(result) => result,
                    Err(e) => {
                        // This branch handles fundamental fetch errors like DNS failure, timeouts,
                        // or issues with the URL itself.
                        eprintln!("CrawlerError for {}: {}", source_url, e);
                        CrawlResult {
                            source_url: source_url.clone(),
                            status: CrawlStatus::FetchError(e.to_string()),
                            content: None,
                            discovered_urls: vec![],
                        }
                    },
                };

                // Serialize the result to JSON
                let result_json = serde_json::to_string(&crawl_result)
                    .expect("Failed to serialize CrawlResult to JSON");

                // Create a record for the results topic
                let record = FutureRecord::to(produce_topic)
                    .key(&source_url)
                    .payload(&result_json);

                // Send the result to Kafka
                match producer.send(record, Duration::from_secs(0)).await {
                    Ok(_) => {
                        println!("Successfully produced crawl result for {}", source_url);

                        // ---- NEW PART: Manually commit the offset ----
                        // We only commit AFTER we know the result was produced.
                        if let Err(e) = consumer.commit_message(&m, rdkafka::consumer::CommitMode::Async) {
                            eprintln!("Failed to commit offset: {}", e);
                        }
                    },
                    Err((e, _)) => {
                        eprintln!("Failed to produce Kafka message for {}: {}", source_url, e);
                        // We DO NOT commit here, so the message will be re-processed later.
                    }
                }
            }
        }
    }
}