use anyhow::Result;
use clap::{Parser, Subcommand};

mod commands;

/// Arachne — High-performance distributed web crawler
#[derive(Parser)]
#[command(name = "arachne", version, about, long_about = None)]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "config/default.toml")]
    config: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Seed URLs into the crawl queue
    Seed {
        /// URLs to seed directly
        #[arg(short, long, num_args = 1..)]
        urls: Option<Vec<String>>,

        /// Path to a file containing URLs (one per line)
        #[arg(short, long)]
        file: Option<String>,

        /// Read URLs from stdin
        #[arg(long)]
        stdin: bool,

        /// Label for this batch of URLs
        #[arg(short, long, default_value = "manual")]
        label: String,
    },

    /// Start a new crawl job
    Crawl {
        /// Seed URLs to start crawling from
        #[arg(short, long, num_args = 1.., required = true)]
        seeds: Vec<String>,

        /// Name for this crawl job
        #[arg(short, long)]
        name: Option<String>,

        /// Maximum total pages to crawl
        #[arg(long)]
        max_pages: Option<u64>,

        /// Maximum pages per domain
        #[arg(long)]
        max_pages_per_domain: Option<i64>,

        /// Maximum crawl depth from seed URLs
        #[arg(long)]
        max_depth: Option<u32>,

        /// Only crawl within these domains (comma-separated)
        #[arg(long, value_delimiter = ',')]
        allowed_domains: Option<Vec<String>>,

        /// Follow links to external domains
        #[arg(long, default_value = "false")]
        follow_external: bool,

        /// Crawl delay in milliseconds
        #[arg(long)]
        crawl_delay: Option<u64>,

        /// Topic keywords for focused crawling (comma-separated)
        #[arg(long, value_delimiter = ',')]
        topic: Option<Vec<String>>,

        /// Maximum content size per page (e.g., "5MB", "1MB")
        #[arg(long)]
        max_content_size: Option<String>,

        /// Store raw HTML content
        #[arg(long, default_value = "true")]
        store_html: bool,

        /// Store extracted text content
        #[arg(long, default_value = "true")]
        store_text: bool,

        /// Don't respect robots.txt (not recommended)
        #[arg(long)]
        ignore_robots: bool,

        /// License to attribute organically-discovered audio (e.g. "cc-by");
        /// audio without a license and without this is never admitted
        #[arg(long)]
        default_license: Option<String>,
    },

    /// Check the status of crawl jobs
    Status {
        /// Specific job ID to check
        #[arg(short, long)]
        job_id: Option<String>,
    },

    /// Export crawled data
    Export {
        /// Job ID to export data from
        #[arg(short, long)]
        job_id: Option<String>,

        /// Domain to export data for
        #[arg(short, long)]
        domain: Option<String>,

        /// Output format
        #[arg(short, long, default_value = "json")]
        format: String,

        /// Output file path
        #[arg(short, long, default_value = "./export")]
        output: String,
    },

    /// Harvest audio from a legal source (enumerate catalog → queue downloads)
    Harvest {
        /// Source adapter: "jamendo" or "archive-org"
        #[arg(short, long)]
        source: String,

        /// Cap on newly-admitted tracks (jamendo) or items scanned (archive-org)
        #[arg(long)]
        limit: Option<u64>,

        /// Jamendo API client_id (or set JAMENDO_CLIENT_ID)
        #[arg(long)]
        jamendo_client_id: Option<String>,

        /// Contact address for the User-Agent (required by archive.org policy)
        #[arg(long)]
        contact: Option<String>,
    },

    /// Export the track manifest for a source as a Sivana handoff snapshot
    TracksExport {
        /// Source adapter name (e.g. "jamendo", "archive-org")
        #[arg(short, long)]
        source: String,

        /// Output directory for manifest.jsonl.zst / manifest.json / attribution.txt
        #[arg(short, long, default_value = "./handoff")]
        output: String,

        /// Include pending/failed/rejected tracks too (default exports done-only)
        #[arg(long)]
        include_incomplete: bool,
    },

    /// Inspect domain information
    Inspect {
        /// Domain to inspect
        domain: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    arachne_core::logging::init_logging();

    // Load configuration
    let config = arachne_core::config::ArachneConfig::load(Some(&cli.config))?;

    match cli.command {
        Commands::Seed {
            urls,
            file,
            stdin,
            label,
        } => commands::seed::run(config, urls, file, stdin, label).await,
        Commands::Crawl {
            seeds,
            name,
            max_pages,
            max_pages_per_domain,
            max_depth,
            allowed_domains,
            follow_external,
            crawl_delay,
            topic,
            max_content_size,
            store_html,
            store_text,
            ignore_robots,
            default_license,
        } => {
            commands::crawl::run(
                config,
                seeds,
                name,
                max_pages,
                max_pages_per_domain,
                max_depth,
                allowed_domains,
                follow_external,
                crawl_delay,
                topic,
                max_content_size,
                store_html,
                store_text,
                ignore_robots,
                default_license,
            )
            .await
        }
        Commands::Status { job_id } => commands::status::run(config, job_id).await,
        Commands::Export {
            job_id,
            domain,
            format,
            output,
        } => commands::export::run(config, job_id, domain, format, output).await,
        Commands::Harvest {
            source,
            limit,
            jamendo_client_id,
            contact,
        } => {
            commands::harvest::run(config, source, limit, jamendo_client_id, contact).await
        }
        Commands::TracksExport {
            source,
            output,
            include_incomplete,
        } => commands::tracks_export::run(config, source, output, include_incomplete).await,
        Commands::Inspect { domain } => commands::inspect::run(config, domain).await,
    }
}
