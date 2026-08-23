use anyhow::Result;
use arachne_core::config::ArachneConfig;
use arachne_core::db::ArachneRepo;

/// Display status of crawl jobs.
pub async fn run(config: ArachneConfig, job_id: Option<String>) -> Result<()> {
    let db = ArachneRepo::new(&config.scylla).await?;

    if let Some(id_str) = job_id {
        let id = id_str.parse()?;
        match db.get_job(&id).await? {
            Some(job) => {
                println!("Job: {}", job.name);
                println!("  ID:      {}", job.id);
                println!("  Status:  {:?}", job.status);
                println!("  Created: {}", job.created_at);
                println!("  Seeds:   {}", job.seed_urls.len());
                if let Some(max) = job.max_pages {
                    println!("  Max pages: {}", max);
                }
                if let Some(depth) = job.max_depth {
                    println!("  Max depth: {}", depth);
                }
            }
            None => {
                println!("Job not found: {}", id_str);
            }
        }
    } else {
        let jobs = db.list_jobs().await?;
        if jobs.is_empty() {
            println!("No crawl jobs found.");
        } else {
            println!("{:<38} {:<20} {:<12} Created", "ID", "Name", "Status");
            println!("{}", "-".repeat(90));
            for job in jobs {
                println!(
                    "{:<38} {:<20} {:<12} {}",
                    job.id,
                    truncate(&job.name, 18),
                    format!("{:?}", job.status),
                    job.created_at.format("%Y-%m-%d %H:%M")
                );
            }
        }
    }

    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() > max {
        format!("{}…", &s[..max - 1])
    } else {
        s.to_string()
    }
}
