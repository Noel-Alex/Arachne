use anyhow::Result;
use arachne_core::config::ArachneConfig;
use arachne_core::db::ArachneRepo;

/// Display status of crawl jobs.
pub async fn run(config: ArachneConfig, job_id: Option<String>) -> Result<()> {
    let db = ArachneRepo::new(&config).await?;

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
        // Cut at the last char boundary at or before `max - 1` bytes so a
        // multibyte char never gets split mid-sequence.
        let end = s
            .char_indices()
            .map(|(i, _)| i)
            .take_while(|&i| i < max)
            .last()
            .unwrap_or(0);
        format!("{}…", &s[..end])
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::truncate;

    #[test]
    fn truncate_multibyte_does_not_panic_and_cuts_on_boundary() {
        let name = "créature-créature-créature";
        // At max=14 a naive byte slice (&s[..13]) lands inside the second 'é'
        // (bytes 12-13) and panics; we must cut before the whole char instead.
        let truncated = truncate(name, 14);
        assert_eq!(truncated, "créature-cr…");
        assert!(truncated.ends_with('…'));
    }

    #[test]
    fn truncate_multibyte_call_site_width_is_safe() {
        // The list view truncates names at width 18.
        assert_eq!(
            truncate("créature-créature-créature", 18),
            "créature-créatu…"
        );
    }

    #[test]
    fn truncate_ascii_unchanged() {
        assert_eq!(truncate("plain-job-name-here", 18), "plain-job-name-he…");
        assert_eq!(truncate("short", 18), "short");
    }
}
