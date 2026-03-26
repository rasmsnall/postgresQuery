use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const MAX_RECENT: usize = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecentConnection {
    pub host:   String,
    pub port:   u16,
    pub dbname: String,
    pub user:   String,
    pub use_tls: bool,
    // password intentionally omitted
}

pub struct RecentStore {
    pub entries: Vec<RecentConnection>,
    path: PathBuf,
}

impl RecentStore {
    pub fn load(path: &Path) -> Self {
        let entries = std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        Self { entries, path: path.to_path_buf() }
    }

    pub fn save(&self) -> Result<(), String> {
        let json = serde_json::to_string_pretty(&self.entries).map_err(|e| e.to_string())?;
        crate::utils::atomic_write(&self.path, &json)
    }

    // push a new entry, deduplicating by host+port+dbname+user, keep last MAX_RECENT
    pub fn push(&mut self, entry: RecentConnection) {
        self.entries.retain(|e| {
            !(e.host == entry.host
                && e.port == entry.port
                && e.dbname == entry.dbname
                && e.user == entry.user)
        });
        self.entries.insert(0, entry);
        self.entries.truncate(MAX_RECENT);
    }
}
