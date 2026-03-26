use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snippet {
    pub name: String,
    pub sql:  String,
}

pub struct SnippetStore {
    pub snippets: Vec<Snippet>,
    path: PathBuf,
}

impl SnippetStore {
    pub fn load(path: &Path) -> Self {
        let snippets = std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        Self { snippets, path: path.to_path_buf() }
    }

    pub fn save(&self) -> Result<(), String> {
        let json = serde_json::to_string_pretty(&self.snippets).map_err(|e| e.to_string())?;
        crate::utils::atomic_write(&self.path, &json)
    }

    pub fn add_or_replace(&mut self, snippet: Snippet) {
        if let Some(existing) = self.snippets.iter_mut().find(|s| s.name == snippet.name) {
            *existing = snippet;
        } else {
            self.snippets.push(snippet);
        }
    }

    pub fn delete(&mut self, name: &str) {
        self.snippets.retain(|s| s.name != name);
    }
}
