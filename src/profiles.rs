use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const KEYRING_SERVICE: &str = "postgres_query_launcher";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfile {
    pub name:         String,
    pub host:         String,
    pub port:         u16,
    pub dbname:       String,
    pub user:         String,
    pub save_password: bool,
}

pub struct ProfileStore {
    pub profiles: Vec<ConnectionProfile>,
    path: PathBuf,
}

impl ProfileStore {
    pub fn load(path: &Path) -> Self {
        let profiles = std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        Self { profiles, path: path.to_path_buf() }
    }

    pub fn save(&self) -> Result<(), String> {
        let json = serde_json::to_string_pretty(&self.profiles).map_err(|e| e.to_string())?;
        crate::utils::atomic_write(&self.path, &json)
    }

    pub fn add_or_replace(&mut self, profile: ConnectionProfile) {
        if let Some(existing) = self.profiles.iter_mut().find(|p| p.name == profile.name) {
            *existing = profile;
        } else {
            self.profiles.push(profile);
        }
    }

    pub fn delete(&mut self, name: &str) {
        // remove stored password when deleting a profile
        let _ = delete_password(name);
        self.profiles.retain(|p| p.name != name);
    }
}

// ---------------------------------------------------------------------------
// Windows Credential Manager helpers via the keyring crate
// ---------------------------------------------------------------------------

// key format: service = KEYRING_SERVICE, account = profile name
pub fn save_password(profile_name: &str, password: &str) -> Result<(), String> {
    keyring::Entry::new(KEYRING_SERVICE, profile_name)
        .map_err(|e| e.to_string())?
        .set_password(password)
        .map_err(|e| e.to_string())
}

pub fn load_password(profile_name: &str) -> Option<String> {
    keyring::Entry::new(KEYRING_SERVICE, profile_name)
        .ok()?
        .get_password()
        .ok()
}

pub fn delete_password(profile_name: &str) -> Result<(), String> {
    match keyring::Entry::new(KEYRING_SERVICE, profile_name)
        .map_err(|e| e.to_string())?
        .delete_credential()
    {
        Ok(_) => Ok(()),
        // not found is fine: means i don't need to delete 
        Err(keyring::Error::NoEntry) => Ok(()),
        Err(e) => Err(e.to_string()),
    }
}
