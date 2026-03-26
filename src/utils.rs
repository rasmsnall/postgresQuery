use std::path::{Path, PathBuf};

/// returns the per-user app data directory, creating it if needed.
/// falls back to the current directory if the OS path can't be determined.
pub fn app_data_dir() -> PathBuf {
    let base = dirs_next::data_local_dir()
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let dir = base.join("postgres_query_launcher");
    let _ = std::fs::create_dir_all(&dir);
    dir
}

/// write `content` to `path` atomically: write to a temp file in the same
/// directory, then rename over the target. this guarantees the target is never
/// left in a partially-written state on crash.
pub fn atomic_write(path: &Path, content: &str) -> Result<(), String> {
    let dir = path.parent().unwrap_or(Path::new("."));
    // use a fixed tmp name alongside the target to stay on the same filesystem
    let tmp = dir.join(format!(
        ".{}.tmp",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
    ));
    std::fs::write(&tmp, content).map_err(|e| format!("write tmp: {e}"))?;
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("rename to {}: {e}", path.display())
    })
}
