use chrono::{DateTime, Local, NaiveDate, TimeZone};
use rusqlite::{params, Connection, Result};

const MAX_HISTORY_ROWS: i64 = 10_000;

#[derive(Debug, Clone)]
pub struct HistoryEntry {
    #[allow(dead_code)]
    pub id:        i64,
    pub timestamp: DateTime<Local>,
    pub query:     String,
    pub row_count: Option<usize>,
    pub error:     Option<String>,
}

pub struct HistoryStore {
    conn: Connection,
}

impl HistoryStore {
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        // WAL mode for crash safety and better concurrent reads
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS query_history (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT    NOT NULL,
                query     TEXT    NOT NULL,
                row_count INTEGER,
                error     TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_history_ts ON query_history (timestamp DESC);",
        )?;
        Ok(Self { conn })
    }

    pub fn insert(&self, entry: &HistoryEntry) -> Result<()> {
        self.conn.execute(
            "INSERT INTO query_history (timestamp, query, row_count, error)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                entry.timestamp.to_rfc3339(),
                entry.query,
                entry.row_count.map(|n| n as i64),
                entry.error,
            ],
        )?;
        // prune oldest rows beyond the cap to keep the DB bounded
        self.conn.execute(
            "DELETE FROM query_history WHERE id NOT IN (
                SELECT id FROM query_history ORDER BY timestamp DESC LIMIT ?1
             )",
            params![MAX_HISTORY_ROWS],
        )?;
        Ok(())
    }

    pub fn fetch_all(&self) -> Result<Vec<HistoryEntry>> {
        self.fetch_with_filter(None, None)
    }

    pub fn fetch_between(&self, from: NaiveDate, to: NaiveDate) -> Result<Vec<HistoryEntry>> {
        self.fetch_with_filter(Some(from), Some(to))
    }

    fn fetch_with_filter(
        &self,
        from: Option<NaiveDate>,
        to: Option<NaiveDate>,
    ) -> Result<Vec<HistoryEntry>> {
        let (sql, from_str, to_str) = match (from, to) {
            (Some(f), Some(t)) => {
                let from_str = Local
                    .from_local_datetime(&f.and_hms_opt(0, 0, 0).unwrap())
                    .unwrap()
                    .to_rfc3339();
                // end of the 'to' day
                let to_str = Local
                    .from_local_datetime(&t.and_hms_opt(23, 59, 59).unwrap())
                    .unwrap()
                    .to_rfc3339();
                (
                    "SELECT id, timestamp, query, row_count, error FROM query_history \
                     WHERE timestamp >= ?1 AND timestamp <= ?2 ORDER BY timestamp DESC",
                    Some(from_str),
                    Some(to_str),
                )
            }
            _ => (
                "SELECT id, timestamp, query, row_count, error FROM query_history \
                 ORDER BY timestamp DESC",
                None,
                None,
            ),
        };

        let mut stmt = self.conn.prepare(sql)?;

        let map_row = |row: &rusqlite::Row| {
            let ts_str: String = row.get(1)?;
            let timestamp = DateTime::parse_from_rfc3339(&ts_str)
                .map(|dt| dt.with_timezone(&Local))
                .unwrap_or_else(|_| Local::now());
            Ok(HistoryEntry {
                id:        row.get(0)?,
                timestamp,
                query:     row.get(2)?,
                row_count: row.get::<_, Option<i64>>(3)?.map(|n| n as usize),
                error:     row.get(4)?,
            })
        };

        let rows = if let (Some(f), Some(t)) = (from_str, to_str) {
            stmt.query_map(params![f, t], map_row)?
                .collect::<Result<Vec<_>>>()?
        } else {
            stmt.query_map([], map_row)?
                .collect::<Result<Vec<_>>>()?
        };

        Ok(rows)
    }
}
